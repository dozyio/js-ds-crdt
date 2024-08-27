import { BloomFilter } from '@libp2p/utils/filters'
import { Mutex } from 'async-mutex'
import { type Batch, type Datastore, Key, type Pair, type Query } from 'interface-datastore'
import { compareUint8Arrays } from './utils'
import type * as pb from './pb/delta'
import type { Logger } from '@libp2p/logger'

// Define namespaces and suffixes
const elemsNs = 's'
const tombsNs = 't'
const keysNs = 'k'
const valueSuffix = 'v'
const prioritySuffix = 'p'

// The Go implementation uses a bloom filter with a size of 30 MiB and 2 hashes.
// We use a smaller size and hash count as a trade-off
const TombstonesBloomFilterSize = 1024 * 1024 * 8 // 1 MiB
const TombstonesBloomFilterHashes = 2

export interface IBloomFilter {
  add(key: string): void
  has(key: string): boolean
}

export class CRDTSet {
  private readonly store: Datastore
  private readonly namespace: Key
  private readonly putHook?: (key: string, value: Uint8Array) => void
  private readonly deleteHook?: (key: string) => void
  private readonly logger: Logger
  private readonly putElemsMux: Mutex
  private readonly tombstonesBloom?: IBloomFilter

  constructor (
    store: Datastore,
    namespace: Key,
    logger: Logger,
    tombstonesBloom?: IBloomFilter,
    putHook?: (key: string, value: Uint8Array) => void,
    deleteHook?: (key: string) => void
  ) {
    this.store = store
    this.namespace = namespace
    this.logger = logger
    this.putHook = putHook
    this.deleteHook = deleteHook
    this.putElemsMux = new Mutex()
    this.tombstonesBloom = tombstonesBloom

    if (this.tombstonesBloom === undefined) {
      this.tombstonesBloom = new BloomFilter({
        bits: TombstonesBloomFilterSize,
        hashes: TombstonesBloomFilterHashes
      })
    }

    if (this.tombstonesBloom !== undefined) {
      this.primeBloomFilter().catch((err: any) => {
        throw err
      })
    }
  }

  // Prime the Bloom filter with existing tombstones
  private async primeBloomFilter (): Promise<void> {
    if (this.tombstonesBloom === undefined) {
      return
    }

    const tombsPrefix = this.keyPrefix(tombsNs)
    const q: Query = {
      prefix: tombsPrefix.toString()
      // keysOnly: true, // TODO look at pair filter
    }

    const results = this.store.query(q)
    let nTombs = 0

    for await (const result of results) {
      const key = new Key(result.key.toString()).parent()
      this.tombstonesBloom.add(key.toString())
      nTombs++
    }

    this.logger(`Tombstones have bloomed: ${nTombs} tombs.`)
  }

  // Add a new element
  public add (key: string, value: Uint8Array): pb.delta.Delta {
    return {
      elements: [{ key, value }] as pb.delta.Element[],
      tombstones: [],
      priority: BigInt(0)
    }
  }

  // Remove an element
  public async remove (key: string): Promise<pb.delta.Delta> {
    const delta: pb.delta.Delta = {
      elements: [],
      tombstones: [],
      priority: BigInt(0)
    }
    const prefix = this.elemsPrefix(key)
    const q: Query = {
      prefix: prefix.toString()
      // keysOnly: true // TODO look at pair filter
    }
    const results = this.store.query(q)

    for await (const result of results) {
      const id = this.removePrefix(result.key.toString(), prefix.toString())

      if (!this.rawKey(id).isTopLevel()) {
        // our prefix matches blocks from other keys i.e. our
        // prefix is "hello" and we have a different key like
        // "hello/bye" so we have a block id like
        // "bye/<block>". If we got the right key, then the id
        // should be the block id only.
        continue
      }

      // check if its already tombed, which case don't add it to the
      // Rmv delta set.
      const deleted = await this.inTombsKeyID(key, id)
      // if (!deleted && delta?.tombstones != null && delta.tombstones.length > 0) {
      // if (!deleted && delta?.tombstones != null) {
      if (!deleted) {
        delta.tombstones.push({ key, id, value: new Uint8Array() })
      }
    }
    return delta
  }

  private removePrefix (str: string, prefix: string): string {
    if (str.startsWith(prefix)) {
      return str.substring(prefix.length)
    }
    return str
  }

  // rawKey creates a new Key without safety checking the input. Use with care.
  private rawKey (s: string): Key {
    // accept an empty string and fix it to avoid special cases
    // elsewhere
    if (s.length === 0) {
      return new Key('/')
    }

    // perform a quick sanity check that the key is in the correct
    // format, if it is not then it is a programmer error and it is
    // okay to panic
    if (s.length === 0 || s[0] !== '/' || (s.length > 1 && s[s.length - 1] === '/')) {
      throw new Error('invalid datastore key: ' + s)
    }

    return new Key(s)
  }

  public async element (key: string): Promise<Uint8Array | null> {
    const valueK = this.valueKey(key)

    try {
      const value = await this.store.get(valueK)
      const inSet = await this.checkNotTombstoned(key)
      if (!inSet) {
        return null
      }
      return value
    } catch (error: any) {
      if (error.name === 'NotFoundError') {
        return null // Key does not exist
      }
      throw error // Re-throw any other errors
    }
  }

  // Return all elements in the set
  public async elements (q: Query): Promise<Pair[]> {
    if (q.prefix === null || q.prefix === undefined || q.prefix === '') {
      throw new Error('Query prefix is required')
    }

    const srcQueryPrefixKey = new Key(q.prefix)
    const keyNamespacePrefix = this.keyPrefix(keysNs)
    const setQueryPrefix = keyNamespacePrefix
      .child(srcQueryPrefixKey)
      .toString()
    const vSuffix = `/${valueSuffix}`

    const setQuery: Query = {
      prefix: setQueryPrefix
      // keysOnly: false, // TODO look at pair filter
    }

    const results = this.store.query(setQuery)

    const finalResults: Pair[] = []

    for await (const result of results) {
      if (!result.key.toString().endsWith(vSuffix)) continue

      const key = result.key
        .toString()
        .replace(keyNamespacePrefix.toString(), '')
        .replace(`/${valueSuffix}`, '')

      const inSet = await this.checkNotTombstoned(key)
      if (!inSet) continue

      finalResults.push({
        key: new Key(key),
        value: result.value
        // size: -1 // TODO result.size,
        // expiration: -1 // TODO result.expiration,
      })
    }

    return finalResults
  }

  // Check if a key belongs to the set
  public async inSet (key: string): Promise<boolean> {
    const valueK = this.valueKey(key)
    const exists = await this.store.has(valueK)
    if (!exists) return false

    return this.checkNotTombstoned(key)
  }

  // Perform a sync against all the paths associated with a key prefix
  public async datastoreSync (prefix: Key): Promise<void> {
    const toSync = [
      this.elemsPrefix(prefix.toString()),
      this.tombsPrefix(prefix.toString()),
      this.keyPrefix(keysNs).child(prefix)
    ]

    const errors = await Promise.all(
      toSync.map(async (k) => {
        try {
          if ('sync' in this.store && typeof this.store.sync === 'function') {
            await this.store.sync(k)
          }
        } catch (error) {
          return error
        }
      })
    )

    if (errors.some(Boolean)) {
      // eslint-disable-next-line @typescript-eslint/no-throw-literal
      throw errors
    }
  }

  // Helper methods for generating keys

  private keyPrefix (key: string): Key {
    return this.namespace.child(new Key(key))
  }

  private elemsPrefix (key: string): Key {
    return this.keyPrefix(elemsNs).child(new Key(key))
  }

  private tombsPrefix (key: string): Key {
    return this.keyPrefix(tombsNs).child(new Key(key))
  }

  private valueKey (key: string): Key {
    return this.keyPrefix(keysNs)
      .child(new Key(key))
      .child(new Key(valueSuffix))
  }

  private priorityKey (key: string): Key {
    return this.keyPrefix(keysNs)
      .child(new Key(key))
      .child(new Key(prioritySuffix))
  }

  // Check if a key is in the tombstones
  private async inTombsKeyID (key: string, id: string): Promise<boolean> {
    const k = this.tombsPrefix(key).child(new Key(id))
    const exists = await this.store.has(k)
    return exists
  }

  // Check if a key is not tombstoned
  private async checkNotTombstoned (key: string): Promise<boolean> {
    if (this.tombstonesBloom !== undefined) {
      if (!this.tombstonesBloom.has(key)) {
        return true
      }
    }

    const prefix = this.elemsPrefix(key)
    const q: Query = {
      prefix: prefix.toString()
      // keysOnly: true // TODO look at pair filter
    }
    const results = this.store.query(q)

    for await (const result of results) {
      const id = result.key.toString().replace(prefix.toString(), '')
      if (new Key(id).isTopLevel()) {
        const inTomb = await this.inTombsKeyID(key, id)
        if (!inTomb) {
          return true
        }
      }
    }

    return false
  }

  private async setValue (
    writeStore: Datastore | Batch,
    key: string,
    id: string,
    value: Uint8Array,
    prio: bigint
  ): Promise<void> {
    const deleted = await this.inTombsKeyID(key, id)
    if (deleted) return

    const curPrio = await this.getPriority(key)
    if (
      prio > curPrio ||
      (prio === curPrio &&
        compareUint8Arrays(value, await this.store.get(this.valueKey(key))) > 0)
    ) {
      // New priority is higher, or priorities are equal but value is lexicographically greater
      await writeStore.put(this.valueKey(key), value)
      await this.setPriority(writeStore, key, prio)
      if (this.putHook !== undefined) {
        this.putHook(key, value)
      }
    }
  }

  private async getPriority (key: string): Promise<bigint> {
    const prioK = this.priorityKey(key)
    try {
      const data = await this.store.get(prioK)
      if (data === null || data === undefined || data.length === 0) {
        return BigInt(0)
      }

      const view = new DataView(data.buffer)
      const prio = view.getBigUint64(0, true)

      return prio - BigInt(1)
    } catch (error: unknown) {
      const err = error as Error
      if (
        (err as any).code === 'ERR_NOT_FOUND' || // Only if error might have a 'code' property
        err.message.includes('Not Found')
      ) {
        // Return default priority if key is not found
        return BigInt(0)
      }
      throw error // Re-throw other errors
    }
  }

  private async setPriority (
    writeStore: Datastore | Batch,
    key: string,
    prio: bigint
  ): Promise<void> {
    const prioK = this.priorityKey(key)
    const buf = new Uint8Array(8)
    const view = new DataView(buf.buffer)
    view.setBigUint64(0, prio + BigInt(1), true)

    await writeStore.put(prioK, buf)
  }

  public async putElems (
    elems: pb.delta.Element[],
    id: string,
    prio: bigint
  ): Promise<void> {
    await this.putElemsMux.runExclusive(async () => {
      if (elems.length === 0) return

      let store

      if ('batch' in this.store && typeof this.store.batch === 'function') {
        store = this.store.batch()
      } else {
        store = this.store
      }

      for (const elem of elems) {
        elem.id = id
        const key = elem.key
        const k = this.elemsPrefix(key).child(new Key(id))

        // Store an empty value for the element in the datastore
        await store.put(k, new Uint8Array())

        // Set the value and priority
        await this.setValue(store, key, id, elem.value, prio)
      }

      // If batching, commit the transaction
      if ('commit' in store && typeof store.commit === 'function') {
        await store.commit()
      }
    })
  }

  // Put tombstones into the set
  public async putTombs (tombs: pb.delta.Element[]): Promise<void> {
    if (tombs.length === 0) return

    let store

    if ('batch' in this.store && typeof this.store.batch === 'function') {
      store = this.store.batch()
    } else {
      store = this.store
    }

    for (const tomb of tombs) {
      const elemKey = tomb.key
      const k = this.tombsPrefix(elemKey).child(new Key(tomb.id))
      await store.put(k, new Uint8Array())

      if (this.tombstonesBloom !== undefined) {
        this.tombstonesBloom.add(elemKey)
      }

      if (this.deleteHook !== undefined) {
        this.deleteHook(elemKey)
      }
    }

    if ('commit' in store && typeof store.commit === 'function') {
      await store.commit()
    }
  }

  // Merge deltas into the set
  public async merge (delta: pb.delta.Delta, id: string): Promise<void> {
    await this.putTombs(delta.tombstones)
    await this.putElems(delta.elements, id, delta.priority)
  }
}
