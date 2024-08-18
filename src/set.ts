import { type Datastore, Key, type Pair, type Query } from 'interface-datastore'
import { BloomFilter } from 'bloom-filters'
import { Mutex } from 'async-mutex'
import type { Logger } from '@libp2p/logger'
import * as pb from './pb/delta'

// Define namespaces and suffixes
const elemsNs = 's'
const tombsNs = 't'
const keysNs = 'k'
const valueSuffix = 'v'
const prioritySuffix = 'p'

// Define Bloom filter options
const TombstonesBloomFilterSize = 30 * 1024 * 1024 * 8 // 30 MiB
const TombstonesBloomFilterHashes = 2

export class CRDTSet {
  private store: Datastore
  private namespace: Key
  private putHook?: (key: string, value: Uint8Array) => void
  private deleteHook?: (key: string) => void
  private logger: Logger
  private putElemsMux: Mutex
  private tombstonesBloom: BloomFilter

  constructor(
    store: Datastore,
    namespace: Key,
    logger: Logger,
    putHook?: (key: string, value: Uint8Array) => void,
    deleteHook?: (key: string) => void,
  ) {
    this.store = store
    this.namespace = namespace
    this.logger = logger
    this.putHook = putHook
    this.deleteHook = deleteHook
    this.putElemsMux = new Mutex()
    this.tombstonesBloom = new BloomFilter(
      TombstonesBloomFilterSize,
      TombstonesBloomFilterHashes,
    )

    this.primeBloomFilter()
  }

  // Prime the Bloom filter with existing tombstones
  private async primeBloomFilter(): Promise<void> {
    const tombsPrefix = this.keyPrefix(tombsNs)
    const q: Query = {
      prefix: tombsPrefix.toString(),
      // keysOnly: true, // TODO look at pair filter
    }

    const results = this.store.query(q)
    let nTombs = 0

    for await (const result of results) {
      const key = new Key(result.key.toString()).parent()
      this.tombstonesBloom.add(key.toString())
      nTombs++
    }

    this.logger.trace(`Tombstones have bloomed: ${nTombs} tombs.`)
  }

  // Add a new element
  public add(key: string, value: Uint8Array): pb.delta.Delta {
    return {
      elements: [{ key, value }] as pb.delta.Element[],
      tombstones: [],
      priority: BigInt(0),
    }
  }

  // Remove an element
  public async remove(key: string): Promise<pb.delta.Delta> {
    const delta: pb.delta.Delta = {
      elements: [],
      tombstones: [],
      priority: BigInt(0),
    }
    const prefix = this.elemsPrefix(key)
    const q: Query = {
      prefix: prefix.toString(),
      // keysOnly: true // TODO look at pair filter
    }
    const results = this.store.query(q)

    for await (const result of results) {
      const id = result.key.toString().replace(prefix.toString(), '')
      if (new Key(id).isTopLevel()) {
        const deleted = await this.inTombsKeyID(key, id)
        if (!deleted && delta && delta.tombstones) {
          delta.tombstones.push({ key, id, value: new Uint8Array() })
        }
      }
    }
    return delta
  }

  // Retrieve the value of an element from the CRDT set
  // public async element(key: string): Promise<Uint8Array | null> {
  //   const valueK = this.valueKey(key);
  //   const value = await this.store.get(valueK);
  //
  //   if (!value) return null;
  //
  //   const inSet = await this.checkNotTombstoned(key);
  //   if (!inSet) {
  //     return null;
  //   }
  //   return value;
  // }
  public async element(key: string): Promise<Uint8Array | null> {
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
  public async elements(q: Query): Promise<Pair[]> {
    if (!q.prefix) {
      throw new Error('Query prefix is required')
    }

    const srcQueryPrefixKey = new Key(q.prefix)
    const keyNamespacePrefix = this.keyPrefix(keysNs)
    const setQueryPrefix = keyNamespacePrefix
      .child(srcQueryPrefixKey)
      .toString()
    const vSuffix = `/${valueSuffix}`

    const setQuery: Query = {
      prefix: setQueryPrefix,
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
        value: result.value,
        // size: -1 // TODO result.size,
        // expiration: -1 // TODO result.expiration,
      })
    }

    return finalResults
  }

  // Check if a key belongs to the set
  public async inSet(key: string): Promise<boolean> {
    const valueK = this.valueKey(key)
    const exists = await this.store.has(valueK)
    if (!exists) return false

    return this.checkNotTombstoned(key)
  }

  // Perform a sync against all the paths associated with a key prefix
  public async datastoreSync(prefix: Key): Promise<void> {
    const toSync = [
      this.elemsPrefix(prefix.toString()),
      this.tombsPrefix(prefix.toString()),
      this.keyPrefix(keysNs).child(prefix),
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
      }),
    )

    if (errors.some(Boolean)) {
      throw errors
    }
  }

  // Helper methods for generating keys

  private keyPrefix(key: string): Key {
    return this.namespace.child(new Key(key))
  }

  private elemsPrefix(key: string): Key {
    return this.keyPrefix(elemsNs).child(new Key(key))
  }

  private tombsPrefix(key: string): Key {
    return this.keyPrefix(tombsNs).child(new Key(key))
  }

  private valueKey(key: string): Key {
    return this.keyPrefix(keysNs)
      .child(new Key(key))
      .child(new Key(valueSuffix))
  }

  private priorityKey(key: string): Key {
    return this.keyPrefix(keysNs)
      .child(new Key(key))
      .child(new Key(prioritySuffix))
  }

  // Check if a key is in the tombstones
  private async inTombsKeyID(key: string, id: string): Promise<boolean> {
    const k = this.tombsPrefix(key).child(new Key(id))
    return this.store.has(k)
  }

  // Check if a key is not tombstoned
  private async checkNotTombstoned(key: string): Promise<boolean> {
    if (!this.tombstonesBloom.has(key)) {
      return true
    }

    const prefix = this.elemsPrefix(key)
    const q: Query = {
      prefix: prefix.toString(),
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

  private async setValue(
    writeStore: Datastore,
    key: string,
    id: string,
    value: Uint8Array,
    prio: bigint,
  ): Promise<void> {
    const deleted = await this.inTombsKeyID(key, id)
    if (deleted) return

    const curPrio = await this.getPriority(key)
    if (
      prio > curPrio ||
      (prio === curPrio &&
        Buffer.compare(value, await this.store.get(this.valueKey(key))) > 0)
    ) {
      // New priority is higher, or priorities are equal but value is lexicographically greater
      await writeStore.put(this.valueKey(key), value)
      await this.setPriority(writeStore, key, prio)
      if (this.putHook) {
        this.putHook(key, value)
      }
    }
  }

  private async getPriority(key: string): Promise<bigint> {
    const prioK = this.priorityKey(key)
    try {
      const data = await this.store.get(prioK)
      if (!data || data.length === 0) return BigInt(0)

      const view = new DataView(data.buffer)
      const prio = view.getBigUint64(0, true)

      return prio - BigInt(1)
    } catch (error: any) {
      if (
        error.code === 'ERR_NOT_FOUND' ||
        error.message.includes('Not Found')
      ) {
        // Return default priority if key is not found
        return BigInt(0)
      }
      throw error // Re-throw other errors
    }
  }

  private async setPriority(
    writeStore: Datastore,
    key: string,
    prio: bigint,
  ): Promise<void> {
    const prioK = this.priorityKey(key)
    const buf = new Uint8Array(8)
    const view = new DataView(buf.buffer)
    view.setBigUint64(0, prio + BigInt(1), true)

    await writeStore.put(prioK, buf)
  }

  public async putElems(
    elems: pb.delta.Element[],
    id: string,
    prio: bigint,
  ): Promise<void> {
    await this.putElemsMux.runExclusive(async () => {
      if (!elems.length) return

      let store = this.store
      if ('batch' in store && typeof store.batch === 'function') {
        store.batch()
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
  public async putTombs(tombs: pb.delta.Element[]): Promise<void> {
    if (!tombs.length) return

    const store = this.store
    for (const tomb of tombs) {
      const elemKey = tomb.key
      const k = this.tombsPrefix(elemKey).child(new Key(tomb.id))
      await store.put(k, new Uint8Array())

      this.tombstonesBloom.add(elemKey)
      if (this.deleteHook) {
        this.deleteHook(elemKey)
      }
    }
  }

  // Merge deltas into the set
  public async merge(delta: pb.delta.Delta, id: string): Promise<void> {
    await this.putTombs(delta.tombstones)
    await this.putElems(delta.elements, id, delta.priority)
  }
}
