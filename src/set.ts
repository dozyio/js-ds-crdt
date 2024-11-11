import { Mutex } from 'async-mutex'
import { type Batch, type Datastore, Key, type KeyQuery, type Pair, type Query } from 'interface-datastore'
import { CID } from 'multiformats'
import { compareUint8Arrays, dsKeyToMultihash, putUvarint, toUint8Array, uvarint } from './utils'
import type { CRDTNodeGetter } from './ipld'
import type * as pb from './pb/delta'
import type { Logger } from '@libp2p/logger'

export class NoBestValueError extends Error {
  constructor () {
    super()
    this.message = 'No best value found'
    this.name = 'NoBestValueError'
    Object.setPrototypeOf(this, new.target.prototype) // Restore prototype chain
  }
}

// Define namespaces and suffixes
const elemsNs = 's'
const tombsNs = 't'
const keysNs = 'k'
const valueSuffix = 'v'
const prioritySuffix = 'p'

export class CRDTSet {
  private readonly store: Datastore
  private readonly namespace: Key
  private readonly nodeGetter: CRDTNodeGetter
  private readonly putHook?: (key: string, value: Uint8Array) => void
  private readonly deleteHook?: (key: string) => void
  private readonly logger: Logger
  private readonly putElemsMux: Mutex

  constructor (
    store: Datastore,
    namespace: Key,
    nodeGetter: CRDTNodeGetter,
    logger: Logger,
    putHook?: (key: string, value: Uint8Array) => void,
    deleteHook?: (key: string) => void
  ) {
    this.store = store
    this.namespace = namespace
    this.nodeGetter = nodeGetter
    this.logger = logger
    this.putHook = putHook
    this.deleteHook = deleteHook
    this.putElemsMux = new Mutex()
  }

  // Add returns an delta with element
  public add (key: string, value: Uint8Array): pb.delta.Delta {
    return {
      elements: [{ key, value }] as pb.delta.Element[],
      tombstones: [],
      priority: BigInt(0)
    }
  }

  // JS implementation of go-datastore Clean
  private cleanKey (key: string): string {
    if (key === '') {
      return '/'
    }

    const rooted = key[0] === '/'
    const n = key.length

    let result = ''
    let r = 0

    if (rooted) {
      result += '/'
      r = 1
    }

    while (r < n) {
      if (key[r] === '/') {
        // Skip redundant slashes
        r++
      } else {
        // Real key element, add a slash if needed
        if (result !== '/' && result.length > 0) {
          result += '/'
        }
        // Copy element
        while (r < n && key[r] !== '/') {
          result += key[r]
          r++
        }
      }
    }

    return result
  }

  // queryPrefixFilter is a JS implementation of NaiveQueryApply
  // https://github.com/ipfs/go-datastore/blob/master/query/query_impl.go#L119
  private queryPrefixFilter (q: Query | KeyQuery, qr: string): string | null {
    if (q.prefix !== '' && q.prefix !== undefined && q.prefix !== null) {
      // Clean the prefix as a key and append / so a prefix of /bar
      // only finds /bar/baz, not /barbaz.
      let prefix = q.prefix

      if (prefix.length === 0) {
        prefix = '/'
      } else {
        if (prefix[0] !== '/') {
          prefix = '/' + prefix
        }

        prefix = this.cleanKey(prefix)
      }

      // If the prefix is empty, ignore it.
      if (prefix !== '/') {
        const testPrefix = prefix + '/'
        if (!qr.startsWith(testPrefix)) {
          return null
        }
      }
    }

    return qr
  }

  // Remove an element
  public async remove (key: string): Promise<pb.delta.Delta> {
    const delta: pb.delta.Delta = {
      elements: [],
      tombstones: [],
      priority: 0n
    }

    // /namespace/<key>/elements
    const prefix = this.elemsPrefix(key)
    const q: KeyQuery = {
      prefix: prefix.toString()
    }

    const results = this.store.queryKeys(q)

    for await (const result of results) {
      let id: string | null = result.toString()

      id = this.queryPrefixFilter(q, id)

      if (id === null) {
        continue
      }

      if (id.startsWith(prefix.toString())) {
        id = id.slice(prefix.toString().length)
      }

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

  // rawKey creates a new Key without safety checking the input. Use with care.
  private rawKey (s: string): Key {
    // accept an empty string and fix it to avoid special cases
    // elsewhere
    if (s.length === 0) {
      return new Key('/')
    }

    // perform a quick sanity check that the key is in the correct
    // format, if it is not then it is a programmer error and it is
    // okay to throw
    if (s.length === 0 || s[0] !== '/' || (s.length > 1 && s[s.length - 1] === '/')) {
      throw new Error('invalid datastore key: ' + s)
    }

    return new Key(s)
  }

  // Element retrieves the value of an element from the CRDT set.
  public async element (key: string): Promise<Uint8Array | null> {
    // We can only GET an element if it's part of the Set (in
    // "elements" and not in "tombstones").

    // If the key has a value in the store it means that it has been
    // written and is alive. putTombs will delete the value if all elems
    // are tombstoned, or leave the best one.

    const valueK = this.valueKey(key)

    try {
      const value = await this.store.get(valueK)

      return toUint8Array(value)
    } catch (error: unknown) {
      const err = error as Error
      if (
        ((err as any).code !== undefined && (err as any).code === 'ERR_NOT_FOUND') ||
        ((err as any).name !== undefined && (err as any).name === 'NotFoundError') ||
        ((err as any).code === undefined && (err as any).name === undefined &&
          !(
            err.message.includes('Not Found') ||
            err.message.includes('NotFound') ||
            err.message.includes('no such file or directory')
          )
        )
      ) {
        return null // Key does not exist
      }
      throw error // Re-throw any other errors
    }
  }

  // Return all elements in the set
  // TODO should probably return an async iterator
  public async elements (q: Query): Promise<Pair[]> {
    const srcQueryPrefixKey = new Key(q.prefix ?? '')
    const keyNamespacePrefix = this.keyPrefix(keysNs)
    const setQueryPrefix = keyNamespacePrefix
      .child(srcQueryPrefixKey)
      .toString()
    const vSuffix = `/${valueSuffix}`

    // We are going to be reading everything in the /set/ namespace which
    // will return items in the form:
    // * /set/<key>/value
    // * /set<key>/priority (a Uvarint)
    // TODO see benchmark notes in set.go

    const setQuery: Query = {
      prefix: setQueryPrefix
      // keysOnly: false, // TODO look at pair filter
    }

    const results = this.store.query(setQuery)

    const finalResults: Pair[] = []

    for await (const result of results) {
      // We will be getting keys in the form of
      // /namespace/keys/<key>/v and /namespace/keys/<key>/p
      // We discard anything not ending in /v and sanitize
      // those from:
      // /namespace/keys/<key>/v -> <key>

      // The fact that /v is set means it is not tombstoned,
      // as tombstoning removes /v and /p or sets them to
      // the best value.
      if (!result.key.toString().endsWith(vSuffix)) {
        continue
      }

      let key = result.key.toString()
      if (key.startsWith(keyNamespacePrefix.toString())) {
        key = key.slice(keyNamespacePrefix.toString().length)
      }

      if (key.endsWith('/' + valueSuffix)) {
        key = key.slice(0, key.length - ('/' + valueSuffix).length)
      }

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

    return this.store.has(valueK)
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
  // /namespace/<key>
  private keyPrefix (key: string): Key {
    return this.namespace.child(new Key(key))
  }

  // /namespace/elems/<key>
  private elemsPrefix (key: string): Key {
    return this.keyPrefix(elemsNs).child(new Key(key))
  }

  // /namespace/tombs/<key>
  private tombsPrefix (key: string): Key {
    return this.keyPrefix(tombsNs).child(new Key(key))
  }

  // /namespace/keys/<key>/value
  private valueKey (key: string): Key {
    return this.keyPrefix(keysNs)
      .child(new Key(key))
      .child(new Key(valueSuffix))
  }

  // /namespace/keys/<key>/priority
  private priorityKey (key: string): Key {
    return this.keyPrefix(keysNs)
      .child(new Key(key))
      .child(new Key(prioritySuffix))
  }

  // Check if a key is in the tombstones
  private async inTombsKeyID (key: string, id: string): Promise<boolean> {
    const k = this.tombsPrefix(key).child(new Key(id))
    return this.store.has(k)
  }

  // sets a value if priority is higher. When equal, it sets if the
  // value is lexicographically higher than the current value.
  private async setValue (
    writeStore: Datastore | Batch,
    key: string,
    id: string,
    value: Uint8Array,
    prio: bigint
  ): Promise<void> {
    // If this key was tombstoned already, do not store/update the value
    // at all.
    const deleted = await this.inTombsKeyID(key, id)
    if (deleted) {
      return
    }

    const curPrio = await this.getPriority(key)

    if (prio < curPrio) {
      return
    }

    const valueK = this.valueKey(key)

    if (prio === curPrio) {
      const curValue = await this.store.get(valueK)
      if (compareUint8Arrays(curValue, value) >= 0) {
        return
      }
    }

    await writeStore.put(valueK, value)
    await this.setPriority(writeStore, key, prio)

    // trigger put hook
    if (this.putHook !== undefined) {
      this.putHook(key, value)
    }
  }

  private async findBestValue (key: string, pendingTombIDs: string[]): Promise<{ value: Uint8Array, priority: bigint }> {
    const prefix = this.elemsPrefix(key)
    const q: KeyQuery = {
      prefix: prefix.toString()
    }

    const results = this.store.queryKeys(q)

    let bestValue: Uint8Array = new Uint8Array()
    let bestPriority: bigint = 0n
    let hasBestValue = false

    // range all the /namespace/elems/<key>/<block_cid>.
    for await (const r of results) {
      // console.log('findBestValue', 'r', r)
      let id = r.toString()

      // console.log('findBestValue', 'id', id)
      if (id.startsWith(prefix.toString())) {
        id = id.slice(prefix.toString().length)
        // console.log('findBestValue', 'id after', id)
      }

      try {
        if (!this.rawKey(id).isTopLevel()) {
          // our prefix matches blocks from other keys i.e. our
          // prefix is "hello" and we have a different key like
          // "hello/bye" so we have a block id like
          // "bye/<block>". If we got the right key, then the id
          // should be the block id only.
          continue
        }
      } catch {
        // ok if rawKey throws here, just continue
        continue
      }

      // if block is one of the pending tombIDs, continue
      if (pendingTombIDs.includes(id)) {
        // console.log('findBestValue', 'pendingTombIDs includes', pendingTombIDs)
        continue
      }

      // if tombstoned, continue
      const inTomb = await this.inTombsKeyID(key, id)
      if (inTomb) {
        // console.log('findBestValue', 'inTomb')
        continue
      }

      // get the block
      const mhash = dsKeyToMultihash(new Key(this.cleanKey(id)))
      const deltaCid = CID.createV1(0x70, mhash) // dag-protobuf
      const delta = await this.nodeGetter.getDelta(deltaCid)
      // console.log('findBestValue', 'delta', delta)

      if (delta.delta.priority < bestPriority) {
        // console.log('findBestValue', 'delta.delta.priority < bestPriority')
        continue
      }

      let greatestValueInDelta: Uint8Array = new Uint8Array()

      for (const elem of delta.delta.elements) {
        if (elem.key !== key) {
          continue
        }

        const v = elem.value
        if (compareUint8Arrays(greatestValueInDelta, v) < 0) {
          greatestValueInDelta = v
        }
      }

      if (delta.delta.priority > bestPriority) {
        hasBestValue = true
        bestValue = greatestValueInDelta
        bestPriority = delta.delta.priority
        continue
      }

      if (compareUint8Arrays(bestValue, greatestValueInDelta) < 0) {
        hasBestValue = true
        bestValue = greatestValueInDelta
      }
    }

    if (!hasBestValue) {
      throw new NoBestValueError()
    }

    return { value: bestValue, priority: bestPriority }
  }

  private async getPriority (key: string): Promise<bigint> {
    const prioK = this.priorityKey(key)
    try {
      const data = await this.store.get(prioK)
      if (data === null || data === undefined || data.length === 0) {
        return 0n
      }

      const [prio, n] = uvarint(data)
      if (n <= 0) {
        throw new Error('error decoding priority')
      }

      return prio - 1n
    } catch (error: unknown) {
      const err = error as Error
      if (
        (err as any).code === 'ERR_NOT_FOUND' ||
        err.message.includes('Not Found') || // memory datastore
        err.message.includes('NotFound') || // level datastore
        err.message.includes('no such file or directory') // fs datastore
      ) {
        // Return default priority if key is not found
        return 0n
      }
      throw error // Re-throw other errors
    }
  }

  async setPriority (
    writeStore: Datastore | Batch,
    key: string,
    prio: bigint
  ): Promise<void> {
    const maxVarintLen64 = 10

    const prioK = this.priorityKey(key)
    const buf = new Uint8Array(maxVarintLen64)
    const n = putUvarint(buf, prio + 1n)
    if (n === 0) {
      throw new Error('encoding priority failed')
    }

    await writeStore.put(prioK, buf.slice(0, n))
  }

  // putElems adds items to the "elems" set. It will also set current
  // values and priorities for each element. This needs to run in a lock,
  // as otherwise races may occur when reading/writing the priorities, resulting
  // in bad behaviours.
  //
  // Technically the lock should only affect the keys that are being written,
  // but with the batching optimization the locks would need to be hold until
  // the batch is written), and one lock per key might be way worse than a single
  // global lock in the end.
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
        // /namespace/elems/<key>/<id>
        const k = this.elemsPrefix(key).child(new Key(id))

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
    if (tombs.length === 0) {
      return
    }

    let store

    if ('batch' in this.store && typeof this.store.batch === 'function') {
      store = this.store.batch()
    } else {
      store = this.store
    }

    const deletedElems = new Map<string, string[]>()

    for (const e of tombs) {
      // /namespace/tombs/<key>/<id>
      const key = e.key
      const id = e.id
      const valueK = this.valueKey(key)
      deletedElems.set(key, [...deletedElems.get(key) ?? [], id])

      const dElems = deletedElems.get(key)

      if (dElems === undefined) {
        throw new Error('dElems is undefined')
      }

      try {
        const { value, priority } = await this.findBestValue(key, dElems)
        await this.store.put(new Key(this.cleanKey(valueK.toString())), value)
        await this.setPriority(store, this.cleanKey(key), priority)
      } catch (e) {
        if (e instanceof NoBestValueError) {
          await this.store.delete(new Key(this.cleanKey(valueK.toString())))
          await this.store.delete(this.priorityKey(this.cleanKey(key)))
        } else {
          throw e
        }
      }

      const k = this.tombsPrefix(key).child(new Key(id))
      await this.store.put(k, new Uint8Array())
    }

    if ('commit' in store && typeof store.commit === 'function') {
      await store.commit()
    }

    for (const [key, ids] of deletedElems) {
      if (this.deleteHook !== undefined) {
        this.deleteHook(key)
      }
    }
  }

  // Merge deltas into the set
  public async merge (delta: pb.delta.Delta, id: string): Promise<void> {
    await this.putTombs(delta.tombstones)
    await this.putElems(delta.elements, id, delta.priority)
  }
}
