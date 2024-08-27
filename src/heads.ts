import * as dagPb from '@ipld/dag-pb'
import { Mutex } from 'async-mutex'
import { Key } from 'interface-datastore'
import { CID } from 'multiformats/cid'
import {
  compareUint8Arrays,
  dsKeyToCidV1,
  multihashToDsKey,
  uvarint
} from './utils'
import type { Logger } from '@libp2p/logger'
import type { Batch, Datastore, Query } from 'interface-datastore'

type Cache = Record<string, bigint>

export class Heads {
  private readonly store: Datastore
  private cache: Cache
  private readonly cacheMux: Mutex
  public namespace: Key
  private readonly logger: Logger

  constructor (store: Datastore, namespace: Key, logger: Logger) {
    this.store = store
    this.namespace = namespace
    this.logger = logger
    this.cache = {}
    this.cacheMux = new Mutex()

    void this.primeCache()
  }

  private key (c: CID): Key {
    const multihashKey = multihashToDsKey(c.multihash.bytes)
    return this.namespace.child(multihashKey) // Attach the namespace to the key
  }

  // isHead returns if a given cid is among the current heads.
  public async isHead (c: CID): Promise<{ isHead: boolean, height: bigint }> {
    return this.cacheMux.runExclusive(async () => {
      for (const cachedCidStr in this.cache) {
        if (Object.prototype.hasOwnProperty.call(this.cache, cachedCidStr)) {
          // Check if the property is a direct property
          const cachedCid = CID.parse(cachedCidStr)
          if (c.equals(cachedCid)) {
            return { isHead: true, height: this.cache[cachedCidStr] }
          }
        }
      }
      return { isHead: false, height: 0n }
    })
  }

  public async len (): Promise<number> {
    return this.cacheMux.runExclusive(async () => {
      return Object.keys(this.cache).length
    })
  }

  public async replace (h: CID, c: CID, height: bigint): Promise<void> {
    this.logger(`replacing DAG head: ${h.toString()} -> ${c.toString()} (new height: ${height})`)
    // Check if the original CID is among the current heads
    const { isHead } = await this.isHead(h)
    if (!isHead) {
      throw new Error(`CID ${h.toString()} not found among current heads`)
    }

    let store
    let batching = false

    if ('batch' in this.store && typeof this.store.batch === 'function') {
      store = this.store.batch()
      batching = true
    } else {
      store = this.store
    }

    await this.write(store, c, height)

    await this.cacheMux.runExclusive(async () => {
      if (!batching) {
        this.cache[c.toString()] = height
      }

      await this.delete(store, h)

      if (!batching) {
        // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
        delete this.cache[h.toString()]
      }

      if (batching) {
        if ('commit' in store && typeof store.commit === 'function') {
          await store.commit()
        }

        // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
        delete this.cache[h.toString()]

        this.cache[c.toString()] = height
      }
    })
  }

  async write (store: Datastore | Batch, c: CID, height: bigint): Promise<void> {
    const buf = new ArrayBuffer(8) // 8 bytes should be enough for a BigInt
    const view = new DataView(buf)

    try {
      view.setBigUint64(0, height, true) // true for little-endian
    } catch (error) {
      throw new Error('Error encoding height')
    }

    const actualLength = this.getActualLength(view)
    this.logger(
      `Writing key: ${this.key(c).toString()}, CID: ${c.toString()}, height: ${height}`
    )

    await store.put(this.key(c), new Uint8Array(buf, 0, actualLength))
  }

  private getActualLength (view: DataView): number {
    let length = 8
    while (length > 0 && view.getUint8(length - 1) === 0) {
      length--
    }
    return length
  }

  private async delete (store: Datastore | Batch, c: CID): Promise<void> {
    const key = this.key(c)
    this.logger(`Deleting key: ${key.toString()}, CID: ${c.toString()}`)
    await store.delete(key)
  }

  public async add (c: CID, height: bigint): Promise<void> {
    this.logger(`Adding new DAG head: ${c.toString()} (height: ${height})`)

    await this.write(this.store, c, height)

    await this.cacheMux.runExclusive(async () => {
      this.cache[c.toString()] = height
    })
  }

  public async list (): Promise<{ heads: CID[], maxHeight: bigint }> {
    let maxHeight: bigint = BigInt(0)
    let heads: CID[] = []

    await this.cacheMux.runExclusive(async () => {
      heads = Object.keys(this.cache).map((cidStr) => CID.parse(cidStr))
      maxHeight = Object.values(this.cache)
        .map((value) => BigInt(value))
        .reduce((max, current) => (current > max ? current : max), BigInt(0))
    })

    heads.sort((a, b) => compareUint8Arrays(a.bytes, b.bytes))

    return { heads, maxHeight }
  }

  public async primeCache (): Promise<void> {
    const q: Query = {
      prefix: this.namespace.toString()
    }

    const results = this.store.query(q)

    for await (const r of results) {
      const keyStr = r.key.toString()

      // Strip the namespace
      const multibaseStr = keyStr.replace(this.namespace.toString() + '/', '')

      try {
        const headCid = dsKeyToCidV1(new Key(multibaseStr), dagPb.code)

        const [height, n] = uvarint(r.value.buffer)
        if (n <= 0) {
          throw new Error('error decoding height')
        }

        this.cache[headCid.toString()] = height
      } catch (error) {
        this.logger.error(`Failed to decode key: ${multibaseStr}`, error)
        throw error
      }
    }
  }
}
