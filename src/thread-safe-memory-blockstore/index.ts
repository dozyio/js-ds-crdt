import { Mutex } from 'async-mutex'
import { BaseBlockstore } from 'blockstore-core'
import { NotFoundError } from 'interface-store'
import { base32 } from 'multiformats/bases/base32'
import { CID } from 'multiformats/cid'
import * as raw from 'multiformats/codecs/raw'
import * as Digest from 'multiformats/hashes/digest'
import type { Pair } from 'interface-blockstore'
import type { Await, AwaitIterable } from 'interface-store'

export class ThreadSafeMemoryBlockstore extends BaseBlockstore {
  private readonly data: Map<string, Uint8Array>
  private readonly mux: Mutex = new Mutex()

  constructor () {
    super()

    this.data = new Map()
  }

  // eslint-disable-next-line require-await
  put (key: CID, val: Uint8Array): Await<CID> {
    this.mux
      .runExclusive(() => {
        this.data.set(base32.encode(key.multihash.bytes), val)
      }).catch((err) => {
        throw err
      })

    return key
  }

  get (key: CID): Await<Uint8Array> {
    let buf: Uint8Array | undefined

    this.mux
      .runExclusive(() => {
        buf = this.data.get(base32.encode(key.multihash.bytes))
      }).catch((err) => {
        throw err
      })

    if (buf == null) {
      throw new NotFoundError()
    }

    return buf
  }

  has (key: CID): Await<boolean> {
    let has: boolean = false

    this.mux
      .runExclusive(() => {
        has = this.data.has(base32.encode(key.multihash.bytes))
      }).catch((err) => {
        throw err
      })

    return has
  }

  async delete (key: CID): Promise<void> {
    this.mux
      .runExclusive(() => {
        this.data.delete(base32.encode(key.multihash.bytes))
      }).catch((err) => {
        throw err
      })
  }

  // Not ideal as we snapshot the data before iterating over it which may cause
  // memory issues if the store is large.
  // The other way would be to lock for the entire duration of the iteration
  // which would block other operations.
  async * getAll (): AwaitIterable<Pair> {
    const entries = await this.mux.runExclusive(() => {
      // Create a snapshot of the current data entries to safely iterate over them outside the lock
      return Array.from(this.data.entries())
    })

    for (const [key, value] of entries) {
      yield {
        cid: CID.createV1(raw.code, Digest.decode(base32.decode(key))),
        block: value
      }
    }
  }
}
