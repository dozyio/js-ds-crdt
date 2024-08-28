import * as codec from '@ipld/dag-pb'
import * as Block from 'multiformats/block'
import { type CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { delta } from './pb/delta'
import type { Logger } from '@libp2p/interface'
import type { Blockstore } from 'interface-blockstore'
import type { AbortOptions } from 'interface-store'
import type { BlockView } from 'multiformats'

export interface DeltaOption {
  delta?: delta.Delta
  node?: BlockView
  err?: Error
}

const { createNode } = codec

export class CRDTNodeGetter {
  private readonly blockstore: Blockstore
  private readonly logger: Logger

  constructor (blockstore: Blockstore, logger: Logger) {
    this.blockstore = blockstore
    this.logger = logger
  }

  async getDelta (
    cid: CID,
    options?: AbortOptions
  ): Promise<{ node: BlockView, delta: delta.Delta }> {
    this.logger('getting delta', cid.toString())
    const node = await this.get(cid, options)
    this.logger('getDelta node', node)
    const delta = this.extractDelta(node)
    return { node, delta }
  }

  async getPriority (cid: CID, options?: AbortOptions): Promise<bigint> {
    this.logger('getting priority', cid.toString())
    const { delta } = await this.getDelta(cid, options)
    this.logger('priority', delta.priority)
    return delta.priority
  }

  getDeltas (cids: CID[], options?: AbortOptions): AsyncIterable<DeltaOption> {
    const self = this

    async function * generator (): AsyncIterable<DeltaOption> {
      for await (const { node, error } of self.getMany(cids, options)) {
        if (error !== undefined) {
          yield { err: error }
          continue
        }

        if (node === null || node === undefined) {
          yield { err: new Error('Node is undefined') }
          continue
        }

        try {
          if (typeof node.value !== 'object' || node.value === null || !('Data' in node.value)) {
            yield { err: new Error('Node has no data undefined') }
            continue
          }

          const data = (node.value as { Data: Uint8Array }).Data

          if (data.length === 0) {
            yield { err: new Error('Node has 0 data length') }
            continue
            // throw new Error('Node has no data')
          }

          const delta = self.extractDelta(node)
          yield { delta, node }
        } catch (err) {
          yield { err: err as Error }
        }
      }
    }

    return generator()
  }

  private async get (
    cid: CID,
    options?: AbortOptions
  ): Promise<BlockView> {
    this.logger('getting node', cid.toString())
    const block = await this.blockstore.get(cid, options)
    this.logger('block', block)
    const node = await Block.decode({ bytes: block, codec, hasher })
    this.logger('node', node)

    return node
  }

  private async * getMany (
    cids: CID[],
    options?: AbortOptions
  ): AsyncIterable<{ node?: BlockView, error?: Error }> {
    for (const cid of cids) {
      try {
        const node = await this.get(cid, options)
        yield { node }
      } catch (error: any) {
        yield { node: undefined, error }
      }
    }
  }

  private extractDelta (node: BlockView): delta.Delta {
    if (typeof node.value !== 'object' || node.value === null || !('Data' in node.value)) {
      throw new Error('Node has no data')
    }

    const data = (node.value as { Data: Uint8Array }).Data

    if (data.length === 0) {
      throw new Error('Node has no data')
    }

    try {
      // Attempt to decode the delta; if this fails, an error should be thrown
      const d = delta.Delta.decode(data)
      return d
    } catch (err) {
      // If decoding fails, treat it as an error similar to how Go's proto.Unmarshal would fail
      throw new Error('Failed to decode delta data')
    }
  }

  public static async makeNode (d: delta.Delta | null, heads: CID[]): Promise<BlockView> {
    let data: Uint8Array = new Uint8Array()

    if (d != null) {
      data = delta.Delta.encode(d)
    }

    const links: codec.PBLink[] = []
    for (const head of heads) {
      links.push({ Name: '', Hash: head })
    }

    const pbnode = createNode(data, links)
    const block = await Block.encode({ value: pbnode, codec, hasher })

    return block
  }
}
