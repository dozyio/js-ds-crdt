/* eslint-disable no-console */
import * as codec from '@ipld/dag-pb'
import * as Block from 'multiformats/block'
import { type CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { delta } from './pb/delta'
import type { Logger } from '@libp2p/interface'
import type { Blockstore } from 'interface-blockstore'
import type { AbortOptions } from 'interface-store'

export interface DeltaOption {
  delta?: delta.Delta
  node?: codec.PBNode
  err?: Error
}

const { createNode } = codec

export class CrdtNodeGetter {
  private readonly blockstore: Blockstore
  private readonly logger: Logger

  constructor (blockstore: Blockstore, logger: Logger) {
    this.blockstore = blockstore
    this.logger = logger
  }

  async getDelta (
    cid: CID,
    options?: AbortOptions
  ): Promise<{ node: codec.PBNode, delta: delta.Delta }> {
    this.logger('getting delta', cid.toString())
    const node = await this.getNode(cid, options)
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
      for await (const { node, error } of self.getNodes(cids, options)) {
        if (error !== undefined) {
          yield { err: error }
          continue
        }
        try {
          if (node?.Data === undefined) {
            continue
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

  private async getNode (
    cid: CID,
    options?: AbortOptions
  ): Promise<codec.PBNode> {
    this.logger('getting node', cid.toString())
    const block = await this.blockstore.get(cid, options)
    this.logger('block', block)
    const node = await Block.decode({ bytes: block, codec, hasher })
    this.logger('node', node)

    const links: codec.PBLink[] = []
    for (const [name, cid] of node.links()) {
      links.push({
        Name: name,
        Hash: cid
      })
    }

    return { Data: node.value.Data, Links: links }
  }

  private async * getNodes (
    cids: CID[],
    options?: AbortOptions
  ): AsyncIterable<{ node?: codec.PBNode, error?: Error }> {
    for (const cid of cids) {
      try {
        const node = await this.getNode(cid, options)
        yield { node }
      } catch (error: any) {
        yield { node: undefined, error }
      }
    }
  }

  private extractDelta (node: codec.PBNode): delta.Delta {
    if (node?.Data === undefined || node.Data.length === 0) {
      throw new Error('Node has no data')
    }

    try {
      // Attempt to decode the delta; if this fails, an error should be thrown
      const d = delta.Delta.decode(node.Data)
      return d
    } catch (err) {
      // If decoding fails, treat it as an error similar to how Go's proto.Unmarshal would fail
      throw new Error('Failed to decode delta data')
    }
  }

  static async makeNode (d: delta.Delta, heads: CID[]): Promise<codec.PBNode> {
    const links: codec.PBLink[] = []
    for (const head of heads) {
      links.push({ Name: '', Hash: head })
    }

    return createNode(delta.Delta.encode(d), links)
  }
}
