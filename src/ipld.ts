import { CID } from 'multiformats/cid';
import type { Blockstore } from 'interface-blockstore';
import { delta } from './pb/delta';
import type { AbortOptions } from 'interface-store';
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import * as codec from '@ipld/dag-pb'
import * as Block from 'multiformats/block'

export interface DeltaOption {
  delta?: delta.Delta;
  node?: codec.PBNode;
  err?: Error;
}

const { createNode } = codec

class CrdtNodeGetter {
  private readonly blockstore: Blockstore;

  constructor(blockstore: Blockstore) {
    this.blockstore = blockstore;
  }

  async getDelta(cid: CID, options?: AbortOptions): Promise<{ node: codec.PBNode; delta: delta.Delta }> {
    const node = await this.getNode(cid, options);
    const delta = this.extractDelta(node);
    return { node, delta };
  }

  async getPriority(cid: CID, options?: AbortOptions): Promise<bigint> {
    const { delta } = await this.getDelta(cid, options);
    return delta.priority;
  }

  getDeltas(cids: CID[], options?: AbortOptions): AsyncIterable<DeltaOption> {
    const self = this;

    async function* generator() {
      for await (const { node, error } of self.getNodes(cids, options)) {
        if (error) {
          yield { err: error };
          continue;
        }
        try {
          if (!node || !node.Data) {
            continue;
          }
          const delta = self.extractDelta(node);
          yield { delta, node };
        } catch (err) {
          yield { err: err as Error };
        }
      }
    }

    return generator();
  }

  private async getNode(cid: CID, options?: AbortOptions): Promise<codec.PBNode> {
    const block = await this.blockstore.get(cid, options);
    const node = await Block.decode({ bytes: block, codec, hasher })

    let links: codec.PBLink[] = []
    for (const [name, cid] of node.links()) {
      links.push({
        Name: name,
        Hash: cid
      })
    }

    return { Data: node.value.Data, Links: links };
  }

  private async *getNodes(cids: CID[], options?: AbortOptions): AsyncIterable<{ node?: codec.PBNode; error?: Error }> {
    for (const cid of cids) {
      try {
        const node = await this.getNode(cid, options);
        yield { node };
      } catch (error: any) {
        yield { node: undefined, error };
      }
    }
  }

  private extractDelta(node: codec.PBNode): delta.Delta {
    if (!node || !node.Data || node.Data.length === 0) {
      throw new Error('Node has no data');
    }

    try {
      // Attempt to decode the delta; if this fails, an error should be thrown
      const d = delta.Delta.decode(node.Data);
      return d;
    } catch (err) {
      // If decoding fails, treat it as an error similar to how Go's proto.Unmarshal would fail
      throw new Error('Failed to decode delta data');
    }
  }

  static async makeNode(d: delta.Delta, heads: CID[]): Promise<codec.PBNode> {
    let links: codec.PBLink[] = []
    for (const head of heads) {
      links.push({ Name: '', Hash: head });
    }

    return createNode(delta.Delta.encode(d), links)

    // const block = await Block.encode({ value, codec, hasher })
    //
    // return Block.decode({ bytes: block, codec, hasher })
  }
}

export { CrdtNodeGetter };
