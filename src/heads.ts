import { CID } from 'multiformats/cid';
import type { Datastore, Query } from 'interface-datastore';
import { Key } from 'interface-datastore';
import type { Logger } from '@libp2p/logger';
import { Mutex } from 'async-mutex';
import * as dagPb from '@ipld/dag-pb';
import { arrayBufferToBigInt, bigintToUint8Array, dsKeyToCidV1, multihashToDsKey } from './utils';

interface Cache {
  [cid: string]: bigint;
}

export class Heads {
  private store: Datastore;
  private cache: Cache;
  private cacheMux: Mutex;
  public namespace: Key;
  private logger: Logger;

  constructor(store: Datastore, namespace: Key, logger: Logger) {
    this.store = store;
    this.namespace = namespace;
    this.logger = logger;
    this.cache = {};
    this.cacheMux = new Mutex();
  }

  private key(c: CID): Key {
    const multihashKey = multihashToDsKey(c.multihash.bytes);
    return this.namespace.child(multihashKey); // Attach the namespace to the key
  }

  public async isHead(c: CID): Promise<{ isHead: boolean, height: bigint}> {
    return this.cacheMux.runExclusive(async () => {
      for (const cachedCidStr in this.cache) {
        const cachedCid = CID.parse(cachedCidStr);
        if (c.equals(cachedCid)) {
          return { isHead: true, height: this.cache[cachedCidStr] };
        }
      }
      return { isHead: false, height: 0n };
    });
  }

  public async len(): Promise<number> {
    return this.cacheMux.runExclusive(async () => {
      return Object.keys(this.cache).length;
    });
  }

public async replace(h: CID, c: CID, height: bigint): Promise<void> {
    let store = this.store;

    // Check if the original CID is among the current heads
    const { isHead } = await this.isHead(h);
    if (!isHead) {
        throw new Error(`CID ${h.toString()} not found among current heads`);
    }

    if ('batch' in store && typeof store.batch === 'function') {
        const batch = store.batch();
        await this.write(store, c, height);
        await this.delete(store, h);
        await batch.commit();
    } else {
        await this.write(store, c, height);
        await this.delete(store, h);
    }

    await this.cacheMux.runExclusive(async () => {
        delete this.cache[h.toString()];
        this.cache[c.toString()] = height;
    });
}

  private async write(store: Datastore, c: CID, height: bigint): Promise<void> {
    // const buf = new Uint8Array(8);
    // const view = new DataView(buf.buffer);
    // view.setUint32(0, height, true);

    const key = this.key(c); // Now includes the namespace
    this.logger.trace(`Writing key: ${key.toString()}, CID: ${c.toString()}, height: ${height}`);

    await store.put(key, bigintToUint8Array(height));
  }

  private async delete(store: Datastore, c: CID): Promise<void> {
    const key = this.key(c);
    this.logger.trace(`Deleting key: ${key.toString()}, CID: ${c.toString()}`);
    await store.delete(key);
  }

  public async add(c: CID, height: bigint): Promise<void> {
    this.logger.trace(`Adding new DAG head: ${c} (height: ${height})`);
    await this.write(this.store, c, height);

    await this.cacheMux.runExclusive(async () => {
      this.cache[c.toString()] = height;
    });
  }

  public async list(): Promise<{ heads: CID[], maxHeight: bigint}> {
    return this.cacheMux.runExclusive(async () => {
      const heads = Object.keys(this.cache).map(cidStr => CID.parse(cidStr));
      const maxHeight = Object.values(this.cache)
    .map(value => BigInt(value))
    .reduce((max, current) => (current > max ? current : max), BigInt(0));

      heads.sort((a, b) => Buffer.compare(a.bytes, b.bytes));

      return { heads, maxHeight };
    });
  }

  public async primeCache(): Promise<void> {
    const q: Query = {
      prefix: this.namespace.toString(),
    };

    const results = this.store.query(q);

    for await (const r of results) {
      const keyStr = r.key.toString();

      // Strip the namespace
      const multibaseStr = keyStr.replace(this.namespace.toString() + '/', '');

      try {
        const headCid = dsKeyToCidV1(new Key(multibaseStr), dagPb.code);

        const height = arrayBufferToBigInt(r.value.buffer);
        this.cache[headCid.toString()] = height;

      } catch (error) {
        console.error(`Failed to decode key: ${multibaseStr}`, error);
        throw error;
      }
    }
  }
}
