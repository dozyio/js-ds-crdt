import { CID } from 'multiformats/cid';
import { Key } from 'interface-datastore';
import { MemoryDatastore } from 'datastore-core';
import type { Datastore } from 'interface-datastore';
import { logger } from '@libp2p/logger';
import { Heads } from './heads'; // Assuming the class is in heads.ts
import { Mutex } from 'async-mutex';

describe('Heads', () => {
  let store: Datastore;
  let namespace: Key;
  let log: ReturnType<typeof logger>;
  let heads: Heads;
  let cid1: CID;
  let cid2: CID;

  beforeEach(async () => {
    store = new MemoryDatastore(); // Using in-memory datastore for tests
    namespace = new Key('/testnamespace');
    log = logger('test');
    heads = new Heads(store, namespace, log);

    // Create some sample CIDs for testing
    cid1 = CID.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku');
    cid2 = CID.parse('bafybeigdyrztg6nbv3f2vzk3euvr63zjkvqlukmhhojz6kmiy7m7xvlhvq');
  });

  test('should add a new head', async () => {
    await heads.add(cid1, 1);

    const { isHead, height } = await heads.isHead(cid1);
    expect(isHead).toBe(true);
    expect(height).toBe(1);
  });

  test('should replace an existing head', async () => {
    await heads.add(cid1, 1);
    await heads.replace(cid1, cid2, 2);

    const { isHead: isHead1 } = await heads.isHead(cid1);
    const { isHead: isHead2, height: height2 } = await heads.isHead(cid2);

    expect(isHead1).toBe(false);
    expect(isHead2).toBe(true);
    expect(height2).toBe(2);
  });

  test('should correctly list all heads and max height', async () => {
    await heads.add(cid1, 1);
    await heads.add(cid2, 2);

    const { heads: headList, maxHeight } = await heads.list();

    expect(headList).toHaveLength(2);
    expect(maxHeight).toBe(2);
    expect(headList).toContainEqual(cid1);
    expect(headList).toContainEqual(cid2);
  });

  test('should correctly prime the cache from the datastore', async () => {
    await heads.add(cid1, 1);
    await heads.add(cid2, 2);

    const newHeads = new Heads(store, namespace, log);
    await newHeads.primeCache();

    const { isHead: isHead1, height: height1 } = await newHeads.isHead(cid1);
    const { isHead: isHead2, height: height2 } = await newHeads.isHead(cid2);

    expect(isHead1).toBe(true);
    expect(height1).toBe(1);
    expect(isHead2).toBe(true);
    expect(height2).toBe(2);
  });

  test('should return the correct number of heads', async () => {
    await heads.add(cid1, 1);
    await heads.add(cid2, 2);

    const length = await heads.len();

    expect(length).toBe(2);
  });

  test('should handle delete operations correctly', async () => {
    await heads.add(cid1, 1);
    await heads.replace(cid1, cid2, 2);

    const { isHead: isHead1 } = await heads.isHead(cid1);
    const { isHead: isHead2 } = await heads.isHead(cid2);

    expect(isHead1).toBe(false);
    expect(isHead2).toBe(true);
  });

  it('should handle non-batching datastore correctly', async () => {
    const store = new MemoryDatastore({ batching: false });
    const namespace = new Key('/testnamespace');
    const log = logger('test');
    const heads = new Heads(store, namespace, log);

    const cid1 = CID.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku');
    const cid2 = CID.parse('bafybeigdyrztg6nbv3f2vzk3euvr63zjkvqlukmhhojz6kmiy7m7xvlhvq');

    await heads.add(cid1, 1);
    await heads.add(cid2, 2);

    const { heads: headList } = await heads.list();
    expect(headList).toHaveLength(2);
  });

  it('should handle concurrent additions and replacements', async () => {
    const store = new MemoryDatastore();
    const namespace = new Key('/testnamespace');
    const log = logger('test');
    const heads = new Heads(store, namespace, log);

    const cid1 = CID.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku');
    const cid2 = CID.parse('bafybeigdyrztg6nbv3f2vzk3euvr63zjkvqlukmhhojz6kmiy7m7xvlhvq');

    await Promise.all([
      heads.add(cid1, 1),
      heads.add(cid1, 1).then(() => heads.replace(cid1, cid2, 2)),
    ]);

    const { heads: headList } = await heads.list();
    expect(headList).toHaveLength(1);
    expect(headList).toContainEqual(cid2);
  });

  it('should not replace a non-existing CID', async () => {
    const store = new MemoryDatastore();
    const namespace = new Key('/testnamespace');
    const log = logger('test');
    const heads = new Heads(store, namespace, log);

    const cid1 = CID.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku');
    const cid2 = CID.parse('bafybeigdyrztg6nbv3f2vzk3euvr63zjkvqlukmhhojz6kmiy7m7xvlhvq');
    const cid3 = CID.parse('bafybeia7xyvnabemkgjjg7op5pjbyxgniowp7p37oayhtf5slwuwqe4lyq'); // Some other CID

    await heads.add(cid1, 1); // First, add cid1 to the heads

    await expect(heads.replace(cid3, cid2, 2)).rejects.toThrow()
  });

  it('should handle very large heights', async () => {
    const store = new MemoryDatastore();
    const namespace = new Key('/testnamespace');
    const log = logger('test');
    const heads = new Heads(store, namespace, log);

    const cid1 = CID.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku');
    const largeHeight = Number.MAX_SAFE_INTEGER;

    await heads.add(cid1, largeHeight);
    const { height } = await heads.isHead(cid1);
    expect(height).toBe(largeHeight);
  });

  it('should fail gracefully on datastore errors', async () => {
    // Simulate a failing datastore
    const store = new MemoryDatastore();
    store.put = async () => { throw new Error('Simulated failure'); };
    const namespace = new Key('/testnamespace');
    const log = logger('test');
    const heads = new Heads(store, namespace, log);

    const cid1 = CID.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku');

    await expect(heads.add(cid1, 1)).rejects.toThrow('Simulated failure');
  });
});
