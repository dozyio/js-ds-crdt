import { describe, it, expect, beforeEach } from 'vitest'
import { Datastore } from '../src/crdt'
import { CID } from 'multiformats/cid'
import { MemoryDatastore } from 'datastore-core/memory'
import { Key } from 'interface-datastore'
import type { Helia } from 'helia'
import * as dagPb from '@ipld/dag-pb'
import * as Block from 'multiformats/block'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import * as codec from '@ipld/dag-pb'
import { MemoryBlockstore } from 'blockstore-core'

describe('Datastore', () => {
  let store: MemoryDatastore
  let namespace: Key
  let dagService: Helia
  let broadcaster: any
  let options: any
  let datastore: Datastore

  beforeEach(() => {
    store = new MemoryDatastore()
    namespace = new Key('testNamespace')
    dagService = {
      blockstore: new MemoryBlockstore(),
    } as unknown as Helia

    broadcaster = {
      broadcast: async (data: Uint8Array) => {},
      setHandler: (handler: (data: Uint8Array) => Promise<void>) => {},
    }

    options = {
      logger: console,
      rebroadcastInterval: 1000,
      repairInterval: 2000,
      logInterval: 3000,
      numWorkers: 1,
      dagSyncerTimeout: 1000,
      maxBatchDeltaSize: 1000,
      multiHeadProcessing: false,
    }

    datastore = new Datastore(
      store,
      namespace,
      dagService,
      broadcaster,
      options,
    )
  })

  it('should initialize correctly', () => {
    expect(datastore.options).toEqual(options)
    expect(datastore['store']).toBe(store)
    expect(datastore['namespace']).toBe(namespace)
  })

  it('should add and retrieve elements from the set', async () => {
    const key = new Key('key1')
    const value = new Uint8Array([1, 2, 3])

    await datastore.put(key, value)
    const storedValue = await datastore.get(key)

    expect(storedValue).toEqual(value)
  })

  it('should delete elements from the set', async () => {
    const key = new Key('key2')
    const value = new Uint8Array([4, 5, 6])

    await datastore.put(key, value)
    await datastore.delete(key)
    const storedValue = await datastore.get(key)

    expect(storedValue).toBeNull()
  })

  it('should mark the datastore as dirty and clean', async () => {
    datastore.MarkDirty()
    expect(await datastore.IsDirty()).toBe(true)

    await datastore.MarkClean()
    expect(await datastore.IsDirty()).toBe(false)
  })

  // it('should process nodes correctly', async () => {
  //   const delta = { priority: 1n, elements: [], tombstones: [] } as any;
  //   const cid = CID.parse('bafyreigx2zx5k2gxejyfmksls5bl6bhybcq4aqmhft7y2jxup4lgjxbiou');
  //   const node = dagPb.createNode(new Uint8Array(), []);
  //
  //   await datastore.processNode(cid, 1n, delta, node);
  //
  //   const isProcessed = await datastore.isProcessed(cid);
  //   expect(isProcessed).toBe(true);
  // });

  it('should process nodes correctly', async () => {
    const delta = { priority: 1n, elements: [], tombstones: [] } as any
    const node = dagPb.createNode(new Uint8Array(), [])

    // Simulate adding the node to the DAG service's blockstore (optional, if necessary)
    const block = await Block.encode({ value: node, codec: dagPb, hasher })
    await dagService.blockstore.put(block.cid, block.bytes)

    const cid = block.cid

    // Now, process the node
    await datastore.processNode(cid, 1n, delta, node)

    // Check if the node is marked as processed
    const isProcessed = await datastore.isProcessed(cid)
    expect(isProcessed).toBe(true)
  })

  it.only('should handle branch processing', async () => {
    const blockstore = new MemoryBlockstore()
    const datastore = new Datastore(
      store,
      namespace,
      dagService,
      broadcaster,
      options,
    )

    // Create a node and add it to the blockstore
    const node = dagPb.createNode(new Uint8Array(), [])
    const block = await Block.encode({ value: node, codec, hasher })

    // Store the block in the blockstore
    await blockstore.put(block.cid, block.bytes)

    const cid = block.cid

    // Simulate processing the branch with this CID
    await datastore.handleBranch(cid, cid)

    // Verify that the node has been marked as processed
    const isProcessed = await datastore.isProcessed(cid)
    expect(isProcessed).toBe(true)
  })

  it('should rebroadcast heads', async () => {
    const cid = CID.parse(
      'bafyreigx2zx5k2gxejyfmksls5bl6bhybcq4aqmhft7y2jxup4lgjxbiou',
    )
    await datastore['heads'].add(cid, 1n)

    await datastore['rebroadcastHeads']()
    // Since there's no real broadcast logic in this test, we just check that no errors were thrown.
    expect(true).toBe(true)
  })
})
