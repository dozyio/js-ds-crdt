import * as codec from '@ipld/dag-pb'
import { prefixLogger } from '@libp2p/logger'
import { MemoryDatastore } from 'datastore-core/memory'
import { Key } from 'interface-datastore'
import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { describe, it, expect, beforeEach } from 'vitest'
import { Datastore, type MyLibp2pServices } from '../src/crdt'
import { PubSubBroadcaster } from '../src/pubsub_broadcaster'
import { createNode, createReplicas, waitForPropagation, waitUntil } from './utils'
import type { Libp2p } from '@libp2p/interface'
import type { HeliaLibp2p } from 'helia'
// import debug from 'weald'

describe('Datastore', () => {
  let store: MemoryDatastore
  let namespace: Key
  let dagService: HeliaLibp2p<Libp2p<MyLibp2pServices>>
  let broadcaster: any
  let options: any
  let datastore: Datastore

  beforeEach(async () => {
    store = new MemoryDatastore()
    namespace = new Key('testNamespace')
    dagService = await createNode()
    broadcaster = new PubSubBroadcaster(dagService.libp2p, 'test', prefixLogger('crdt').forComponent('pubsub'))

    options = {
      logger: console,
      rebroadcastInterval: 1000,
      repairInterval: 2000,
      logInterval: 3000,
      numWorkers: 1,
      dagSyncerTimeout: 1000,
      maxBatchDeltaSize: 1000,
      multiHeadProcessing: false
    }

    datastore = new Datastore(
      store,
      namespace,
      dagService,
      broadcaster,
      options
    )
  })

  it('should initialize correctly', () => {
    expect(datastore.options).toEqual(options)
    expect(datastore.store).toBe(store)
    expect(datastore.namespace).toBe(namespace)
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
    await datastore.markDirty()
    expect(await datastore.isDirty()).toBe(true)

    await datastore.markClean()
    expect(await datastore.isDirty()).toBe(false)
  })

  it('should process nodes correctly', async () => {
    const delta = { priority: 1n, elements: [], tombstones: [] } as any
    const node = codec.createNode(new Uint8Array(), [])

    // Simulate adding the node to the DAG service's blockstore (optional, if necessary)
    const block = await Block.encode({ value: node, codec, hasher })
    await dagService.blockstore.put(block.cid, block.bytes)

    const cid = block.cid

    // Now, process the node
    await datastore.processNode(cid, 1n, delta, node)

    // Check if the node is marked as processed
    const isProcessed = await datastore.isProcessed(cid)
    expect(isProcessed).toBe(true)
  })

  it('should rebroadcast heads', async () => {
    const cid = CID.parse(
      'bafyreigx2zx5k2gxejyfmksls5bl6bhybcq4aqmhft7y2jxup4lgjxbiou'
    )
    await datastore.heads.add(cid, 1n)

    await datastore.rebroadcastHeads()
    // Since there's no real broadcast logic in this test, we just check that no errors were thrown.
    expect(true).toBe(true)
  })

  it('should add and retrieve data correctly', async () => {
    const key = new Key('/test/key')
    const value = new Uint8Array([1, 2, 3])

    await datastore.put(key, value)
    const retrievedValue = await datastore.get(key)

    expect(retrievedValue).toEqual(value)
  })

  it('should return null when retrieving non-existent key', async () => {
    const key = new Key('/nonexistent/key')

    const retrievedValue = await datastore.get(key)

    expect(retrievedValue).toBeNull()
  })

  it('should delete data correctly', async () => {
    const key = new Key('/test/key')
    const value = new Uint8Array([1, 2, 3])

    await datastore.put(key, value)
    await datastore.delete(key)

    const retrievedValue = await datastore.get(key)
    expect(retrievedValue).toBeNull()
  })

  it('should put and get values', async () => {
    const key = new Key('hi')
    await datastore.put(key, new Uint8Array(Buffer.from('hola')))

    const value = await datastore.get(key)
    expect(value).toEqual(new Uint8Array(Buffer.from('hola')))
  })

  describe('Replication', () => {
    it('should replicate data across replicas', async () => {
      const replicas = await createReplicas(2, 't1')

      const key = new Key('/test/key')
      const value = Buffer.from('hola')

      // Put the value in the first replica
      await replicas[0].put(key, value)

      await waitForPropagation(15000, replicas[1], key, value)

      // console.log('replica[0] DAG')
      // await replicas[0].PrintDAG()
      //
      // console.log('replica[1] DAG')
      // await replicas[1].PrintDAG()

      // const list0 = []
      // for await (const { key, value } of replicas[0].store.query({})) {
      //   list0.push(key)
      // }
      // console.log('LIST0 ALL THE VALUES', list0)
      //
      // const list1 = []
      // for await (const { key, value } of replicas[1].store.query({})) {
      //   list1.push(key)
      // }
      // console.log('LIST1 ALL THE VALUES', list1)

      // Wait for the value to be available in all replicas
      for (const replica of replicas) {
        await waitUntil(() => replica.get(key) !== null)
        const replicatedValue = await replica.get(key)
        expect(replicatedValue).toEqual(value)
      }
    }, 10000)

    it('should replicate updates across replicas', async () => {
      const replicas = await createReplicas(6, 't2')

      const key = new Key('/test/key')
      let value

      // Put the value in the first replica
      for (let i = 0; i < 20; i++) {
        value = Buffer.from(`hola${i}`)
        await replicas[0].put(key, value)
      }

      await waitForPropagation(15000, replicas[replicas.length - 1], key, value)

      // Wait for the value to be available in all replicas
      for (const replica of replicas) {
        await waitUntil(() => replica.get(key) !== null)
        const replicatedValue = await replica.get(key)
        expect(replicatedValue).toEqual(value)
      }
    }, 20000)

    it('should replicate large data across replicas', async () => {
      const replicas = await createReplicas(3, 't4')

      const key = new Key('/test/large')
      const sizeInBytes = 1024 * 1024 // 1 MB
      const asciiValue = 97 // ASCII value of 'a'
      const largeValue = new Uint8Array(sizeInBytes).fill(asciiValue)

      await replicas[0].put(key, largeValue)

      await waitForPropagation(15000, replicas[replicas.length - 1], key, largeValue)

      for (const replica of replicas) {
        const replicatedValue = await replica.get(key)
        expect(replicatedValue).toEqual(largeValue)
      }
    }, 20000)

    it('should delete data across replicas', async () => {
      const replicas = await createReplicas(3, 't8')

      const key = new Key('/test/delete')
      const value = Buffer.from('to be deleted')

      await replicas[0].put(key, value)

      await waitForPropagation(4000, replicas[replicas.length - 1], key, value)

      // Delete the value from the first replica
      await replicas[0].delete(key)

      await waitForPropagation(2000)

      for (const replica of replicas) {
        const deletedValue = await replica.get(key)
        expect(deletedValue).toBeNull()
      }
    }, 10000)
  })

  // describe('Interop', () => {
  //   it('should replicate data to Go', async () => {
  //     debug.enable('*') // 'crdt*,*crdt:trace')
  //     const remote = '/ip4/127.0.0.1/tcp/49477/p2p/12D3KooWEkgRTTXGsmFLBembMHxVPDcidJyqFcrqbm9iBE1xhdXq'
  //     const ma = multiaddr(remote)
  //
  //     const replicas = await createReplicas(1, 'globaldb-example', ma)
  //
  //     for (let i = 0; i < 500; i++) {
  //       const key = new Key(`/test/key${i}`)
  //       const value = Buffer.from(`hola${i}`)
  //       await replicas[0].put(key, value)
  //     }
  //
  //     await waitForPropagation(10000)
  //
  //     // console.log('replica[0] DAG')
  //     // await replicas[0].printDAG()
  //
  //     // const list0 = []
  //     // for await (const { key, value } of replicas[0].store.query({})) {
  //     //   list0.push(key)
  //     // }
  //     // console.log('LIST0 ALL THE VALUES', list0)
  //
  //     expect(true).toEqual(true)
  //   }, 20000)
  //
  //   it.skip('should wait for propagation from 3rd party', async () => {
  //     debug.enable('*') // 'crdt*,*crdt:trace')
  //     const remote = '/ip4/127.0.0.1/tcp/49477/p2p/12D3KooWEkgRTTXGsmFLBembMHxVPDcidJyqFcrqbm9iBE1xhdXq'
  //     const ma = multiaddr(remote)
  //
  //     const replicas = await createReplicas(1, 'globaldb-example', ma)
  //
  //     await waitForPropagation(15000)
  //
  //     // console.log('replica[0] DAG')
  //     // await replicas[0].printDAG()
  //
  //     const stats = await replicas[0].internalStats()
  //
  //     replicas[0].logger(`Number of heads: ${stats.heads.length}`)
  //     replicas[0].logger(`Max height: ${stats.maxHeight}`)
  //     replicas[0].logger(`Queued jobs: ${stats.queuedJobs}`)
  //     replicas[0].logger(`Dirty: ${await replicas[0].isDirty()}`)
  //
  //
  //     expect(true).toEqual(true)
  //   }, 20000)
  // })
})
