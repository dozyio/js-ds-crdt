import { prefixLogger } from '@libp2p/logger'
import { MemoryDatastore } from 'datastore-core/memory'
import { Key } from 'interface-datastore'
import { CID } from 'multiformats/cid'
import { describe, it, expect, beforeEach } from 'vitest'
// import debug from 'weald'
import { CRDTDatastore, type CRDTLibp2pServices } from '../src/crdt'
import { CRDTNodeGetter } from '../src/ipld'
import { PubSubBroadcaster } from '../src/pubsub_broadcaster'
import {
  createNode,
  createReplicas,
  waitForPropagation,
  waitUntil
} from './utils'
import type { Libp2p } from '@libp2p/interface'
import type { HeliaLibp2p } from 'helia'

// debug.enable('crdt*,-crdt0:crdt')
// debug.enable('*')

// for interop tests - see https://github.com/dozyio/ds-crdt-interop

describe('Datastore', () => {
  describe('Single node', () => {
    let store: MemoryDatastore
    let namespace: Key
    let dagService: HeliaLibp2p<Libp2p<CRDTLibp2pServices>>
    let broadcaster: any
    let options: any
    let datastore: CRDTDatastore

    beforeEach(async () => {
      store = new MemoryDatastore()
      namespace = new Key('testNamespace')
      dagService = await createNode()
      broadcaster = new PubSubBroadcaster(
        dagService.libp2p,
        'test',
        prefixLogger('crdt').forComponent('pubsub')
      )

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

      datastore = new CRDTDatastore(
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
      let storedValue = await datastore.get(key)
      expect(storedValue).toEqual(value)

      await datastore.delete(key)
      storedValue = await datastore.get(key)
      expect(storedValue).toBeNull()
    })

    it('should mark the datastore as dirty and clean', async () => {
      await datastore.markDirty()
      expect(await datastore.isDirty()).toBe(true)

      await datastore.markClean()
      expect(await datastore.isDirty()).toBe(false)
    })

    it('should process nodes correctly', async () => {
      const d = { priority: 1n, elements: [], tombstones: [] } as any
      const heads: CID[] = [
        CID.parse('bafybeigdyrzt5xjzqmtgmbyew7zkk64un4qxpv6ysgtg3dvlnsmjqyulxa')
      ]

      const nd = await CRDTNodeGetter.makeNode(d, heads)

      // Now, process the node
      await datastore.processNode(nd.cid, 1n, d, nd)

      // Check if the node is marked as processed
      const isProcessed = await datastore.isProcessed(nd.cid)

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

    it('should return null when retrieving non-existent key', async () => {
      const key = new Key('/nonexistent/key')

      const retrievedValue = await datastore.get(key)

      expect(retrievedValue).toBeNull()
    })
  })

  describe('Replication', () => {
    it('should replicate data across replicas', async () => {
      const replicas = await createReplicas(2, 't1')

      const key = new Key('/test/key')
      const value = new TextEncoder().encode('hola')

      // Put the value in the first replica
      await replicas[0].put(key, value)

      await waitForPropagation(2000, replicas[1], key, value)

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

      // replicas[0].logger('DAG')
      // await replicas[0].printDAG()
      //
      // replicas[1].logger('DAG')
      // await replicas[1].printDAG()
    }, 5000)

    it('should replicate updates across replicas', async () => {
      const replicas = await createReplicas(6, 't2')

      const key = new Key('/test/key')
      let value = new TextEncoder().encode('hola')

      // Put the value in the first replica
      for (let i = 0; i < 20; i++) {
        value = new TextEncoder().encode(`hola${i}`)
        await replicas[0].put(key, value)
      }

      await waitForPropagation(5000, replicas[replicas.length - 1], key, value)

      // Wait for the value to be available in all replicas
      for (const replica of replicas) {
        await waitUntil(() => replica.get(key) !== null)
        const replicatedValue = await replica.get(key)
        expect(replicatedValue).toEqual(value)
      }
    }, 6000)

    it('should replicate large data across replicas', async () => {
      const replicas = await createReplicas(2, 't4')

      const key = new Key('/test/large')
      const sizeInBytes = 1024 * 1024 // 1 MB
      const asciiValue = 97 // ASCII value of 'a'
      const largeValue = new Uint8Array(sizeInBytes).fill(asciiValue)

      await replicas[0].put(key, largeValue)

      await waitForPropagation(
        5000,
        replicas[replicas.length - 1],
        key,
        largeValue
      )

      for (const replica of replicas) {
        const replicatedValue = await replica.get(key)
        expect(replicatedValue).toEqual(largeValue)
      }
    }, 6000)

    it('should delete data across replicas', async () => {
      const replicas = await createReplicas(2, 't8')

      const key = new Key('/test/delete')
      const value = new TextEncoder().encode('delete me')

      await replicas[0].put(key, value)

      await waitForPropagation(2000, replicas[replicas.length - 1], key, value)

      // Delete the value from the first replica
      await replicas[0].delete(key)

      await waitForPropagation(2000)

      for (const replica of replicas) {
        const deletedValue = await replica.get(key)
        expect(deletedValue).toBeNull()
      }
    }, 8000)
  })
})
