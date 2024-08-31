import { Key } from 'interface-datastore'
import { CID } from 'multiformats/cid'
import { describe, it, expect, beforeEach } from 'vitest'
import { type CRDTDatastore } from '../src/crdt'
import { CRDTNodeGetter } from '../src/ipld'
import {
  connectReplicas,
  createReplicas,
  validateKeyConsistency,
  waitForPropagation,
  waitUntil,
  waitUntilAsync
} from './utils'
// debug.enable('crdt*,-crdt0:crdt')
// debug.enable('*')

// for interop tests - see https://github.com/dozyio/ds-crdt-interop

describe('Datastore', () => {
  describe('Single node', () => {
    let replicas: CRDTDatastore[]
    let crdtDatastore: CRDTDatastore

    beforeEach(async () => {
      replicas = await createReplicas(1, 't1')
      crdtDatastore = replicas[0]
    })

    // it('should initialize correctly', () => {
    //   // expect(crdtDatastore.store).toBe(datastore)
    //   expect(crdtDatastore.namespace).toBe(namespace)
    // })

    it('should add and retrieve elements from the set', async () => {
      const key = new Key('key1')
      const value = new Uint8Array([1, 2, 3])

      await crdtDatastore.put(key, value)
      const storedValue = await crdtDatastore.get(key)

      expect(storedValue).toEqual(value)
    })

    it('should delete elements from the set', async () => {
      const key = new Key('key2')
      const value = new Uint8Array([4, 5, 6])

      await crdtDatastore.put(key, value)
      let storedValue = await crdtDatastore.get(key)
      expect(storedValue).toEqual(value)

      await crdtDatastore.delete(key)
      storedValue = await crdtDatastore.get(key)
      expect(storedValue).toBeNull()
    })

    it('should mark the datastore as dirty and clean', async () => {
      await crdtDatastore.markDirty()
      expect(await crdtDatastore.isDirty()).toBe(true)

      await crdtDatastore.markClean()
      expect(await crdtDatastore.isDirty()).toBe(false)
    })

    it('should process nodes correctly', async () => {
      const d = { priority: 1n, elements: [], tombstones: [] } as any
      const heads: CID[] = [
        CID.parse('bafybeigdyrzt5xjzqmtgmbyew7zkk64un4qxpv6ysgtg3dvlnsmjqyulxa')
      ]

      const nd = await CRDTNodeGetter.makeNode(d, heads)

      // Now, process the node
      await crdtDatastore.processNode(nd.cid, 1n, d, nd)

      // Check if the node is marked as processed
      const isProcessed = await crdtDatastore.isProcessed(nd.cid)

      expect(isProcessed).toBe(true)
    })

    it('should rebroadcast heads', async () => {
      const cid = CID.parse(
        'bafyreigx2zx5k2gxejyfmksls5bl6bhybcq4aqmhft7y2jxup4lgjxbiou'
      )
      await crdtDatastore.heads.add(cid, 1n)

      await crdtDatastore.rebroadcastHeads()
      // Since there's no real broadcast logic in this test, we just check that no errors were thrown.
      expect(true).toBe(true)
    })

    it('should return null when retrieving non-existent key', async () => {
      const key = new Key('/nonexistent/key')

      const retrievedValue = await crdtDatastore.get(key)

      expect(retrievedValue).toBeNull()
    })
  })

  describe('Replication', () => {
    it('should replicate data across replicas', async () => {
      const replicas = await createReplicas(2, 't1')
      await connectReplicas(replicas)

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
      await connectReplicas(replicas)

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
      await connectReplicas(replicas)

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
      await connectReplicas(replicas)

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

  describe('Convergence', () => {
    const operations = async (
      replicas: CRDTDatastore[],
      replicaId: number,
      keys: string[],
      numOperations: number,
      numKeys: number
    ): Promise<void> => {
      for (let i = 0; i < numOperations; i++) {
        const key = new Key(keys[i % numKeys])
        const value = new TextEncoder().encode(`valueR${replicaId}-${i}`)

        await replicas[replicaId].put(key, value)
      }
    }

    const waitKeyValueConvergence = async (replicas: CRDTDatastore[], keys: string[], timeout: number = 30000, interval: number = 1000): Promise<void> => {
      for (const key of keys) {
        await waitUntilAsync(
          async () => {
            const res = await validateKeyConsistency(replicas, key)
            return res
          },
          timeout,
          interval,
          "replicas don't have the same values for key"
        )
      }
    }

    const waitHeadConvergence = async (replicas: CRDTDatastore[], timeout: number = 30000, interval: number = 1000): Promise<void> => {
      await waitUntilAsync(
        async () => {
          const heads: any[] = []

          for (let i = 0; i < replicas.length; i++) {
            const stats = await replicas[i].internalStats()
            heads[i] = JSON.stringify(stats.heads.map(h => h.toString()))
            // console.log(`r${i} heads: ${heads[i]}`)
          }

          return heads.every(h => h === heads[0])
        },
        timeout,
        interval,
        "replicas don't have the same heads"
      )
    }

    it('put/delete converge after partition', async () => {
      const numReplicas = 3

      const replicas = await createReplicas(numReplicas, 't9')

      await replicas[0].put(new Key('key1'), new TextEncoder().encode('value1'))
      await replicas[0].delete(new Key('key1'))
      await replicas[1].put(new Key('key1'), new TextEncoder().encode('value2'))
      await replicas[2].put(new Key('key1'), new TextEncoder().encode('value3'))

      await connectReplicas(replicas)

      await waitKeyValueConvergence(replicas, ['key1'], 10000, 1000)

      await waitHeadConvergence(replicas, 30000, 1000)
    }, 10000)

    it('2 nodes with should converge - 1 key, 100 ops', async () => {
      const numReplicas = 2
      const numKeys = 1
      const numOperations = 100
      const promises: Array<Promise<void>> = []
      const keys = Array.from({ length: numKeys }, (_, i) => `key${i}`)

      const replicas = await createReplicas(numReplicas, 't9')

      // add state to unconnected replicas
      for (let replicaId = 0; replicaId < numReplicas; replicaId++) {
        promises.push(operations(replicas, replicaId, keys, numOperations, numKeys))
      }
      await Promise.all(promises)

      await connectReplicas(replicas)

      await waitKeyValueConvergence(replicas, keys, 50000, 1000)

      await waitHeadConvergence(replicas, 30000, 1000)
    }, 40000)

    it('5 nodes with should converge - 5 keys, 100 ops', async () => {
      const numReplicas = 5
      const numKeys = 5
      const numOperations = 100
      const promises: Array<Promise<void>> = []
      const keys = Array.from({ length: numKeys }, (_, i) => `key${i}`)

      const replicas = await createReplicas(numReplicas, 't9')

      // add state to unconnected replicas
      for (let replicaId = 0; replicaId < numReplicas; replicaId++) {
        promises.push(operations(replicas, replicaId, keys, numOperations, numKeys))
      }
      await Promise.all(promises)

      await connectReplicas(replicas)

      await waitKeyValueConvergence(replicas, keys, 50000, 1000)

      await waitHeadConvergence(replicas, 50000, 1000)
    }, 40000)
  })
})
