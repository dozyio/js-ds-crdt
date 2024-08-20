import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import * as codec from '@ipld/dag-pb'
import { bootstrap } from '@libp2p/bootstrap'
import { identify } from '@libp2p/identify'
import { prefixLogger } from '@libp2p/logger'
import { tcp } from '@libp2p/tcp'
import { MemoryBlockstore } from 'blockstore-core'
import { MemoryDatastore } from 'datastore-core/memory'
import { createHelia, type HeliaLibp2p } from 'helia'
import { Key } from 'interface-datastore'
import { createLibp2p } from 'libp2p'
import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { describe, it, expect, beforeEach } from 'vitest'
import { Datastore, defaultOptions, type MyLibp2pServices } from '../src/crdt'
import { delta } from '../src/pb/delta'
import { PubSubBroadcaster } from '../src/pubsub_broadcaster'
import type { Libp2p, Logger, Message, SignedMessage } from '@libp2p/interface'

export async function msgIdFnStrictNoSign (msg: Message): Promise<Uint8Array> {
  const signedMessage = msg as SignedMessage
  const encodedSeqNum = new TextEncoder().encode(
    signedMessage.sequenceNumber.toString()
  )

  return hasher.encode(encodedSeqNum)
}

async function waitUntil (condition: () => boolean, timeout = 1000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (condition()) {
      return
    }
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  throw new Error('Condition not met within timeout')
}

async function createNode (): Promise<HeliaLibp2p<Libp2p<MyLibp2pServices>>> {
  const blockstore = new MemoryBlockstore()
  const datastore = new MemoryDatastore()

  const libp2p = await createLibp2p({
    addresses: {
      listen: [
        '/ip4/127.0.0.1/tcp/0'
      ]
    },
    transports: [
      tcp()
    ],
    connectionEncryption: [
      noise()
    ],
    streamMuxers: [
      yamux()
    ],
    // peerDiscovery: [
    //   bootstrap({
    //     list: [
    //       '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
    //       '/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa',
    //       '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb',
    //       '/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt'
    //     ]
    //   })
    // ],
    services: {
      identify: identify(),
      pubsub: gossipsub({
        emitSelf: false,
        allowPublishToZeroTopicPeers: true,
        msgIdFn: msgIdFnStrictNoSign,
        ignoreDuplicatePublishError: true,
        tagMeshPeers: true
        // doPX: true
      })
    }
  })

  const h = await createHelia({
    datastore,
    blockstore,
    libp2p
  })

  return h
}

async function createReplicas (count: number): Promise<Datastore[]> {
  const replicas: Datastore[] = []
  for (let i = 0; i < count; i++) {
    const store = new MemoryDatastore()
    const namespace = new Key(`crdt${i}`)
    const dagService = await createNode()
    const broadcaster = new PubSubBroadcaster(dagService.libp2p, 'test', prefixLogger(`crdt${i}`).forComponent('pubsub'))

    const options = {
      loggerPrefix: `crdt${i}`,
      rebroadcastInterval: 5000,
      repairInterval: 60000,
      logInterval: 1000,
      numWorkers: 1,
      dagSyncerTimeout: 2000,
      maxBatchDeltaSize: 1000,
      multiHeadProcessing: false
    } as any

    const datastore = new Datastore(store, namespace, dagService, broadcaster, options)
    replicas.push(datastore)
  }

  // for (let i = 0; i < count; i++) {
  //   console.log(`replica ${i} peerId`, replicas[i].dagService.libp2p.peerId.toString())
  // }

  // connect each replica to each other
  for (let i = 0; i < count - 1; i++) {
    for (let j = i + 1; j < count; j++) {
      const ma = replicas[j].dagService.libp2p.getMultiaddrs()
      await replicas[i].dagService.libp2p.dial(ma[0])
    }
  }

  for (let i = 0; i < count; i++) {
    await waitUntil(() => replicas[i].broadcaster.getSubscribers().length > 0, 5000)
    console.log(`replica ${i} subscribers`, replicas[i].broadcaster.getSubscribers())
  }

  return replicas
}

async function simulatePropagation (replicas: Datastore[]): Promise<void> {
  // This could be a simple delay or a more sophisticated simulation
  await new Promise((resolve) => setTimeout(resolve, 20000))
}

describe('Datastore', () => {
  // let store: MemoryDatastore
  // let namespace: Key
  // let dagService: HeliaLibp2p<Libp2p<MyLibp2pServices>>
  // let broadcaster: any
  // let options: any
  // let datastore: Datastore

  // beforeEach(async () => {
  //   store = new MemoryDatastore()
  //   namespace = new Key('testNamespace')
  //   dagService = await createNode()
  //   broadcaster = new PubSubBroadcaster(dagService.libp2p, 'test')
  //
  //   options = {
  //     logger: console,
  //     rebroadcastInterval: 1000,
  //     repairInterval: 2000,
  //     logInterval: 3000,
  //     numWorkers: 1,
  //     dagSyncerTimeout: 1000,
  //     maxBatchDeltaSize: 1000,
  //     multiHeadProcessing: false
  //   }
  //
  //   datastore = new Datastore(
  //     store,
  //     namespace,
  //     dagService,
  //     broadcaster,
  //     options
  //   )
  // })

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
    await datastore.MarkDirty()
    expect(await datastore.IsDirty()).toBe(true)

    await datastore.MarkClean()
    expect(await datastore.IsDirty()).toBe(false)
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

  it('should handle branch processing', async () => {
    // const blockstore = new MemoryBlockstore()
    // const store = new MemoryDatastore()
    // const namespace = new Key('namespace')
    // const broadcaster = { broadcast: vi.fn(), setHandler: vi.fn() }
    // const dagService = { blockstore } as unknown as Helia
    const options = { ...defaultOptions() }

    const datastore = new Datastore(
      store,
      namespace,
      dagService,
      broadcaster,
      options
    )

    // Create some valid delta data for the CRDT
    const deltaData: delta.Delta = {
      elements: [{ key: 'key1', value: new Uint8Array([1, 2, 3]) }],
      tombstones: [],
      priority: 1n
    }

    // Encode the delta data into a protobuf format
    const encodedDelta = delta.Delta.encode(deltaData)

    // Create a node with the encoded delta data
    const node = codec.createNode(encodedDelta, [])
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

  // it('should sync data correctly', async () => {
  //   const key = new Key('/test/key')
  //   const value = new Uint8Array([1, 2, 3])
  //
  //   await datastore.put(key, value)
  //
  //   const syncSpy = vi.spyOn(store, 'sync')
  //   await datastore.Sync(key)
  //
  //   expect(syncSpy).toHaveBeenCalledWith(namespace.child(key))
  // })

  // it('should rebroadcast heads correctly', async () => {
  //   const broadcastSpy = vi.spyOn(broadcaster, 'broadcast')
  //
  //   await datastore.rebroadcastHeads()
  //
  //   expect(broadcastSpy).toHaveBeenCalled()
  // })
  it('should put and get values', async () => {
    const key = new Key('hi')
    await datastore.put(key, new Uint8Array(Buffer.from('hola')))

    const value = await datastore.get(key)
    expect(value).toEqual(new Uint8Array(Buffer.from('hola')))
  })

  it.only('should replicate data across replicas', async () => {
    const replicas = await createReplicas(2)

    const key = new Key('/test/key')
    const value = new Uint8Array(Buffer.from('hola'))

    // Put the value in the first replica
    await replicas[0].put(key, value)

    await simulatePropagation(replicas)

    // console.log('replica[0] DAG')
    // await replicas[0].PrintDAG()
    //
    // console.log('replica[1] DAG')
    // await replicas[1].PrintDAG()

    const list0 = []
    for await (const { key, value } of replicas[0].store.query({})) {
      list0.push(key)
    }
    console.log('LIST0 ALL THE VALUES', list0)

    const list1 = []
    for await (const { key, value } of replicas[1].store.query({})) {
      list1.push(key)
    }
    console.log('LIST1 ALL THE VALUES', list1)

    // Wait for the value to be available in all replicas
    for (const replica of replicas) {
      await waitUntil(() => replica.get(key) !== null)
      const replicatedValue = await replica.get(key)
      expect(replicatedValue).toEqual(value)
    }
  }, 30000)

  it('should handle concurrent updates and ensure final consistency', async () => {
    const key = new Key('k')
    const replicas = await createReplicas(5)
    const tasks = []

    for (let i = 0; i < replicas.length; i++) {
      tasks.push(async () => {
        for (let j = 0; j < 50; j++) {
          await replicas[i].put(key, new Uint8Array(Buffer.from(`r#${i}`)))
        }
      })
    }

    await Promise.all(tasks)
    await simulatePropagation(replicas)

    let finalValue = null
    for (const replica of replicas) {
      const value = await replica.get(key)
      if (finalValue !== null && finalValue !== value) {
        throw new Error('Inconsistent values across replicas')
      }
      finalValue = value
    }

    expect(finalValue).toBeTruthy() // Replace with an appropriate value check
  })
})
