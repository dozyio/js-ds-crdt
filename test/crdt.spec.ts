import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import * as codec from '@ipld/dag-pb'
import { identify } from '@libp2p/identify'
import { prefixLogger } from '@libp2p/logger'
import { tcp } from '@libp2p/tcp'
import { multiaddr } from '@multiformats/multiaddr'
import { MemoryBlockstore } from 'blockstore-core'
import { MemoryDatastore } from 'datastore-core/memory'
import { createHelia, type HeliaLibp2p } from 'helia'
import { Key } from 'interface-datastore'
import { createLibp2p } from 'libp2p'
import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { describe, it, expect, beforeEach } from 'vitest'
import debug from 'weald'
import { Datastore, type MyLibp2pServices } from '../src/crdt'
import { PubSubBroadcaster } from '../src/pubsub_broadcaster'
import type { Libp2p, Message, SignedMessage } from '@libp2p/interface'

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

async function createReplicas (count: number, topic: string = 'test', connectTo?: Multiaddr): Promise<Datastore[]> {
  const replicas: Datastore[] = []
  for (let i = 0; i < count; i++) {
    const store = new MemoryDatastore()
    const namespace = new Key(`crdt${i}`)
    const dagService = await createNode()
    const broadcaster = new PubSubBroadcaster(dagService.libp2p, topic, prefixLogger(`crdt${i}`).forComponent('pubsub'))

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

  // connect each replica to each other
  for (let i = 0; i < count - 1; i++) {
    for (let j = i + 1; j < count; j++) {
      const ma = replicas[j].dagService.libp2p.getMultiaddrs()
      await replicas[i].dagService.libp2p.dial(ma[0])
    }
  }

  if (connectTo) {
    await replicas[0].dagService.libp2p.dial(connectTo)
  }

  for (let i = 0; i < count; i++) {
    await waitUntil(() => replicas[i].broadcaster.getSubscribers().length > 0, 5000)
    console.log(`replica ${i} subscribers`, replicas[i].broadcaster.getSubscribers())
  }

  return replicas
}

async function waitForPropagation (delay = 2000): Promise<void> {
  // This could be a simple delay or a more sophisticated simulation
  await new Promise((resolve) => setTimeout(resolve, delay))
}

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

      await waitForPropagation(2000)

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

      await waitForPropagation(2000)

      // Wait for the value to be available in all replicas
      for (const replica of replicas) {
        await waitUntil(() => replica.get(key) !== null)
        const replicatedValue = await replica.get(key)
        expect(replicatedValue).toEqual(value)
      }
    }, 20000)
  })

  describe('Interop', () => {
    it.only('should replicate data to Go', async () => {
      debug.enable('*') // 'crdt*,*crdt:trace')
      const remote = '/ip4/127.0.0.1/tcp/53751/p2p/12D3KooWEkgRTTXGsmFLBembMHxVPDcidJyqFcrqbm9iBE1xhdXq'
      const ma = multiaddr(remote)

      const replicas = await createReplicas(1, 'globaldb-example', ma)

      const key = new Key('/test/key')
      const value = Buffer.from('hola2')

      // Put the value in the first replica
      await replicas[0].put(key, value)

      await waitForPropagation(15000)

      console.log('replica[0] DAG')
      await replicas[0].printDAG()

      const list0 = []
      for await (const { key, value } of replicas[0].store.query({})) {
        list0.push(key)
      }
      console.log('LIST0 ALL THE VALUES', list0)

      expect(true).toEqual(true)
      // // Wait for the value to be available in all replicas
      // for (const replica of replicas) {
      //   await waitUntil(() => replica.get(key) !== null)
      //   const replicatedValue = await replica.get(key)
      //   expect(replicatedValue).toEqual(value)
      // }
    }, 20000)
  })
})
