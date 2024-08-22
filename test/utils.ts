import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { prefixLogger } from '@libp2p/logger'
import { tcp } from '@libp2p/tcp'
import { MemoryBlockstore } from 'blockstore-core'
import { MemoryDatastore } from 'datastore-core/memory'
import { createHelia, type HeliaLibp2p } from 'helia'
import { Key } from 'interface-datastore'
import { createLibp2p } from 'libp2p'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { Datastore, type MyLibp2pServices } from '../src/crdt'
import { PubSubBroadcaster } from '../src/pubsub_broadcaster'
import type { Libp2p, Message, SignedMessage } from '@libp2p/interface'
import type { Multiaddr } from '@multiformats/multiaddr'

export async function msgIdFnStrictNoSign (msg: Message): Promise<Uint8Array> {
  const signedMessage = msg as SignedMessage
  const encodedSeqNum = new TextEncoder().encode(
    signedMessage.sequenceNumber.toString()
  )

  return hasher.encode(encodedSeqNum)
}

export async function waitUntilAsync (condition: () => Promise<boolean>, timeout = 5000, checkInterval = 10): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (await condition()) {
      return
    }
    await new Promise(resolve => setTimeout(resolve, checkInterval))
  }
  throw new Error('Condition not met within timeout')
}

export async function waitUntil (condition: () => boolean, timeout = 1000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (condition()) {
      return
    }
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  throw new Error('Condition not met within timeout')
}

export async function createNode (): Promise<HeliaLibp2p<Libp2p<MyLibp2pServices>>> {
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

export async function createReplicas (count: number, topic: string = 'test', connectTo?: Multiaddr): Promise<Datastore[]> {
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

  if (connectTo !== undefined) {
    await replicas[0].dagService.libp2p.dial(connectTo)
  }

  if (count > 1) {
    for (let i = 0; i < count; i++) {
      await waitUntil(() => replicas[i].broadcaster.getSubscribers().length > 0, 5000)
      // console.log(`replica ${i} subscribers`, replicas[i].broadcaster.getSubscribers())
    }
  }

  return replicas
}

export async function waitForPropagation (delay = 2000, replica?: Datastore, expectedKey?: Key, expectedValue?: Uint8Array): Promise<void> {
  if (replica != null && expectedKey != null && expectedValue != null) {
    await waitUntilAsync(async () => {
      const res = await replica.get(expectedKey)
      return res !== null && res.length === expectedValue.length && res.every((value, index) => value === expectedValue[index])
    }, delay, 100)
  } else {
    // This could be a simple delay or a more sophisticated simulation
    await new Promise((resolve) => setTimeout(resolve, delay))
  }
}
