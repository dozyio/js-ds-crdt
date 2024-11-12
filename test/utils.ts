import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { bitswap } from '@helia/block-brokers'
import { identify } from '@libp2p/identify'
import { prefixLogger } from '@libp2p/logger'
import { tcp } from '@libp2p/tcp'
import { MemoryBlockstore } from 'blockstore-core'
import { MemoryDatastore } from 'datastore-core/memory'
import { createHelia, type HeliaLibp2p } from 'helia'
import { Key, type Datastore } from 'interface-datastore'
import { createLibp2p } from 'libp2p'
import { CRDTDatastore, defaultOptions, type CRDTLibp2pServices, type Options } from '../src/crdt'
import { PubSubBroadcaster } from '../src/pubsub-broadcaster'
import { msgIdFnStrictNoSign } from '../src/utils'
import type { ComponentLogger, Libp2p } from '@libp2p/interface'
import type { Blockstore } from 'interface-blockstore'

export async function waitUntilAsync (
  condition: () => Promise<boolean>,
  timeout = 5000,
  checkInterval = 10,
  message = 'Condition not met within timeout'
): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (await condition()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, checkInterval))
  }
  throw new Error(message)
}

export async function waitUntil (
  condition: () => boolean,
  timeout = 1000
): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (condition()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  throw new Error('Condition not met within timeout')
}

export async function createNode (logger: ComponentLogger, datastore: Datastore, blockstore: Blockstore, minConnections = 1): Promise<HeliaLibp2p<Libp2p<CRDTLibp2pServices>>> {
  const libp2p = await createLibp2p({
    logger,
    addresses: {
      listen: ['/ip4/127.0.0.1/tcp/0']
    },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    connectionMonitor: {
      enabled: false
    },
    services: {
      identify: identify(),
      pubsub: gossipsub({
        emitSelf: false,
        allowPublishToZeroTopicPeers: false,
        msgIdFn: msgIdFnStrictNoSign,
        ignoreDuplicatePublishError: true,
        tagMeshPeers: true,
        doPX: true
      })
    },
    contentRouters: []
  })

  const blockBrokers = [bitswap()]

  const h = await createHelia({
    logger,
    datastore,
    blockstore,
    libp2p,
    blockBrokers,
    dns: undefined
  })

  return h
}

export async function createReplicas (
  count: number = 1,
  options?: Partial<Options>,
  datastore?: Datastore,
  blockstore?: Blockstore
): Promise<CRDTDatastore[]> {
  const replicas: CRDTDatastore[] = []

  if (count > 1 && (datastore !== undefined || blockstore !== undefined)) {
    throw new Error('datastore and blockstore must be undefined when creating more than one replica')
  }

  for (let i = 0; i < count; i++) {
    if (datastore === undefined) {
      datastore = new MemoryDatastore()
    }

    if (blockstore === undefined) {
      blockstore = new MemoryBlockstore()
    }

    const namespace = new Key(`crdt${i}`)
    const dagService = await createNode(prefixLogger(`crdt${i}`), datastore, blockstore, count - 1)
    const broadcaster = new PubSubBroadcaster(
      dagService.libp2p,
      'test',
      prefixLogger(`crdt${i}`).forComponent('pubsub')
    )

    let opts
    if (options !== undefined) {
      opts = { ...defaultOptions(), ...options }
    } else {
      opts = defaultOptions()
    }

    opts.loggerPrefix = `crdt${i}`

    const crdtDatastore = new CRDTDatastore(
      datastore,
      namespace,
      dagService,
      broadcaster,
      opts
    )

    replicas.push(crdtDatastore)
  }

  return replicas
}

export async function connectReplicas (replicas: CRDTDatastore[]): Promise<void> {
  for (let i = 0; i < replicas.length - 1; i++) {
    for (let j = i + 1; j < replicas.length; j++) {
      const ma = replicas[j].dagService.libp2p.getMultiaddrs()
      let attempt = 0
      const maxAttempts = 3

      while (attempt < maxAttempts) {
        try {
          await replicas[i].dagService.libp2p.dial(ma[0])
          break // Dial was successful, exit the retry loop
        } catch (e) {
          attempt++
          if (attempt >= maxAttempts) {
            console.error('Replica ${i} Max dial attempts reached. Throwing error.')
            throw e // All retries failed, rethrow the error
          } else {
            // Optionally, wait for a short period before retrying
            await new Promise((resolve) => setTimeout(resolve, 1000))
          }
        }
      }
    }
  }

  if (replicas.length > 1 && replicas.length < 5) {
    for (let i = 0; i < replicas.length; i++) {
      try {
        await waitUntil(
          () => replicas[i].broadcaster.getSubscribers().length === replicas.length - 1,
          5000
        )
      } catch (e) {
        throw new Error(`replica ${i} not enough subscribers: ${replicas[i].broadcaster.getSubscribers().length}/${replicas.length - 1}`)
      }
    }
  }

  if (replicas.length > 5) {
    for (let i = 0; i < replicas.length; i++) {
      try {
        await waitUntil(
          () => replicas[i].broadcaster.getSubscribers().length >= Math.ceil((replicas.length - 1) / 2),
          10000
        )
      } catch (e) {
        throw new Error(`replica ${i} not enough subscribers: ${replicas[i].broadcaster.getSubscribers().length}/${replicas.length - 1}`)
      }
    }
  }
}

export async function waitForPropagation (
  timeout = 2000,
  replica?: CRDTDatastore,
  expectedKey?: Key,
  expectedValue?: Uint8Array | null
): Promise<void> {
  if (replica !== undefined && expectedKey !== undefined && expectedValue !== undefined) {
    await waitUntilAsync(
      async () => {
        const res = await replica.get(expectedKey)
        if (expectedValue !== null) {
          return (
            res !== null &&
            res.length === expectedValue.length &&
            res.every((value, index) => value === expectedValue[index])
          )
        } else {
          return res === expectedValue
        }
      },
      timeout,
      100
    )
  } else {
    // just wait for timeout
    await new Promise((resolve) => setTimeout(resolve, timeout))
  }
}

export async function validateKeyConsistency (replicas: CRDTDatastore[], key: string, debug = false): Promise<boolean> {
  const res: Array<Uint8Array | string | null> = []

  for (let i = 0; i < replicas.length; i++) {
    let decoded: string | null = null

    const raw = await replicas[i].get(new Key(key))
    if (raw !== null) {
      decoded = new TextDecoder().decode(raw)
      res[i] = decoded
    } else {
      decoded = null
      res[i] = null
    }

    if (debug) {
      // eslint-disable-next-line no-console
      console.log(`r${i} ${key} ${decoded}`)
    }
  }

  const match = res.every(v => v === res[0])
  if (!match && debug) {
    for (let i = 0; i < replicas.length; i++) {
      const stats = await replicas[i].internalStats()
      // eslint-disable-next-line no-console
      console.log(`NOMATCH r${i} ${key}: ${res[i]} heads: ${stats.heads}, height: ${stats.maxHeight}, queued Jobs: ${stats.queuedJobs}`)
    }
  }

  return match
}

export function cmpValues (a: Uint8Array | null, b: Uint8Array | null): boolean {
  if (a === null && b === null) {
    return true
  }

  if ((a === null && b !== null) || (a !== null && b === null)) {
    return false
  }

  if (a !== null && b !== null) {
    if (a.length !== b.length) {
      return false
    }

    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) {
        return false
      }
    }
  }

  return true
}
