import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { bootstrap } from '@libp2p/bootstrap'
import { circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { identify, type Identify } from '@libp2p/identify'
import {
  type Libp2p,
  type SignedMessage,
  type Message,
  type PubSub,
} from '@libp2p/interface'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { tcp } from '@libp2p/tcp'
import { webRTC, webRTCDirect } from '@libp2p/webrtc'
import { webSockets } from '@libp2p/websockets'
import { webTransport } from '@libp2p/webtransport'
import { MemoryDatastore } from 'datastore-core'
import { createLibp2p } from 'libp2p'
import { sha256 } from 'multiformats/hashes/sha2'

export type Libp2pType = Libp2p<{
  pubsub: PubSub
  identify: Identify
}>

export class PubSubBroadcaster {
  public libp2p: Libp2pType
  private topic: string

  // Constructor
  constructor(libp2p: Libp2pType, topic: string) {
    this.libp2p = libp2p
    this.topic = topic
  }

  // Static method to create a new instance of PubSubBroadcaster
  public static async create(topic: string): Promise<PubSubBroadcaster> {
    const libp2p = await createLibp2p({
      addresses: {
        listen: [
          '/webrtc',
          // Use the app's bootstrap nodes as circuit relays
          // ...relayListenAddrs,
        ],
      },
      transports: [
        tcp(),
        webTransport(),
        webSockets(),
        webRTC({
          rtcConfiguration: {
            iceServers: [
              {
                urls: [
                  'stun:stun.l.google.com:19302',
                  'stun:global.stun.twilio.com:3478',
                ],
              },
            ],
          },
        }),
        webRTCDirect(),
        circuitRelayTransport({
          discoverRelays: 0,
        }),
      ],
      connectionManager: {
        maxConnections: 25,
        minConnections: 5,
      },
      connectionEncryption: [noise()],
      streamMuxers: [yamux()],
      connectionGater: {
        // by default libp2p refuses to dial local addresses from browsers since they
        // are usually sent by remote peers broadcasting undialable multiaddrs and
        // cause errors to appear in the console but in this example we are
        // explicitly connecting to a local node so allow all addresse
        denyDialMultiaddr: async () => false,
      },
      peerDiscovery: [
        bootstrap({
          // multiaddr/p2p/peer id
          // list: 'bootstrapAddrs,
          list: [
            '/ip4/192.168.1.3/tcp/50138/p2p/12D3KooWEkgRTTXGsmFLBembMHxVPDcidJyqFcrqbm9iBE1xhdXq',
          ],
        }),
        pubsubPeerDiscovery({
          interval: 5_000,
          listenOnly: false,
          topics: ['peer-discovery'],
        }),
      ],
      services: {
        pubsub: gossipsub({
          emitSelf: false,
          allowPublishToZeroTopicPeers: true,
          msgIdFn: msgIdFnStrictNoSign,
          ignoreDuplicatePublishError: true,
          tagMeshPeers: true,
          doPX: true,
        }),
        // dht: kadDHT({
        //   protocol: `/${TOPIC}/kad/1.0.0`,
        //   clientMode: true,
        //   kBucketSize: 20,
        // }),
        // ping: ping(),
        identify: identify(),
        // dcutr: dcutr(),
        // delegatedRouting: () => delegatedClient,
      },
      datastore: new MemoryDatastore(),
    })

    return new PubSubBroadcaster(libp2p, topic)
  }

  // Broadcast publishes some data.
  public async broadcast(data: Uint8Array): Promise<void> {
    this.libp2p.services.pubsub.publish(this.topic, data)
  }

  public setHandler(handler: (data: Uint8Array) => void): void {
    this.libp2p.services.pubsub.addEventListener(
      'message',
      (evt: CustomEvent<Message>) => {
        console.log('evt', evt)
        const message = evt.detail
        const data = message.data // assuming message.data is the Uint8Array
        handler(data)
      },
    )
  }

  // public setHandler(handler: (data: Uint8Array) => void): void {
  //   this.libp2p.services.pubsub.addEventListener('message', handler);
  // }

  // public unsetHandler(handler: (data: Uint8Array) => void): void {
  //   this.libp2p.services.pubsub.removeEventListener('message', handler);
  // }
}

export async function msgIdFnStrictNoSign(msg: Message): Promise<Uint8Array> {
  const signedMessage = msg as SignedMessage
  const encodedSeqNum = new TextEncoder().encode(
    signedMessage.sequenceNumber.toString(),
  )

  return await sha256.encode(encodedSeqNum)
}
