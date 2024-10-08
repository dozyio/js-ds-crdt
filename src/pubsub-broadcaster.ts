import { type Identify } from '@libp2p/identify'
import {
  type Libp2p,
  type Logger,
  type Message,
  type PeerId,
  type PubSub
} from '@libp2p/interface'

export type Libp2pType = Libp2p<{
  pubsub: PubSub
  identify: Identify
}>

export class PubSubBroadcaster {
  public libp2p: Libp2pType
  private readonly topic: string
  private readonly logger: Logger

  // Constructor
  constructor (libp2p: Libp2pType, topic: string, logger: Logger) {
    this.logger = logger
    this.topic = topic
    this.libp2p = libp2p
  }

  // Broadcast publishes some data.
  public async broadcast (data: Uint8Array): Promise<void> {
    if (this.libp2p.services.pubsub.getPeers().length > 0) {
      this.logger('broadcasting data', data)
      const res = await this.libp2p.services.pubsub.publish(this.topic, data)
      this.logger('pubsub publish res', res)
    } else {
      this.logger('skipping broadcast, no peers', data)
    }
  }

  public setHandler (handler: (data: Uint8Array) => void): void {
    this.logger('setting pubsub handler')
    this.libp2p.services.pubsub.addEventListener(
      'message',
      (evt: CustomEvent<Message>) => {
        if (evt.detail.topic === this.topic) {
          this.logger('evt', evt)
          const message = evt.detail
          const data = message.data // assuming message.data is the Uint8Array
          handler(data)
        }
      }
    )

    // this.logger('adding subscription change listener')
    // this.libp2p.services.pubsub.addEventListener('subscription-change', this.onSubscriptionChange)

    this.logger('subscribing to topic:', this.topic)
    this.libp2p.services.pubsub.subscribe(this.topic)
  }

  // private onSubscriptionChange (): void {
  //   this.logger('pubsub subscription change')
  // }

  public getSubscribers (): PeerId[] {
    if (this.libp2p !== undefined) {
      return this.libp2p.services.pubsub.getSubscribers(this.topic)
    } else {
      this.logger.error('libp2p is undefined')
    }
    return []
  }

  // public unsetHandler(handler: (data: Uint8Array) => void): void {
  //   this.libp2p.services.pubsub.removeEventListener('message', handler);
  // }
}
