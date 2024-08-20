import { type Identify } from '@libp2p/identify'
import {
  type Libp2p,
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

  // Constructor
  constructor (libp2p: Libp2pType, topic: string) {
    this.libp2p = libp2p
    this.topic = topic
  }

  // Broadcast publishes some data.
  public async broadcast (data: Uint8Array): Promise<void> {
    console.log('broadcasting data', data)
    const res = await this.libp2p.services.pubsub.publish(this.topic, data)
    console.log('pubsub publish res', res)
  }

  public setHandler (handler: (data: Uint8Array) => void): void {
    console.log('setting pubsub handler')
    this.libp2p.services.pubsub.addEventListener(
      'message',
      (evt: CustomEvent<Message>) => {
        // eslint-disable-next-line no-console
        console.log('evt', evt.detail)
        const message = evt.detail
        const data = message.data // assuming message.data is the Uint8Array
        handler(data)
      }
    )

    console.log('adding subscription change listener')
    this.libp2p.services.pubsub.addEventListener('subscription-change', this.onSubscriptionChange)

    console.log('subscribing to topic:', this.topic)
    this.libp2p.services.pubsub.subscribe(this.topic)
  }

  private onSubscriptionChange (): void {
    console.log('pubsub subscription change')
    console.log('osc subscribers', this.getSubscribers())
  }

  public getSubscribers (): PeerId[] {
    if (this.libp2p !== undefined) {
      return this.libp2p.services.pubsub.getSubscribers(this.topic)
    } else {
      console.log('libp2p is undefined')
    }
    return []
  }

  // public unsetHandler(handler: (data: Uint8Array) => void): void {
  //   this.libp2p.services.pubsub.removeEventListener('message', handler);
  // }
}
