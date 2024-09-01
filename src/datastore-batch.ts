import type { CRDTDatastore } from './crdt'
import type { Batch, Key } from 'interface-datastore'

export class DatastoreBatch implements Batch {
  constructor (private readonly store: CRDTDatastore) { }

  async put (key: Key, value: Uint8Array): Promise<void> {
    const size = await this.store.addToDelta(key.toString(), value)
    if (size > this.store.options.maxBatchDeltaSize) {
      // eslint-disable-next-line no-console
      console.log('Delta size exceeded max, committing batch')
      await this.commit()
    }
  }

  async delete (key: Key): Promise<void> {
    const size = await this.store.rmvToDelta(key.toString())
    if (size > this.store.options.maxBatchDeltaSize) {
      // eslint-disable-next-line no-console
      console.log('Delta size exceeded max, committing batch')
      await this.commit()
    }
  }

  async commit (): Promise<void> {
    await this.store.publishDelta()
  }
}
