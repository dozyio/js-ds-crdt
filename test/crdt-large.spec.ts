import { FsBlockstore } from 'blockstore-fs'
import { LevelDatastore } from 'datastore-level'
import { Key } from 'interface-datastore'
import { describe, it, beforeEach } from 'vitest'
import { type CRDTDatastore } from '../src/crdt'
import { createReplicas } from './utils'

describe.skip('Large CRDT', () => {
  describe('Single node', () => {
    let replicas: CRDTDatastore[]
    let crdtDatastore: CRDTDatastore

    beforeEach(async () => {
      const datastore = new LevelDatastore('./large-crdt-test/ds')
      await datastore.open()

      const blockstore = new FsBlockstore('./large-crdt-test/bs')
      await blockstore.open()

      replicas = await createReplicas(1, { bloomFilter: null }, datastore, blockstore)
      crdtDatastore = replicas[0]
    })

    it('generate large dataset - 1 key, 1,000,000 ops', async () => {
      const value = new Uint8Array([1, 2, 3, 4])

      for (let i = 0; i < 1_000_000; i++) {
        const key = new Key('key1')
        await crdtDatastore.put(key, value)
        if (i % 1000 === 0) {
          // eslint-disable-next-line no-console
          console.log(`${new Date().toLocaleString()} put key ${i}`)
        }
      }
    }, 999_999_999)
  })
})
