import { FsBlockstore } from 'blockstore-fs'
import { LevelDatastore } from 'datastore-level'
import { Key } from 'interface-datastore'
import { describe, bench } from 'vitest'
import { createReplicas } from './utils'
import type { CRDTDatastore } from '../src/crdt'

describe('Benchmark', () => {
  let replicas: CRDTDatastore[]
  const value = new Uint8Array([1, 2, 3, 4, 5])
  const key = new Key('/benchmark')

  bench(
    'write keys to single replica',
    async () => {
      await replicas[0].put(key, value)
    },
    {
      time: 5000,
      setup: async () => {
        replicas = await createReplicas(1)
      }
    }
  )

  bench(
    'read key from single replica',
    async () => {
      await replicas[0].get(key)
    },
    {
      time: 5000,
      setup: async () => {
        replicas = await createReplicas(1)
        await replicas[0].put(key, value)
      }
    }
  )

  bench(
    'has key from single replica',
    async () => {
      await replicas[0].has(key)
    },
    {
      time: 5000,
      setup: async () => {
        replicas = await createReplicas(1)
        await replicas[0].put(key, value)
      }
    }
  )
})

describe.skip('Benchmark large', () => {
  let datastore: LevelDatastore
  let blockstore: FsBlockstore
  let replicas: CRDTDatastore[]

  bench.only(
    'read key from single replica, large set',
    async () => {
      // const keyX = new Key(`/key${Math.floor(Math.random() * (999999 - 0 + 1)) + 0}`)
      // const keyX = new Key(`key${Math.floor(Math.random() * (999999 - 0 + 1)) + 0}`)
      const keyX = new Key('key1')
      await replicas[0].get(keyX)
    },
    {
      time: 5000,
      setup: async () => {
        datastore = new LevelDatastore('./large-crdt-test/ds')
        await datastore.open()

        blockstore = new FsBlockstore('./large-crdt-test/bs')
        await blockstore.open()

        replicas = await createReplicas(1, {}, datastore, blockstore)
      },
      teardown: async () => {
        await datastore.close()
        await blockstore.close()
      }
    }
  )
})
