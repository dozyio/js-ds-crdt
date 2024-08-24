import { Key } from 'interface-datastore'
import { describe, bench } from 'vitest'
import { createReplicas, waitUntilAsync } from './utils'
import type { CRDTDatastore } from '../src/crdt'

describe('Benchmark write', () => {
  let replicas: CRDTDatastore[]
  const value = new Uint8Array([1, 2, 3, 4, 5])

  bench(
    'write keys to single replica',
    async () => {
      const key = new Key('/benchmark/writekey')
      await replicas[0].put(key, value)
    },
    {
      time: 5000,
      setup: async () => {
        replicas = await createReplicas(1, 'bench')
      }
    }
  )
})

describe('Benchmark read/write', () => {
  let replicas: CRDTDatastore[]
  const value = new Uint8Array([1, 2, 3, 4, 5])

  bench(
    'read/write keys to single replica',
    async () => {
      const key = new Key('/benchmark/readwrite')
      await replicas[0].put(key, value)
      await waitUntilAsync(
        async () => (await replicas[0].get(key)) !== null,
        5000,
        1
      )
    },
    {
      time: 5000,
      setup: async () => {
        replicas = await createReplicas(1, 'bench')
      }
    }
  )
})
