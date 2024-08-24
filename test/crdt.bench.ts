import { Key } from 'interface-datastore'
import { describe, bench } from 'vitest'
import { type Datastore } from '../src/crdt'
import { createReplicas, waitUntilAsync } from './utils'

describe('Benchmark write', () => {
  let replicas: Datastore[]
  const value = new Uint8Array([1, 2, 3, 4, 5]) // Some sample value

  bench(
    'write keys to single replica',
    async () => {
      const key = new Key('/benchmark/writekey')
      await replicas[0].put(key, value)
      // const v = await replicas[0].get(key)
      // expect(v).toEqual('xxxx')//value)
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
  let replicas: Datastore[]
  const value = new Uint8Array([1, 2, 3, 4, 5]) // Some sample value

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

  // bench('normal', () => {
  //   const x = [1, 5, 4, 2, 3]
  //   x.sort((a, b) => {
  //     return a - b
  //   })
  // })
  //
  // bench('reverse', () => {
  //   const x = [1, 5, 4, 2, 3]
  //   x.reverse().sort((a, b) => {
  //     return a - b
  //   })
  // })
})
