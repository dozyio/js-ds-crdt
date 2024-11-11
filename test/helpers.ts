import os from 'os'
import path from 'path'
import { MemoryDatastore } from 'datastore-core/memory'
import { FsDatastore } from 'datastore-fs'
import { LevelDatastore } from 'datastore-level'

export const datastoreTypes = ['memory', 'level', 'fs'] as const

// Factory function to create datastore instances
export async function createDatastore (type: 'memory' | 'level' | 'fs', dirPath?: string): Promise<MemoryDatastore | LevelDatastore | FsDatastore> {
  let store: MemoryDatastore | LevelDatastore | FsDatastore
  switch (type) {
    case 'memory':
      store = new MemoryDatastore()
      break
    case 'level':
      store = new LevelDatastore(dirPath ?? path.join(os.tmpdir(), 'level-datastore')) // Adjust path as needed
      await store.open()
      break
    case 'fs':
      store = new FsDatastore(dirPath ?? path.join(os.tmpdir(), 'fs-datastore')) // Adjust path as needed
      try {
        await store.open()
      } catch (e) {
        // eslint-disable-next-line no-console
        console.log(e)
        process.exit(1)
      }
      break
    default:
      throw new Error(`Unknown datastore type: ${type}`)
  }

  return store
}
