import { logger } from '@libp2p/logger'
import { MemoryDatastore } from 'datastore-core/memory'
import { Key } from 'interface-datastore'
import { type delta } from '../src/pb/delta'
import { CRDTSet, type IBloomFilter } from '../src/set'

describe('CRDTSet', () => {
  let store: MemoryDatastore
  let namespace: Key
  let crdtSet: CRDTSet
  let putHookMock: ReturnType<typeof vi.fn>
  let deleteHookMock: ReturnType<typeof vi.fn>
  let bloomFilter: IBloomFilter | undefined
  let log: ReturnType<typeof logger>

  beforeEach(() => {
    store = new MemoryDatastore()
    namespace = new Key('/namespace')
    bloomFilter = undefined
    putHookMock = vi.fn()
    deleteHookMock = vi.fn()
    log = logger('test')
    crdtSet = new CRDTSet(
      store,
      namespace,
      log,
      bloomFilter,
      putHookMock,
      deleteHookMock
    )
  })

  it('should add an element to the set', async () => {
    const key = 'key1'
    const value = new Uint8Array([1, 2, 3])

    const delta = crdtSet.add(key, value)
    expect(delta.elements).toHaveLength(1)
    expect(delta.elements[0].key).toBe(key)
    expect(delta.elements[0].value).toBe(value)

    await crdtSet.putElems(delta.elements, 'id1', BigInt(1))

    const result = await crdtSet.element(key)
    expect(result).toEqual(value) // Ensure the element is correctly retrieved
    expect(putHookMock).toHaveBeenCalledWith(key, value)
  })

  it('should remove an element from the set', async () => {
    const key = 'key1'
    const value = new Uint8Array([1, 2, 3])

    const delta = crdtSet.add(key, value)
    await crdtSet.putElems(delta.elements, 'id1', BigInt(1))

    const removeDelta = await crdtSet.remove(key)
    expect(removeDelta.tombstones).toHaveLength(1)
    expect(removeDelta.tombstones[0].key).toBe(key)

    await crdtSet.putTombs(removeDelta.tombstones)

    const result = await crdtSet.element(key)
    expect(result).toBeNull()
    expect(deleteHookMock).toHaveBeenCalledWith(key)
  })

  it('should correctly merge deltas', async () => {
    const key1 = 'key1'
    const value1 = new Uint8Array([1, 2, 3])

    const key2 = 'key2'
    const value2 = new Uint8Array([4, 5, 6])

    const delta1: delta.Delta = {
      elements: [{ key: key1, value: value1, id: 'id1' }],
      tombstones: [],
      priority: BigInt(1)
    }

    const delta2: delta.Delta = {
      elements: [{ key: key2, value: value2, id: 'id2' }],
      tombstones: [],
      priority: BigInt(2)
    }

    await crdtSet.merge(delta1, 'id1')
    await crdtSet.merge(delta2, 'id2')

    const result1 = await crdtSet.element(key1)
    const result2 = await crdtSet.element(key2)

    expect(result1).toEqual(value1)
    expect(result2).toEqual(value2)
  })

  it('should correctly check if an element is in the set', async () => {
    const key = 'key1'
    const value = new Uint8Array([1, 2, 3])

    const delta = crdtSet.add(key, value)
    await crdtSet.putElems(delta.elements, 'id1', BigInt(1))

    const inSet = await crdtSet.inSet(key)
    expect(inSet).toBe(true)

    const removeDelta = await crdtSet.remove(key)
    await crdtSet.putTombs(removeDelta.tombstones)

    const notInSet = await crdtSet.inSet(key)
    expect(notInSet).toBe(false)
  })

  it('should correctly handle empty putElems and putTombs', async () => {
    await crdtSet.putElems([], 'id1', 0n)
    await crdtSet.putTombs([])

    // No elements should be added or removed
    expect(putHookMock).not.toHaveBeenCalled()
    expect(deleteHookMock).not.toHaveBeenCalled()
  })

  it('should correctly sync the datastore', async () => {
    const key = new Key('key1')
    await expect(crdtSet.datastoreSync(key)).resolves.not.toThrow()
  })

  it('should correctly handle priority when adding elements', async () => {
    const key = 'key1'
    const lowValue = new Uint8Array([1, 2, 3])
    const highValue = new Uint8Array([7, 8, 9])

    // First, add an element with a lower priority
    await crdtSet.putElems(
      [{ key, value: lowValue, id: 'id1' }],
      'id1',
      BigInt(1)
    )

    // Now, add an element with a higher priority
    await crdtSet.putElems(
      [{ key, value: highValue, id: 'id2' }],
      'id2',
      BigInt(2)
    )

    // The set should keep the value with the higher priority
    const result = await crdtSet.element(key)
    expect(result).toEqual(highValue) // Expect the higher priority value to be stored
  })

  it('should resolve conflicts by lexicographical order of IDs when priorities are equal', async () => {
    const key = 'key1'
    const value1 = new Uint8Array([1, 2, 3])
    const value2 = new Uint8Array([7, 8, 9])

    // Add elements with the same priority but different IDs
    await crdtSet.putElems(
      [{ key, value: value1, id: 'idA' }],
      'idA',
      BigInt(1)
    )
    await crdtSet.putElems(
      [{ key, value: value2, id: 'idB' }],
      'idB',
      BigInt(1)
    )

    // The element with the lexicographically higher ID should be stored
    const result = await crdtSet.element(key)
    expect(result).toEqual(value2) // Expect the value with id 'idB' to be stored
  })

  it('should return null for non-existent elements', async () => {
    const result = await crdtSet.element('non-existent-key')
    expect(result).toBeNull() // Expect null for non-existent keys
  })

  it('should correctly merge deltas with tombstones', async () => {
    const key1 = 'key1'
    const value1 = new Uint8Array([1, 2, 3])

    const delta1: delta.Delta = {
      elements: [{ key: key1, value: value1, id: 'id1' }],
      tombstones: [],
      priority: BigInt(1)
    }

    // Merge the first delta to add an element
    await crdtSet.merge(delta1, 'id1')
    const result1 = await crdtSet.element(key1)
    expect(result1).toEqual(value1)

    // Create a delta to remove the element
    const removeDelta = await crdtSet.remove(key1)
    await crdtSet.putTombs(removeDelta.tombstones)

    const result2 = await crdtSet.element(key1)
    expect(result2).toBeNull() // Expect the element to be removed
  })

  it('should handle merging an empty delta', async () => {
    const emptyDelta: delta.Delta = {
      elements: [],
      tombstones: [],
      priority: BigInt(0)
    }

    const key = 'key1'
    const value = new Uint8Array([1, 2, 3])

    // Add an element first
    await crdtSet.putElems([{ key, value, id: 'id1' }], 'id1', BigInt(1))

    // Merge an empty delta
    await crdtSet.merge(emptyDelta, 'id2')

    // The original element should still exist
    const result = await crdtSet.element(key)
    expect(result).toEqual(value)
  })

  it('should handle concurrent additions correctly', async () => {
    const key = 'key1'
    const value1 = new Uint8Array([1, 2, 3])
    const value2 = new Uint8Array([7, 8, 9])

    const put1 = crdtSet.putElems(
      [{ key, value: value1, id: 'id1' }],
      'id1',
      BigInt(1)
    )
    const put2 = crdtSet.putElems(
      [{ key, value: value2, id: 'id2' }],
      'id2',
      BigInt(2)
    )

    await Promise.all([put1, put2])

    const result = await crdtSet.element(key)
    expect(result).toEqual(value2) // Expect the value with the higher priority to be stored
  })

  it('should correctly sync the datastore with existing keys', async () => {
    const key1 = 'key1'
    const value1 = new Uint8Array([1, 2, 3])

    await crdtSet.putElems(
      [{ key: key1, value: value1, id: 'id1' }],
      'id1',
      BigInt(1)
    )

    await expect(crdtSet.datastoreSync(new Key('key1'))).resolves.not.toThrow()

    const result = await crdtSet.element(key1)
    expect(result).toEqual(value1)
  })
})
