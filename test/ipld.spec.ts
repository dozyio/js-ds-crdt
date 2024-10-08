import * as codec from '@ipld/dag-pb'
import { prefixLogger } from '@libp2p/logger'
import { MemoryBlockstore } from 'blockstore-core'
import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { delta } from '../src//pb/delta'
import { CRDTNodeGetter, type DeltaOption } from '../src/ipld'

describe('CRDTNodeGetter', () => {
  let blockstore: MemoryBlockstore
  let crdtNodeGetter: CRDTNodeGetter

  beforeEach(() => {
    blockstore = new MemoryBlockstore()
    crdtNodeGetter = new CRDTNodeGetter(
      blockstore,
      prefixLogger('test').forComponent('ipld')
    )
  })

  it('should correctly get delta for a given CID', async () => {
    const cid = CID.parse(
      'bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    )

    const mockDelta: delta.Delta = {
      priority: BigInt(10),
      elements: [{ key: 'key1', id: 'id1', value: new Uint8Array([1, 2, 3]) }],
      tombstones: [
        { key: 'key2', id: 'id2', value: new Uint8Array([4, 5, 6]) }
      ]
    }

    const encodedDelta = delta.Delta.encode(mockDelta)

    const block = await Block.encode({
      value: codec.createNode(encodedDelta, []),
      codec,
      hasher
    })

    const newCid = await blockstore.put(cid, block.bytes)
    expect(newCid.toString()).toBe(cid.toString())

    const result = await crdtNodeGetter.getDelta(cid)
    expect(result.delta.priority).toBe(BigInt(10))
  })

  it('should correctly get priority from delta', async () => {
    const cid = CID.parse(
      'bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    )
    const mockDelta: delta.Delta = {
      priority: BigInt(5),
      elements: [],
      tombstones: []
    }
    const encodedDelta = delta.Delta.encode(mockDelta)
    const block = await Block.encode({
      value: codec.createNode(encodedDelta, []),
      codec,
      hasher
    })

    await blockstore.put(cid, block.bytes)

    const priority = await crdtNodeGetter.getPriority(cid)
    expect(priority).toBe(BigInt(5))
  })

  it('should correctly handle missing data in node', async () => {
    const cid = CID.parse(
      'bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    )
    const block = await Block.encode({
      value: codec.createNode(new Uint8Array(), []),
      codec,
      hasher
    })

    await blockstore.put(cid, block.bytes)

    await expect(crdtNodeGetter.getDelta(cid)).rejects.toThrow(
      'Node has no data'
    )
  })

  it('should yield deltas for multiple CIDs', async () => {
    const cid1 = CID.parse(
      'bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    )
    const cid2 = CID.parse(
      'bafybeigdyrztg6nbv3f2vzk3euvr63zjkvqlukmhhojz6kmiy7m7xvlhvq'
    )

    const mockDelta1: delta.Delta = {
      priority: BigInt(1),
      elements: [],
      tombstones: []
    }
    const mockDelta2: delta.Delta = {
      priority: BigInt(2),
      elements: [],
      tombstones: []
    }
    const encodedDelta1 = delta.Delta.encode(mockDelta1)
    const encodedDelta2 = delta.Delta.encode(mockDelta2)
    const block1 = await Block.encode({
      value: codec.createNode(encodedDelta1, []),
      codec,
      hasher
    })
    const block2 = await Block.encode({
      value: codec.createNode(encodedDelta2, []),
      codec,
      hasher
    })

    await blockstore.put(cid1, block1.bytes)
    await blockstore.put(cid2, block2.bytes)

    const deltas = crdtNodeGetter.getDeltas([cid1, cid2])

    const results: DeltaOption[] = []
    for await (const deltaOption of deltas) {
      results.push(deltaOption)
    }

    expect(results).toHaveLength(2)
    expect(results[0].delta?.priority).toBe(BigInt(1))
    expect(results[1].delta?.priority).toBe(BigInt(2))
  })

  it('should create a node from a delta and heads', async () => {
    const cid1 = CID.parse(
      'bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    )
    const cid2 = CID.parse(
      'bafybeigdyrztg6nbv3f2vzk3euvr63zjkvqlukmhhojz6kmiy7m7xvlhvq'
    )
    const mockDelta: delta.Delta = {
      priority: BigInt(3),
      elements: [],
      tombstones: []
    }
    const node = await CRDTNodeGetter.makeNode(mockDelta, [cid1, cid2])

    const links = Array.from(node.links())

    expect(links).toHaveLength(2)
    expect(links[0][1].equals(cid1)).toBe(true)
    expect(links[1][1].equals(cid2)).toBe(true)
  })
})

describe('makeNode', () => {
  it('should create a BlockView with encoded delta data and links', async () => {
    const d: delta.Delta = { some: 'data' } as any // Replace 'any' with the correct type for delta.Delta
    const heads: CID[] = [
      CID.parse('bafybeigdyrzt5xjzqmtgmbyew7zkk64un4qxpv6ysgtg3dvlnsmjqyulxa')
    ]

    const result = await CRDTNodeGetter.makeNode(d, heads)

    expect(result).toBeDefined()
    expect(result).toHaveProperty('cid')
    expect(result).toHaveProperty('value')
    expect(result.value).toHaveProperty('Data')
  })

  it('should handle null delta and create a BlockView with empty data', async () => {
    const heads: CID[] = [
      CID.parse('bafybeigdyrzt5xjzqmtgmbyew7zkk64un4qxpv6ysgtg3dvlnsmjqyulxa')
    ]

    const result = await CRDTNodeGetter.makeNode(null, heads)

    expect(result).toBeDefined()
    expect(result).toHaveProperty('cid')
    expect(result).toHaveProperty('value')
    expect(result.value).toHaveProperty('Data')
    expect((result.value as { Data: Uint8Array }).Data).toBeInstanceOf(Uint8Array)
    expect((result.value as { Data: Uint8Array }).Data.length).toBe(0)
  })

  it('should handle an empty heads array', async () => {
    const d: delta.Delta = { some: 'data' } as any // Replace 'any' with the correct type for delta.Delta

    const result = await CRDTNodeGetter.makeNode(d, [])

    expect(result).toBeDefined()
    expect(result).toHaveProperty('cid')
    expect(result).toHaveProperty('value')
    expect(result.value).toHaveProperty('Data')
  })
})
