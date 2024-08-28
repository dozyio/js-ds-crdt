import { base32 } from 'multiformats/bases/base32'
import { CID } from 'multiformats/cid'
import * as multihash from 'multiformats/hashes/digest'
import { describe, it, expect } from 'vitest'
import {
  newKeyFromBinary,
  multihashToDsKey,
  binaryFromDsKey,
  dsKeyToMultihash,
  dsKeyToCidV1,
  bigintToUint8Array,
  arrayBufferToBigInt
} from '../src/utils' // Adjust the import path as necessary

describe('utils.ts', () => {
  it('should create a valid Key from a raw multihash using newKeyFromBinary', () => {
    const rawKey = new Uint8Array([
      0x12, 0x20, 0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02
    ])
    const key = newKeyFromBinary(rawKey)
    const expectedKeyStr = `/${base32.baseEncode(rawKey).toUpperCase()}`
    expect(key.toString()).toBe(expectedKeyStr)
  })

  it('should create a valid Key from a multihash using multihashToDsKey', () => {
    const digest = multihash.create(
      0x12,
      new Uint8Array([
        0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02
      ])
    )
    const key = multihashToDsKey(digest.bytes)
    const expectedKeyStr = `/${base32.baseEncode(digest.bytes).toUpperCase()}`
    expect(key.toString()).toBe(expectedKeyStr)
  })

  it('should correctly decode a Key to its original multihash using binaryFromDsKey', () => {
    const rawKey = new Uint8Array([
      0x12, 0x20, 0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02
    ])
    const key = newKeyFromBinary(rawKey)
    const decoded = binaryFromDsKey(key)
    expect(decoded).toEqual(rawKey)
  })

  it('should correctly convert a Key back to a MultihashDigest using dsKeyToMultihash', () => {
    const digest = multihash.create(
      0x12,
      new Uint8Array([
        0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02
      ])
    )
    const key = multihashToDsKey(digest.bytes)
    const multihashDigest = dsKeyToMultihash(key)
    expect(multihashDigest.code).toBe(digest.code)
    expect(multihashDigest.digest).toEqual(digest.digest)
  })

  it('should create a valid CID from a Key using dsKeyToCidV1', () => {
    const digest = multihash.create(
      0x12,
      new Uint8Array([
        0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02
      ])
    )
    const key = multihashToDsKey(digest.bytes)
    const cid = dsKeyToCidV1(key, 0x70)
    const expectedCid = CID.createV1(0x70, digest)
    expect(cid.toString()).toBe(expectedCid.toString())
  })
})

describe('bigintToUint8Array', () => {
  it('should convert a BigInt to a Uint8Array in little-endian', () => {
    const input = BigInt('0x0123456789abcdef')
    const expected = new Uint8Array([
      0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01
    ])
    const result = bigintToUint8Array(input)
    expect(result).toEqual(expected)
  })

  it('should return an empty Uint8Array for BigInt 0', () => {
    const input = BigInt(0)
    const expected = new Uint8Array([])
    const result = bigintToUint8Array(input)
    expect(result).toEqual(expected)
  })

  it('should convert a large BigInt to a Uint8Array', () => {
    const input = BigInt('0x1234567890abcdef1234567890abcdef')
    const expected = new Uint8Array([
      0xef, 0xcd, 0xab, 0x90, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x90,
      0x78, 0x56, 0x34, 0x12
    ])
    const result = bigintToUint8Array(input)
    expect(result).toEqual(expected)
  })

  it('should handle single-byte BigInts correctly', () => {
    const input = BigInt(0xff)
    const expected = new Uint8Array([0xff])
    const result = bigintToUint8Array(input)
    expect(result).toEqual(expected)
  })

  it('should handle zero BigInt correctly', () => {
    const input = BigInt(0)
    const expected = new Uint8Array([])
    const result = bigintToUint8Array(input)
    expect(result).toEqual(expected)
  })

  it('should handle round-trip conversion', () => {
    const input = BigInt('0x1234567890abcdef1234567890abcdef')
    const uint8Array = bigintToUint8Array(input)
    const result = arrayBufferToBigInt(uint8Array.buffer)
    expect(result).toEqual(input)
  })
})

describe('arrayBufferToBigInt', () => {
  it('should convert a Uint8Array to a BigInt in little-endian', () => {
    const input = new Uint8Array([
      0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01
    ]).buffer
    const expected = BigInt('0x0123456789abcdef')
    const result = arrayBufferToBigInt(input)
    expect(result).toEqual(expected)
  })

  it('should convert a large Uint8Array to a BigInt', () => {
    const input = new Uint8Array([
      0xef, 0xcd, 0xab, 0x90, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x90,
      0x78, 0x56, 0x34, 0x12
    ]).buffer
    const expected = BigInt('0x1234567890abcdef1234567890abcdef')
    const result = arrayBufferToBigInt(input)
    expect(result).toEqual(expected)
  })

  it('should handle single-byte Uint8Arrays correctly', () => {
    const input = new Uint8Array([0xff]).buffer
    const expected = BigInt(0xff)
    const result = arrayBufferToBigInt(input)
    expect(result).toEqual(expected)
  })

  it('should handle an empty Uint8Array correctly', () => {
    const input = new Uint8Array([]).buffer
    const expected = BigInt(0)
    const result = arrayBufferToBigInt(input)
    expect(result).toEqual(expected)
  })

  it('should handle round-trip conversion', () => {
    const input = new Uint8Array([
      0xef, 0xcd, 0xab, 0x90, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x90,
      0x78, 0x56, 0x34, 0x12
    ]).buffer
    const bigIntValue = arrayBufferToBigInt(input)
    const uint8Array = bigintToUint8Array(bigIntValue)
    expect(uint8Array).toEqual(new Uint8Array(input))
  })
})
