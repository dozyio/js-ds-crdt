import { Key } from 'interface-datastore'
import { base32 } from 'multiformats/bases/base32'
import { CID } from 'multiformats/cid'
import * as multihash from 'multiformats/hashes/digest'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import type { Message, SignedMessage } from '@libp2p/interface'
import type { MultihashDigest } from 'multiformats/hashes/interface'

export const MaxVarintLen16 = 3
export const MaxVarintLen32 = 5
export const MaxVarintLen64 = 10

// Exported equivalent to `NewKeyFromBinary` in Go
export function newKeyFromBinary (rawKey: Uint8Array): Key {
  const encoded = base32.baseEncode(rawKey).toUpperCase() // Base32 encode without padding
  return new Key(`/${encoded}`)
}

// Exported equivalent to `MultihashToDsKey` in Go
export function multihashToDsKey (k: Uint8Array): Key {
  return newKeyFromBinary(k)
}

// Exported equivalent to `BinaryFromDsKey` in Go
export function binaryFromDsKey (k: Key): Uint8Array {
  const str = k.toString().slice(1) // Remove the leading '/'
  return base32.baseDecode(str.toLowerCase()) // Base32 decode (handle case-insensitivity)
}

// Exported equivalent to `DsKeyToMultihash` in Go
export function dsKeyToMultihash (dsKey: Key): MultihashDigest<number> {
  const binary = binaryFromDsKey(dsKey)
  return multihash.decode(binary) // Get the full MultihashDigest object
}

// Exported equivalent to `DsKeyToCidV1` in Go
export function dsKeyToCidV1 (dsKey: Key, codec: number): CID {
  const multihashDigest = dsKeyToMultihash(dsKey)
  return CID.createV1(codec, multihashDigest) // Use the full MultihashDigest object
}

// Convert BigInt to Uint8Array in little-endian format
export function bigintToUint8Array (bigint: bigint): Uint8Array {
  if (bigint === BigInt(0)) {
    return new Uint8Array([])
  }

  let hex = bigint.toString(16)

  // Ensure the string length is even (since each byte is 2 hex characters)
  if (hex.length % 2 !== 0) {
    hex = '0' + hex
  }

  const len = hex.length / 2
  const uint8Array = new Uint8Array(len)
  for (let i = 0; i < len; i++) {
    uint8Array[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  }

  // Reverse the byte order for little-endian format
  return uint8Array.reverse()
}

// Convert ArrayBuffer to BigInt in little-endian format
export function arrayBufferToBigInt (buffer: ArrayBufferLike): bigint {
  const view = new DataView(buffer)
  let result = BigInt(0)

  // Iterate over the buffer in 8-byte (64-bit) chunks
  for (let i = view.byteLength - 1; i >= 0; i--) {
    const byte = BigInt(view.getUint8(i))
    result = (result << BigInt(8)) | byte
  }

  return result
}

export async function msgIdFnStrictNoSign (msg: Message): Promise<Uint8Array> {
  const signedMessage = msg as SignedMessage
  const encodedSeqNum = new TextEncoder().encode(
    signedMessage.sequenceNumber.toString()
  )

  return hasher.encode(encodedSeqNum)
}

// Compare two uint8arrays - returns -1, 0, or 1
// Replacement for Buffer.compare
export function compareUint8Arrays (
  arr1: Uint8Array,
  arr2: Uint8Array
): -1 | 0 | 1 {
  if (arr1 === arr2) {
    return 0 // same object
  }

  if (arr1.length < arr2.length) {
    return -1
  }
  if (arr1.length > arr2.length) {
    return 1
  }

  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] < arr2[i]) {
      return -1
    }
    if (arr1[i] > arr2[i]) {
      return 1
    }
  }

  return 0 // arrays are equal
}

export function toUint8Array (value: any): Uint8Array {
  if (Buffer.isBuffer(value)) {
    return new Uint8Array(value)
  }

  if (value instanceof Uint8Array) {
    return value
  }

  throw new Error('Invalid value type')
}

// Uvarint decodes a uint64 from buf and returns that value and the
// number of bytes read (> 0). If an error occurred, the value is 0
// and the number of bytes n is <= 0 meaning:
//   - n == 0: buf too small;
//   - n < 0: value larger than 64 bits (overflow) and -n is the number of
//     bytes read.
export function uvarint (buf: ArrayBufferLike): [bigint, number] {
  const view = new Uint8Array(buf)
  let x = 0n
  let s = 0

  for (let i = 0; i < view.length; i++) {
    if (i === MaxVarintLen64) {
      // Catch byte reads past MaxVarintLen64.
      return [0n, -(i + 1)] // overflow
    }

    const b = view[i]
    if (b < 0x80) {
      if (i === MaxVarintLen64 - 1 && b > 1) {
        return [0n, -(i + 1)] // overflow
      }
      return [x | (BigInt(b) << BigInt(s)), i + 1]
    }

    x |= BigInt(b & 0x7f) << BigInt(s)
    s += 7
  }

  return [0n, 0]
}

// PutUvarint encodes a uint64 into buf and returns the number of bytes written.
// If the buffer is too small, PutUvarint will throw
export function putUvarint (buf: Uint8Array, x: bigint): number {
  let i = 0

  while (x >= 0x80n) { // 0x80n is the BigInt literal for 128
    buf[i] = Number(x & 0xffn) | 0x80 // Store the lower 7 bits with the continuation bit
    x >>= 7n // Shift right by 7 bits
    i++
    if (i >= buf.length) {
      throw new Error('Buffer too small')
    }
  }

  buf[i] = Number(x) // Store the last 7 bits without the continuation bit
  return i + 1
}

export function appendUvarint (buf: Uint8Array, x: bigint): Uint8Array {
  const parts: number[] = []

  while (x >= 0x80n) {
    parts.push(Number(x & 0xFFn) | 0x80)
    x >>= 7n
  }
  parts.push(Number(x & 0xFFn))

  const result = new Uint8Array(buf.length + parts.length)
  result.set(buf, 0)
  result.set(parts, buf.length)

  return result
}

// should really work on streams but passing full uint8array instead for now
export function readUvarint (buf: Uint8Array): bigint {
  let x = 0n
  let s = 0

  for (let i = 0; i < Math.min(buf.length, MaxVarintLen64); i++) {
    const b = buf[i]

    if (b < 0x80) {
      if (i === MaxVarintLen64 - 1 && b > 1) {
        throw new Error('Overflow')
      }
      return x | (BigInt(b) << BigInt(s))
    }

    x |= BigInt(b & 0x7f) << BigInt(s)
    s += 7
  }

  throw new Error('Overflow')
}
