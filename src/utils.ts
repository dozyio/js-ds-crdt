import { Key } from 'interface-datastore'
import { base32 } from 'multiformats/bases/base32'
import { CID } from 'multiformats/cid'
import * as multihash from 'multiformats/hashes/digest'
import type { MultihashDigest } from 'multiformats/hashes/interface'

// Exported equivalent to `NewKeyFromBinary` in Go
export function newKeyFromBinary (rawKey: Uint8Array): Key {
  const encoded = base32.encode(rawKey).toUpperCase() // Base32 encode without padding
  return new Key(`/${encoded}`)
}

// Exported equivalent to `MultihashToDsKey` in Go
export function multihashToDsKey (k: Uint8Array): Key {
  return newKeyFromBinary(k)
}

// Exported equivalent to `BinaryFromDsKey` in Go
export function binaryFromDsKey (k: Key): Uint8Array {
  const str = k.toString().slice(1) // Remove the leading '/'
  return base32.decode(str.toLowerCase()) // Base32 decode (handle case-insensitivity)
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
