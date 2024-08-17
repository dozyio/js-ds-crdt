import { Key } from 'interface-datastore';
import { base32 } from 'multiformats/bases/base32';
import { CID } from 'multiformats/cid';
import * as multihash from 'multiformats/hashes/digest';
import type { MultihashDigest } from 'multiformats/hashes/interface';

// Exported equivalent to `NewKeyFromBinary` in Go
export function newKeyFromBinary(rawKey: Uint8Array): Key {
  const encoded = base32.encode(rawKey).toUpperCase(); // Base32 encode without padding
  return new Key(`/${encoded}`);
}

// Exported equivalent to `MultihashToDsKey` in Go
export function multihashToDsKey(k: Uint8Array): Key {
  return newKeyFromBinary(k);
}

// Exported equivalent to `BinaryFromDsKey` in Go
export function binaryFromDsKey(k: Key): Uint8Array {
  const str = k.toString().slice(1); // Remove the leading '/'
  return base32.decode(str.toLowerCase()); // Base32 decode (handle case-insensitivity)
}

// Exported equivalent to `DsKeyToMultihash` in Go
export function dsKeyToMultihash(dsKey: Key): MultihashDigest<number> {
  const binary = binaryFromDsKey(dsKey);
  return multihash.decode(binary); // Get the full MultihashDigest object
}

// Exported equivalent to `DsKeyToCidV1` in Go
export function dsKeyToCidV1(dsKey: Key, codec: number): CID {
  const multihashDigest = dsKeyToMultihash(dsKey);
  return CID.createV1(codec, multihashDigest); // Use the full MultihashDigest object
}
