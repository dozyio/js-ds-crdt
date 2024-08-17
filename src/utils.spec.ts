import { describe, it, expect } from 'vitest';
import { CID } from 'multiformats/cid';
import { base32 } from 'multiformats/bases/base32';
import * as multihash from 'multiformats/hashes/digest';
import {
  newKeyFromBinary,
  multihashToDsKey,
  binaryFromDsKey,
  dsKeyToMultihash,
  dsKeyToCidV1,
} from './utils'; // Adjust the import path as necessary

describe('utils.ts', () => {
  it('should create a valid Key from a raw multihash using newKeyFromBinary', () => {
    const rawKey = new Uint8Array([0x12, 0x20, 0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02]);
    const key = newKeyFromBinary(rawKey);
    const expectedKeyStr = `/${base32.encode(rawKey).toUpperCase()}`;
    expect(key.toString()).toBe(expectedKeyStr);
  });

  it('should create a valid Key from a multihash using multihashToDsKey', () => {
    const digest = multihash.create(0x12, new Uint8Array([0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02]));
    const key = multihashToDsKey(digest.bytes);
    const expectedKeyStr = `/${base32.encode(digest.bytes).toUpperCase()}`;
    expect(key.toString()).toBe(expectedKeyStr);
  });

  it('should correctly decode a Key to its original multihash using binaryFromDsKey', () => {
    const rawKey = new Uint8Array([0x12, 0x20, 0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02]);
    const key = newKeyFromBinary(rawKey);
    const decoded = binaryFromDsKey(key);
    expect(decoded).toEqual(rawKey);
  });

  it('should correctly convert a Key back to a MultihashDigest using dsKeyToMultihash', () => {
    const digest = multihash.create(0x12, new Uint8Array([0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02]));
    const key = multihashToDsKey(digest.bytes);
    const multihashDigest = dsKeyToMultihash(key);
    expect(multihashDigest.code).toBe(digest.code);
    expect(multihashDigest.digest).toEqual(digest.digest);
  });

  it('should create a valid CID from a Key using dsKeyToCidV1', () => {
    const digest = multihash.create(0x12, new Uint8Array([0x03, 0x25, 0x40, 0xd4, 0xa6, 0x77, 0x12, 0x09, 0x20, 0x02]));
    const key = multihashToDsKey(digest.bytes);
    const cid = dsKeyToCidV1(key, 0x70);
    const expectedCid = CID.createV1(0x70, digest);
    expect(cid.toString()).toBe(expectedCid.toString());
  });
});
