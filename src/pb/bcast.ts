/* eslint-disable import/export */
/* eslint-disable complexity */
/* eslint-disable @typescript-eslint/no-namespace */
/* eslint-disable @typescript-eslint/no-unnecessary-boolean-literal-compare */
/* eslint-disable @typescript-eslint/no-empty-interface */

import {
  type Codec,
  decodeMessage,
  type DecodeOptions,
  encodeMessage,
  MaxLengthError,
  message,
} from 'protons-runtime'
import { alloc as uint8ArrayAlloc } from 'uint8arrays/alloc'
import type { Uint8ArrayList } from 'uint8arraylist'

export interface bcast {}

export namespace bcast {
  export interface CRDTBroadcast {
    Heads: bcast.Head[]
  }

  export namespace CRDTBroadcast {
    let _codec: Codec<CRDTBroadcast>

    export const codec = (): Codec<CRDTBroadcast> => {
      if (_codec == null) {
        _codec = message<CRDTBroadcast>(
          (obj, w, opts = {}) => {
            if (opts.lengthDelimited !== false) {
              w.fork()
            }

            if (obj.Heads != null) {
              for (const value of obj.Heads) {
                w.uint32(10)
                bcast.Head.codec().encode(value, w)
              }
            }

            if (opts.lengthDelimited !== false) {
              w.ldelim()
            }
          },
          (reader, length, opts = {}) => {
            const obj: any = {
              Heads: [],
            }

            const end = length == null ? reader.len : reader.pos + length

            while (reader.pos < end) {
              const tag = reader.uint32()

              switch (tag >>> 3) {
                case 1: {
                  if (
                    opts.limits?.Heads != null &&
                    obj.Heads.length === opts.limits.Heads
                  ) {
                    throw new MaxLengthError(
                      'Decode error - map field "Heads" had too many elements',
                    )
                  }

                  obj.Heads.push(
                    bcast.Head.codec().decode(reader, reader.uint32(), {
                      limits: opts.limits?.Heads$,
                    }),
                  )
                  break
                }
                default: {
                  reader.skipType(tag & 7)
                  break
                }
              }
            }

            return obj
          },
        )
      }

      return _codec
    }

    export const encode = (obj: Partial<CRDTBroadcast>): Uint8Array => {
      return encodeMessage(obj, CRDTBroadcast.codec())
    }

    export const decode = (
      buf: Uint8Array | Uint8ArrayList,
      opts?: DecodeOptions<CRDTBroadcast>,
    ): CRDTBroadcast => {
      return decodeMessage(buf, CRDTBroadcast.codec(), opts)
    }
  }

  export interface Head {
    Cid: Uint8Array
  }

  export namespace Head {
    let _codec: Codec<Head>

    export const codec = (): Codec<Head> => {
      if (_codec == null) {
        _codec = message<Head>(
          (obj, w, opts = {}) => {
            if (opts.lengthDelimited !== false) {
              w.fork()
            }

            if (obj.Cid != null && obj.Cid.byteLength > 0) {
              w.uint32(10)
              w.bytes(obj.Cid)
            }

            if (opts.lengthDelimited !== false) {
              w.ldelim()
            }
          },
          (reader, length, opts = {}) => {
            const obj: any = {
              Cid: uint8ArrayAlloc(0),
            }

            const end = length == null ? reader.len : reader.pos + length

            while (reader.pos < end) {
              const tag = reader.uint32()

              switch (tag >>> 3) {
                case 1: {
                  obj.Cid = reader.bytes()
                  break
                }
                default: {
                  reader.skipType(tag & 7)
                  break
                }
              }
            }

            return obj
          },
        )
      }

      return _codec
    }

    export const encode = (obj: Partial<Head>): Uint8Array => {
      return encodeMessage(obj, Head.codec())
    }

    export const decode = (
      buf: Uint8Array | Uint8ArrayList,
      opts?: DecodeOptions<Head>,
    ): Head => {
      return decodeMessage(buf, Head.codec(), opts)
    }
  }

  let _codec: Codec<bcast>

  export const codec = (): Codec<bcast> => {
    if (_codec == null) {
      _codec = message<bcast>(
        (obj, w, opts = {}) => {
          if (opts.lengthDelimited !== false) {
            w.fork()
          }

          if (opts.lengthDelimited !== false) {
            w.ldelim()
          }
        },
        (reader, length, opts = {}) => {
          const obj: any = {}

          const end = length == null ? reader.len : reader.pos + length

          while (reader.pos < end) {
            const tag = reader.uint32()

            switch (tag >>> 3) {
              default: {
                reader.skipType(tag & 7)
                break
              }
            }
          }

          return obj
        },
      )
    }

    return _codec
  }

  export const encode = (obj: Partial<bcast>): Uint8Array => {
    return encodeMessage(obj, bcast.codec())
  }

  export const decode = (
    buf: Uint8Array | Uint8ArrayList,
    opts?: DecodeOptions<bcast>,
  ): bcast => {
    return decodeMessage(buf, bcast.codec(), opts)
  }
}
