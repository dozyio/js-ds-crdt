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

export interface delta {}

export namespace delta {
  export interface Delta {
    elements: delta.Element[]
    tombstones: delta.Element[]
    priority: bigint
  }

  export namespace Delta {
    let _codec: Codec<Delta>

    export const codec = (): Codec<Delta> => {
      if (_codec == null) {
        _codec = message<Delta>(
          (obj, w, opts = {}) => {
            if (opts.lengthDelimited !== false) {
              w.fork()
            }

            if (obj.elements != null) {
              for (const value of obj.elements) {
                w.uint32(10)
                delta.Element.codec().encode(value, w)
              }
            }

            if (obj.tombstones != null) {
              for (const value of obj.tombstones) {
                w.uint32(18)
                delta.Element.codec().encode(value, w)
              }
            }

            if (obj.priority != null && obj.priority !== 0n) {
              w.uint32(24)
              w.uint64(obj.priority)
            }

            if (opts.lengthDelimited !== false) {
              w.ldelim()
            }
          },
          (reader, length, opts = {}) => {
            const obj: any = {
              elements: [],
              tombstones: [],
              priority: 0n,
            }

            const end = length == null ? reader.len : reader.pos + length

            while (reader.pos < end) {
              const tag = reader.uint32()

              switch (tag >>> 3) {
                case 1: {
                  if (
                    opts.limits?.elements != null &&
                    obj.elements.length === opts.limits.elements
                  ) {
                    throw new MaxLengthError(
                      'Decode error - map field "elements" had too many elements',
                    )
                  }

                  obj.elements.push(
                    delta.Element.codec().decode(reader, reader.uint32(), {
                      limits: opts.limits?.elements$,
                    }),
                  )
                  break
                }
                case 2: {
                  if (
                    opts.limits?.tombstones != null &&
                    obj.tombstones.length === opts.limits.tombstones
                  ) {
                    throw new MaxLengthError(
                      'Decode error - map field "tombstones" had too many elements',
                    )
                  }

                  obj.tombstones.push(
                    delta.Element.codec().decode(reader, reader.uint32(), {
                      limits: opts.limits?.tombstones$,
                    }),
                  )
                  break
                }
                case 3: {
                  obj.priority = reader.uint64()
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

    export const encode = (obj: Partial<Delta>): Uint8Array => {
      return encodeMessage(obj, Delta.codec())
    }

    export const decode = (
      buf: Uint8Array | Uint8ArrayList,
      opts?: DecodeOptions<Delta>,
    ): Delta => {
      return decodeMessage(buf, Delta.codec(), opts)
    }
  }

  export interface Element {
    key: string
    id: string
    value: Uint8Array
  }

  export namespace Element {
    let _codec: Codec<Element>

    export const codec = (): Codec<Element> => {
      if (_codec == null) {
        _codec = message<Element>(
          (obj, w, opts = {}) => {
            if (opts.lengthDelimited !== false) {
              w.fork()
            }

            if (obj.key != null && obj.key !== '') {
              w.uint32(10)
              w.string(obj.key)
            }

            if (obj.id != null && obj.id !== '') {
              w.uint32(18)
              w.string(obj.id)
            }

            if (obj.value != null && obj.value.byteLength > 0) {
              w.uint32(26)
              w.bytes(obj.value)
            }

            if (opts.lengthDelimited !== false) {
              w.ldelim()
            }
          },
          (reader, length, opts = {}) => {
            const obj: any = {
              key: '',
              id: '',
              value: uint8ArrayAlloc(0),
            }

            const end = length == null ? reader.len : reader.pos + length

            while (reader.pos < end) {
              const tag = reader.uint32()

              switch (tag >>> 3) {
                case 1: {
                  obj.key = reader.string()
                  break
                }
                case 2: {
                  obj.id = reader.string()
                  break
                }
                case 3: {
                  obj.value = reader.bytes()
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

    export const encode = (obj: Partial<Element>): Uint8Array => {
      return encodeMessage(obj, Element.codec())
    }

    export const decode = (
      buf: Uint8Array | Uint8ArrayList,
      opts?: DecodeOptions<Element>,
    ): Element => {
      return decodeMessage(buf, Element.codec(), opts)
    }
  }

  let _codec: Codec<delta>

  export const codec = (): Codec<delta> => {
    if (_codec == null) {
      _codec = message<delta>(
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

  export const encode = (obj: Partial<delta>): Uint8Array => {
    return encodeMessage(obj, delta.codec())
  }

  export const decode = (
    buf: Uint8Array | Uint8ArrayList,
    opts?: DecodeOptions<delta>,
  ): delta => {
    return decodeMessage(buf, delta.codec(), opts)
  }
}
