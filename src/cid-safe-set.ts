import { Mutex } from 'async-mutex'
import type { CID } from 'multiformats'

export class CidSafeSet {
  private readonly set = new Set<string>()
  private readonly mux: Mutex = new Mutex()

  public async visit (c: CID): Promise<boolean> {
    const cidStr = c.toString()
    let b: boolean = false

    await this.mux.runExclusive(() => {
      if (this.set.has(cidStr)) {
        b = false
      } else {
        this.set.add(cidStr)
        b = true
      }
    })
    return b
  }

  public async add (c: CID): Promise<void> {
    await this.mux.runExclusive(() => {
      this.set.add(c.toString())
    })
  }

  public async remove (c: CID): Promise<void> {
    await this.mux.runExclusive(() => {
      this.set.delete(c.toString())
    })
  }

  public async has (c: CID): Promise<boolean> {
    let b: boolean = false

    await this.mux.runExclusive(() => {
      b = this.set.has(c.toString())
    })

    return b
  }
}
