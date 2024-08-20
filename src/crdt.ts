import { createNode } from '@ipld/dag-pb'
import * as codec from '@ipld/dag-pb'
import { logger, type Logger } from '@libp2p/logger'
import { Mutex } from 'async-mutex'
import {
  Key,
  type Datastore as DSDatastore,
  type Batch as DSBatch
} from 'interface-datastore'
import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { Heads } from './heads'
import { CrdtNodeGetter } from './ipld'
import * as bpb from './pb/bcast'
import * as dpb from './pb/delta'
import { CRDTSet } from './set'
import { multihashToDsKey } from './utils'
import type * as dagPb from '@ipld/dag-pb'
import type { Identify } from '@libp2p/identify'
import type { Libp2p, PeerId, PubSub, ServiceMap } from '@libp2p/interface'
import type { HeliaLibp2p } from 'helia'

const headsNs = 'h' // heads
const setNs = 's' // set
const processedBlocksNs = 'b' // blocks
const dirtyBitKey = 'd' // dirty

// Common errors.
const ErrNoMoreBroadcast = new Error(
  'receiving blocks aborted since no new blocks will be broadcasted'
)

interface Broadcaster {
  broadcast(data: Uint8Array): Promise<void>
  setHandler(handler: (data: Uint8Array) => Promise<void>): void
  getSubscribers(): PeerId[]
}

// export interface SessionDAGService extends DAGService {
//   session(context: AbortSignal): DAGService;
// }

interface Options {
  logger: Logger
  rebroadcastInterval: number
  putHook?(key: string, value: Uint8Array): void
  deleteHook?(key: string): void
  numWorkers: number
  dagSyncerTimeout: number
  maxBatchDeltaSize: number
  repairInterval: number
  logInterval: number
  multiHeadProcessing: boolean
}

interface Stats {
  heads: CID[]
  maxHeight: bigint
  queuedJobs: number
}

export interface MyLibp2pServices extends ServiceMap {
  identify: Identify
  pubsub: PubSub
}

export function defaultOptions (): Options {
  return {
    logger: logger('crdt'),
    rebroadcastInterval: 5000, // 5 seconds in milliseconds
    numWorkers: 5,
    dagSyncerTimeout: 300000, // 5 minutes in milliseconds
    maxBatchDeltaSize: 1048576, // 1MB
    repairInterval: 3600000, // 1 hour in milliseconds
    logInterval: 60000, // 1 minute in milliseconds
    multiHeadProcessing: false,
    putHook: undefined,
    deleteHook: undefined
  }
}

export class Datastore {
  private readonly ctx: AbortController
  public options: Options
  private readonly logger: Logger
  public readonly store: DSDatastore
  public readonly namespace: Key
  private readonly set: CRDTSet
  public readonly heads: Heads
  public readonly dagService: HeliaLibp2p<Libp2p<MyLibp2pServices>>
  public readonly broadcaster: Broadcaster
  private readonly seenHeads: Map<CID, boolean>
  private curDelta: dpb.delta.Delta | null = null
  private readonly curDeltaMutex: Mutex = new Mutex()
  private readonly jobQueue: DagJob[]
  private readonly sendJobs: DagJob[]
  private readonly queuedChildren: CidSafeSet

  constructor (
    store: DSDatastore,
    namespace: Key,
    dagSyncer: HeliaLibp2p<Libp2p<MyLibp2pServices>>,
    broadcaster: Broadcaster,
    options?: Options
  ) {
    this.ctx = new AbortController()
    this.options = options ?? defaultOptions()
    this.logger = this.options.logger
    this.store = store
    this.namespace = namespace
    this.dagService = dagSyncer
    this.broadcaster = broadcaster
    this.seenHeads = new Map<CID, boolean>()
    this.jobQueue = []
    this.sendJobs = []
    this.queuedChildren = new CidSafeSet()

    // Initialize the CRDTSet and heads
    this.set = new CRDTSet(
      store,
      namespace.child(new Key(setNs)),
      this.logger,
      this.options.putHook,
      this.options.deleteHook
    )
    this.heads = new Heads(
      store,
      namespace.child(new Key(headsNs)),
      this.logger
    )

    this.handleNext()

    void this.scheduleDagWorker()
    void this.scheduleSendJobWorker()
    void this.scheduleRebroadcast()
    void this.scheduleRepair()
    void this.scheduleLogStats()
  }

  private async scheduleDagWorker (): Promise<void> {
    try {
      console.log('running dagWorker')
      await this.dagWorker()
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Error in dagWorker:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleDagWorker()
      }, 1000)
    }
  }

  private async scheduleSendJobWorker (): Promise<void> {
    try {
      console.log('running sendJobWorker')
      await this.sendJobWorker()
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Error in sendJobWorker:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleSendJobWorker()
      }, 1000)
    }
  }

  private async scheduleRebroadcast (): Promise<void> {
    try {
      console.log('running rebroadcast')
      await this.rebroadcast()
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Error in rebroadcast:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleRebroadcast()
      }, this.options.rebroadcastInterval)
    }
  }

  private async scheduleRepair (): Promise<void> {
    try {
      console.log('running repair')
      await this.repair()
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Error in repair:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleRepair()
      }, this.options.repairInterval)
    }
  }

  private async scheduleLogStats (): Promise<void> {
    try {
      console.log('running logStats')
      await this.logStats()
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Error in logStats:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleLogStats()
      }, this.options.logInterval)
    }
  }

  private handleNext (): void {
    // if (!this.broadcaster) return // offline
    this.broadcaster.setHandler(async (data: Uint8Array) => {
      try {
        this.logger.trace('Handling incoming pubsub message')
        console.log('Handling incoming pubsub message')
        const bCastHeads = await this.decodeBroadcast(data)

        const processHead = async (c: CID): Promise<void> => {
          try {
            await this.handleBlock(c)
          } catch (err) {
            this.logger.error(`error processing new head: ${err}`)
            console.error(`error processing new head: ${err}`)
          }
        }

        const curHeadCount = await this.heads.len()
        console.log('curHeadCount', curHeadCount)
        if (curHeadCount === 0) {
          const dg = new CrdtNodeGetter(this.dagService.blockstore)
          for (const head of bCastHeads) {
            const prio = await dg.getPriority(head)
            console.log('prio', prio)
            await this.heads.add(head, prio)
          }
        }

        for (const head of bCastHeads) {
          if (this.options.multiHeadProcessing) {
            processHead(head).catch((err) => { this.logger.error(err) })
          } else {
            await processHead(head)
          }
          this.seenHeads.set(head, true)
        }
      } catch (err) {
        if (err === ErrNoMoreBroadcast || this.ctx.signal.aborted) {
          return
        }
        this.logger.error(err)
        console.error(err)
      }
    })
  }

  private async enqueueJob (job: DagJob): Promise<void> {
    if (this.ctx.signal.aborted) {
      return
    }

    this.jobQueue.push(job)
  }

  private async enqueueSendJobs (job: DagJob): Promise<void> {
    if (this.ctx.signal.aborted) {
      return
    }

    this.sendJobs.push(job)
  }

  private async dequeueJob (): Promise<DagJob | null> {
    const job = this.jobQueue.length > 0 ? this.jobQueue.shift() : undefined
    return job ?? null
  }

  private async dequeueSendJob (): Promise<DagJob | null> {
    const job = this.sendJobs.length > 0 ? this.sendJobs.shift() : undefined
    return job ?? null
  }

  private async dagWorker (): Promise<void> {
    while (true) {
      const job = await this.dequeueJob()
      if (job === null) return // Queue closed, exit the worker

      try {
        const children = await this.processNode(
          job.root,
          job.rootPrio,
          job.delta,
          job.node
        )

        await this.enqueueSendJobs(
          new DagJob(
            job.session,
            job.nodeGetter,
            job.root,
            job.rootPrio,
            job.delta,
            job.node,
            children
          )
        )
      } catch (error) {
        this.logger.error(error)
        await this.MarkDirty()
        job.session.release()
      }
    }
  }

  private async sendJobWorker (): Promise<void> {
    while (true) {
      const job = await this.dequeueSendJob()
      if (job === null) return // Queue closed, exit the worker

      await this.enqueueJob(job)
    }
  }

  private async repair (): Promise<void> {
    if (this.options.repairInterval === 0) return

    let timer = setTimeout(() => {
      this.repairDAG().catch(err => {
        this.logger.error('Error in repairDAG:', err)
      })
    }, 0) // Fire immediately on start

    while (!this.ctx.signal.aborted) {
      await new Promise((resolve) =>
        setTimeout(resolve, this.options.repairInterval)
      )
      clearTimeout(timer)
      timer = setTimeout(() => {
        this.repairDAG().catch(err => {
          this.logger.error('Error in repairDAG:', err)
        })
      }, this.options.repairInterval) // Fire immediately on start
    }

    clearTimeout(timer)
  }

  private async repairDAG (): Promise<void> {
    const start = Date.now()

    const heads = await this.heads.list()
    const getter = new CrdtNodeGetter(this.dagService.blockstore)

    const nodes: Array<{ head: CID, node: CID }> = []
    const queued = new Set<string>()

    for (const head of heads.heads) {
      nodes.push({ head, node: head })
      queued.add(head.toString())
    }

    // let visitedNodes = 0
    // let lastPriority = 0n
    // let queuedNodes = nodes.length

    while (nodes.length > 0 && !this.ctx.signal.aborted) {
      const nodeInfo = nodes.shift()
      if (nodeInfo === undefined) {
        throw new Error('Expected a node to process, but got undefined.')
      }

      const { head, node: cur } = nodeInfo
      // const { node, delta } = await getter.getDelta(cur)
      const { node } = await getter.getDelta(cur)

      const isProcessed = await this.isProcessed(cur)
      if (!isProcessed) {
        await this.handleBranch(head, cur)
      }

      for (const link of node.Links) {
        if (!queued.has(link.Hash.toString())) {
          nodes.push({ head, node: link.Hash })
          queued.add(link.Hash.toString())
        }
      }

      // visitedNodes++
      // lastPriority = delta.priority
      // queuedNodes = nodes.length
    }

    this.logger.trace(`DAG repair finished. Took ${Date.now() - start} ms`)
    await this.MarkClean()
  }

  private dirtyKey (): Key {
    return this.namespace.child(new Key(dirtyBitKey))
  }

  public async MarkDirty (): Promise<void> {
    this.logger.error('Marking datastore as dirty')
    await this.store.put(this.dirtyKey(), new Uint8Array())
  }

  public async IsDirty (): Promise<boolean> {
    return this.store.has(this.dirtyKey())
  }

  public async MarkClean (): Promise<void> {
    this.logger.error('Marking datastore as clean')
    await this.store.delete(this.dirtyKey())
  }

  private async logStats (): Promise<void> {
    const interval = 5 * 60 * 1000 // 5 minutes

    while (!this.ctx.signal.aborted) {
      await new Promise((resolve) => setTimeout(resolve, interval))

      const heads = await this.heads.list()
      this.logger.trace(
        `Number of heads: ${heads.heads.length}. Max height: ${heads.maxHeight}. Queued jobs: ${this.jobQueue.length}. Dirty: ${await this.IsDirty()}`
      )
    }
  }

  private async decodeBroadcast (data: Uint8Array): Promise<CID[]> {
    const bcastData = bpb.bcast.CRDTBroadcast.decode(data)
    if (bcastData?.Heads !== undefined) {
      return Promise.all(
        bcastData.Heads.map((protoHead) => CID.decode(protoHead.Cid))
      )
    }
    throw new Error('Invalid broadcast data')
  }

  private encodeBroadcast (heads: CID[]): Uint8Array {
    const bcastData: bpb.bcast.CRDTBroadcast = {
      Heads: heads.map((c) => ({ Cid: c.bytes }))
    }

    return bpb.bcast.CRDTBroadcast.encode(bcastData)
  }

  private async rebroadcast (): Promise<void> {
    await this.rebroadcastHeads()
  }

  public async rebroadcastHeads (): Promise<void> {
    const heads = await this.heads.list()
    const headsToBroadcast: CID[] = []

    for (let i = 0; i < heads.heads.length; i++) {
      if (!this.seenHeads.has(heads.heads[i])) {
        headsToBroadcast.push(heads.heads[i])
      }
    }

    await this.broadcast(headsToBroadcast)

    this.seenHeads.clear()
  }

  private async broadcast (cids: CID[]): Promise<void> {
    if (cids.length === 0) return

    const bcastBytes = this.encodeBroadcast(cids)
    this.logger.trace(`Broadcasting ${cids}`)

    await this.broadcaster.broadcast(bcastBytes)
  }

  private async handleBlock (c: CID): Promise<void> {
    console.log('handling block', c.toString())
    const isProcessed = await this.isProcessed(c)
    if (isProcessed) {
      this.logger.trace(`${c} is known. Skip walking tree`)
      return
    }

    await this.handleBranch(c, c)
  }

  public async handleBranch (head: CID, c: CID): Promise<void> {
    const dg = new CrdtNodeGetter(this.dagService.blockstore)
    const session = new Mutex()

    await this.sendNewJobs(session, dg, head, 0n, [c])
  }

  private async sendNewJobs (
    session: Mutex,
    ng: CrdtNodeGetter,
    root: CID,
    rootPrio: bigint,
    children: CID[]
  ): Promise<void> {
    if (children.length === 0) return

    const goodDeltas = new Map<CID, boolean>()

    for await (const deltaOpt of ng.getDeltas(children)) {
      if (deltaOpt.err !== null && deltaOpt.err !== undefined) {
        this.logger.error(`Error getting delta: ${deltaOpt.err.message}`)
        throw deltaOpt.err
      }

      if (deltaOpt.node === null || deltaOpt.node === undefined) {
        this.logger.error('Error getting delta: node is null')
        continue
      }
      if (deltaOpt.delta === null || deltaOpt.delta === undefined) {
        this.logger.error('Error getting delta: delta is null')
        continue
      }

      const value = createNode(
        deltaOpt.node.Data ?? new Uint8Array(),
        deltaOpt.node.Links
      )
      const block = await Block.encode({ value, codec, hasher })

      goodDeltas.set(block.cid, true)

      const job = new DagJob(
        session,
        ng,
        root,
        rootPrio,
        deltaOpt.delta,
        deltaOpt.node,
        []
      )
      this.sendJobs.push(job)
    }

    for (const child of children) {
      if (!goodDeltas.has(child)) {
        this.logger.trace('GetDeltas did not include all children')
        this.queuedChildren.remove(child)
      }
    }
  }

  public async isProcessed (c: CID): Promise<boolean> {
    const key = this.processedBlockKey(c)
    return this.store.has(key)
  }

  private processedBlockKey (c: CID): Key {
    return this.namespace
      .child(new Key(processedBlocksNs))
      .child(new Key(c.toString()))
  }

  private async markProcessed (c: CID): Promise<void> {
    console.log('marking processed', c.toString())
    const key = this.processedBlockKey(c)
    await this.store.put(key, new Uint8Array())
  }

  public async get (key: Key): Promise<Uint8Array | null> {
    console.log('getting key', key.toString())
    return this.set.element(key.toString())
  }

  public async put (key: Key, value: Uint8Array): Promise<void> {
    console.log('putting key', key.toString())
    const delta = this.set.add(key.toString(), value)
    await this.publish(delta)
  }

  public async delete (key: Key): Promise<void> {
    const delta = await this.set.remove(key.toString())
    if (delta.tombstones.length === 0) return
    await this.publish(delta)
  }

  private async publish (delta: dpb.delta.Delta): Promise<void> {
    console.log('publishing delta', delta)
    const c = await this.addDAGNode(delta)
    await this.broadcast([c])
  }

  private async addDAGNode (delta: dpb.delta.Delta): Promise<CID> {
    console.log('adding dag node')
    const heads = await this.heads.list()
    const height = heads.maxHeight + 1n

    delta.priority = height

    const nd = await this.putBlock(heads.heads, height, delta)

    const value = createNode(nd.Data ?? new Uint8Array(), nd.Links)
    const block = await Block.encode({ value, codec, hasher })

    try {
      await this.processNode(block.cid, height, delta, nd)
    } catch (err: any) {
      this.logger.error(`Error processing node ${block.cid}: ${err}`)
      // TODO make dirty
    }

    return block.cid
  }

  private async putBlock (
    heads: CID[],
    height: bigint,
    delta: dpb.delta.Delta
  ): Promise<dagPb.PBNode> {
    delta.priority = height

    const links: codec.PBLink[] = []
    for (const head of heads) {
      links.push({ Name: '', Hash: head })
    }
    const nd = createNode(dpb.delta.Delta.encode(delta), links)
    const block = await Block.encode({ value: nd, codec, hasher })
    await this.dagService.blockstore.put(block.cid, block.bytes)

    return nd
  }

  public async processNode (
    root: CID,
    rootPrio: bigint,
    delta: dpb.delta.Delta,
    node: dagPb.PBNode
  ): Promise<CID[]> {
    console.log('processing node')
    const value = createNode(node.Data ?? new Uint8Array(), node.Links)
    const block = await Block.encode({ value, codec, hasher })

    const current = block.cid
    const blockKey = multihashToDsKey(current.multihash.bytes).toString()

    try {
      // First, merge the delta in this node.
      await this.set.merge(delta, blockKey)

      // Record that we have processed the node so that any other worker can skip it.
      await this.markProcessed(current)

      // Remove from the set that has the children which are queued for processing.
      this.queuedChildren.remove(block.cid)

      this.logger.error(
        `Merged delta from node ${current} (priority: ${delta.priority})`
      )

      const links = node.Links
      const children: CID[] = []

      // We reached the bottom. Our head must become a new head.
      if (links.length === 0) {
        await this.heads.add(root, rootPrio)
      }

      // Return children that:
      // a) Are not processed
      // b) Are not going to be processed by someone else.
      let addedAsHead = false

      for (const link of links) {
        const child = link.Hash

        const { isHead } = await this.heads.isHead(child)
        const isProcessed = await this.isProcessed(child)

        if (isHead) {
          // Reached one of the current heads. Replace it with the tip of this branch.
          await this.heads.replace(child, root, rootPrio)
          addedAsHead = true

          // If this head was already processed, continue.
          if (isProcessed) {
            continue
          }
        }

        // If the child has already been processed or someone else has reserved it for processing,
        // then we can make ourselves a head right away because we are not meant to replace an existing head.
        if (isProcessed || !this.queuedChildren.has(child)) {
          if (!addedAsHead) {
            // eslint-disable-next-line max-depth
            try {
              await this.heads.add(root, rootPrio)
            } catch (err) {
              this.logger.error(`Error adding head ${root}: ${err}`)
            }
          }
          addedAsHead = true
          continue
        }

        // We can return this child because it is not processed and we reserved it in the queue.
        children.push(child)
      }

      console.log('children', children)
      return children
    } catch (err) {
      this.logger.error(`Error processing node ${current}: ${err}`)
      throw err
    }
  }

  public async Batch (): Promise<DatastoreBatch> {
    return new DatastoreBatch(this)
  }

  public async Sync (prefix: Key): Promise<void> {
    if (
      'sync' in this.store &&
      typeof this.store.sync === 'function' &&
      'sync' in this.set &&
      typeof this.set.sync === 'function'
    ) {
      if (prefix.toString() === '/') {
        await this.store.sync(this.namespace)
      } else {
        await this.set.sync(prefix)
        await this.store.sync(this.heads.namespace)
      }
    }
  }

  public async Close (): Promise<void> {
    if (await this.IsDirty()) {
      this.logger.error('Datastore closed while marked as dirty')
    }
  }

  public async PrintDAG (): Promise<void> {
    const heads = await this.heads.list()
    const getter = new CrdtNodeGetter(this.dagService.blockstore)

    const set = new Set<string>()

    for (const head of heads.heads) {
      await this.printDAGRec(head, 0, getter, set)
    }
  }

  private async printDAGRec (
    from: CID,
    depth: number,
    getter: CrdtNodeGetter,
    set: Set<string>
  ): Promise<void> {
    const padding = ' '.repeat(depth)

    if (set.has(from.toString())) {
      // eslint-disable-next-line no-console
      console.log(`${padding}...`)
      return
    }

    const { node, delta } = await getter.getDelta(from)
    set.add(from.toString())

    const shortCID = from.toString().slice(-4)
    let line = `${padding}- ${delta.priority} | ${shortCID}: Add: {`

    for (const e of delta.elements) {
      line += `${e.key}:${e.value},`
    }

    line += '}. Rmv: {'
    for (const e of delta.tombstones) {
      line += `${e.key},`
    }

    line += '}. Links: {'
    for (const link of node.Links) {
      line += `${link.Hash.toString().slice(-4)},`
    }

    line += '}:'
    // eslint-disable-next-line no-console
    console.log(line)

    for (const link of node.Links) {
      await this.printDAGRec(link.Hash, depth + 1, getter, set)
    }
  }

  public async InternalStats (): Promise<Stats> {
    const heads = await this.heads.list()
    return {
      heads: heads.heads,
      maxHeight: heads.maxHeight,
      queuedJobs: this.jobQueue.length
    }
  }

  public async addToDelta (key: string, value: Uint8Array): Promise<number> {
    const newDelta = this.set.add(key, value)
    return this.updateDelta(newDelta)
  }

  private async updateDelta (newDelta: dpb.delta.Delta): Promise<number> {
    return this.curDeltaMutex.runExclusive(() => {
      if (this.curDelta !== null) {
        this.curDelta = this.mergeDeltas(this.curDelta, newDelta)
        return dpb.delta.Delta.encode(this.curDelta).length
      }
      return 0
    })
  }

  private mergeDeltas (
    d1: dpb.delta.Delta,
    d2: dpb.delta.Delta
  ): dpb.delta.Delta {
    return {
      elements: [...(Array.isArray(d1.elements) ? d1.elements : []), ...(Array.isArray(d2.elements) ? d2.elements : [])],
      tombstones: [...(Array.isArray(d1.tombstones) ? d1.tombstones : []), ...(Array.isArray(d2.tombstones) ? d2.tombstones : [])],
      priority: d2.priority > d1.priority ? d2.priority : d1.priority
    }
  }

  private async updateDeltaWithRemove (
    key: string,
    newDelta: dpb.delta.Delta
  ): Promise<number> {
    return this.curDeltaMutex.runExclusive(() => {
      if (this.curDelta !== null) {
        const elements = this.curDelta.elements.filter((e) => e.key !== key)
        this.curDelta.elements = elements
      } else {
        this.curDelta = newDelta
      }
      this.curDelta = this.mergeDeltas(this.curDelta, newDelta)
      return dpb.delta.Delta.encode(this.curDelta).length
    })
  }

  public async rmvToDelta (key: string): Promise<number> {
    const newDelta = await this.set.remove(key)
    return this.updateDeltaWithRemove(key, newDelta)
  }

  public async publishDelta (): Promise<void> {
    await this.curDeltaMutex.runExclusive(async () => {
      if (this.curDelta === null) {
        return // No delta to publish
      }

      try {
        // Publish the current delta
        await this.publish(this.curDelta)
      } finally {
        // Clear the current delta after publishing
        this.curDelta = null
      }
    })
  }
}

class DagJob {
  constructor (
    public session: Mutex,
    public nodeGetter: CrdtNodeGetter,
    public root: CID,
    public rootPrio: bigint,
    public delta: dpb.delta.Delta,
    public node: dagPb.PBNode,
    public children: CID[]
  ) { }
}

class CidSafeSet {
  private readonly set = new Set<string>()

  public visit (c: CID): boolean {
    const cidStr = c.toString()
    if (this.set.has(cidStr)) {
      return false
    } else {
      this.set.add(cidStr)
      return true
    }
  }

  public remove (c: CID): void {
    this.set.delete(c.toString())
  }

  public has (c: CID): boolean {
    return this.set.has(c.toString())
  }
}

class DatastoreBatch implements DSBatch {
  constructor (private readonly store: Datastore) { }

  async put (key: Key, value: Uint8Array): Promise<void> {
    const size = await this.store.addToDelta(key.toString(), value)
    if (size > this.store.options.maxBatchDeltaSize) {
      // eslint-disable-next-line no-console
      console.log('Delta size exceeded max, committing batch')
      await this.commit()
    }
  }

  async delete (key: Key): Promise<void> {
    const size = await this.store.rmvToDelta(key.toString())
    if (size > this.store.options.maxBatchDeltaSize) {
      // eslint-disable-next-line no-console
      console.log('Delta size exceeded max, committing batch')
      await this.commit()
    }
  }

  async commit (): Promise<void> {
    await this.store.publishDelta()
  }
}
