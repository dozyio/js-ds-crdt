import { prefixLogger } from '@libp2p/logger'
import { Mutex } from 'async-mutex'
import {
  Key,
  type Datastore as DSDatastore,
  type Query,
  type Pair
} from 'interface-datastore'
import { CID } from 'multiformats/cid'
// import debug from 'weald'
import { CidSafeSet } from './cid-safe-set'
import { DatastoreBatch } from './datastore-batch'
import { Heads } from './heads'
import { CRDTNodeGetter } from './ipld'
import * as bpb from './pb/bcast'
import * as dpb from './pb/delta'
import { CRDTSet, type IBloomFilter } from './set'
import { multihashToDsKey } from './utils'
import type { Identify } from '@libp2p/identify'
import type {
  ComponentLogger,
  Libp2p,
  Logger,
  PeerId,
  PubSub,
  ServiceMap
} from '@libp2p/interface'
import type { HeliaLibp2p } from 'helia'
import type { BlockView } from 'multiformats'

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

export interface Options {
  loggerPrefix: string // ComponentLogger
  rebroadcastInterval: number
  bloomFilter?: IBloomFilter | null
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

export interface CRDTLibp2pServices extends ServiceMap {
  identify: Identify
  pubsub: PubSub
}

export function defaultOptions (): Options {
  return {
    loggerPrefix: 'crdt',
    rebroadcastInterval: 5000, // 5 seconds in milliseconds
    bloomFilter: undefined,
    putHook: undefined,
    deleteHook: undefined,
    numWorkers: 1,
    dagSyncerTimeout: 300000, // 5 minutes in milliseconds
    maxBatchDeltaSize: 1048576, // 1MB
    repairInterval: 3600000, // 1 hour in milliseconds
    logInterval: 60000, // 1 minute in milliseconds
    multiHeadProcessing: false
  }
}

class DagJob {
  constructor (
    public session: Mutex,
    public nodeGetter: CRDTNodeGetter,
    public root: CID,
    public rootPrio: bigint,
    public delta: dpb.delta.Delta,
    public node: BlockView,
    public children: CID[]
  ) { }
}

export class CRDTDatastore {
  private readonly ctx: AbortController
  public options: Options
  private readonly prefixedLogger: ComponentLogger
  public readonly logger: Logger
  public readonly store: DSDatastore
  public readonly namespace: Key
  private readonly set: CRDTSet
  public readonly heads: Heads
  public readonly dagService: HeliaLibp2p<Libp2p<CRDTLibp2pServices>>
  public readonly broadcaster: Broadcaster
  private readonly seenHeadsMux: Mutex = new Mutex()
  private readonly seenHeads: Map<CID, boolean>
  private curDelta: dpb.delta.Delta | null = null
  private readonly curDeltaMutex: Mutex = new Mutex()
  private readonly jobQueue: DagJob[]
  private readonly sendJobs: DagJob[]
  private readonly queuedChildren: CidSafeSet

  constructor (
    store: DSDatastore,
    namespace: Key,
    dagSyncer: HeliaLibp2p<Libp2p<CRDTLibp2pServices>>,
    broadcaster: Broadcaster,
    options?: Options
  ) {
    this.ctx = new AbortController()
    this.options = options ?? defaultOptions()
    this.prefixedLogger = prefixLogger(this.options.loggerPrefix)
    this.logger = this.prefixedLogger.forComponent('crdt')
    this.store = store
    this.namespace = namespace
    this.dagService = dagSyncer
    this.broadcaster = broadcaster
    this.seenHeads = new Map<CID, boolean>()
    this.jobQueue = []
    this.sendJobs = []
    this.queuedChildren = new CidSafeSet()

    // debug.enable(`${this.options.loggerPrefix}*`) // 'crdt*,*crdt:trace')
    // debug.enable('*,*trace')

    // Initialize the CRDTSet and heads
    this.set = new CRDTSet(
      store,
      namespace.child(new Key(setNs)),
      this.prefixedLogger.forComponent('set'),
      this.options.bloomFilter,
      this.options.putHook,
      this.options.deleteHook
    )

    this.heads = new Heads(
      store,
      namespace.child(new Key(headsNs)),
      this.prefixedLogger.forComponent('heads')
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
      await this.dagWorker()
    } catch (err) {
      this.logger.error('Error in dagWorker:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleDagWorker()
      }, 1)
    }
  }

  private async scheduleSendJobWorker (): Promise<void> {
    try {
      await this.sendJobWorker()
    } catch (err) {
      this.logger.error('Error in sendJobWorker:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleSendJobWorker()
      }, 1)
    }
  }

  private async scheduleRebroadcast (): Promise<void> {
    try {
      this.logger('running rebroadcast')
      await this.rebroadcast()
    } catch (err) {
      // eslint-disable-next-line no-console
      this.logger.error('Error in rebroadcast:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleRebroadcast()
      }, this.options.rebroadcastInterval)
    }
  }

  private async scheduleRepair (): Promise<void> {
    try {
      this.logger('running repair')
      await this.repair()
    } catch (err) {
      this.logger.error('Error in repair:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleRepair()
      }, this.options.repairInterval)
    }
  }

  private async scheduleLogStats (): Promise<void> {
    try {
      this.logger('running logStats')
      await this.logStats()
    } catch (err) {
      this.logger.error('Error in logStats:', err)
    } finally {
      setTimeout(() => {
        void this.scheduleLogStats()
      }, this.options.logInterval)
    }
  }

  private handleNext (): void {
    this.broadcaster.setHandler(async (data: Uint8Array) => {
      try {
        this.logger('Handling incoming pubsub message')
        const bCastHeads = await this.decodeBroadcast(data)

        const processHead = async (c: CID): Promise<void> => {
          try {
            await this.handleBlock(c)
          } catch (err) {
            this.logger.error(`error processing new head: ${err}`)
          }
        }

        const curHeadCount = await this.heads.len()
        this.logger('curHeadCount', curHeadCount)
        if (curHeadCount === 0) {
          const dg = new CRDTNodeGetter(
            this.dagService.blockstore,
            this.prefixedLogger.forComponent('ipld')
          )
          for (const head of bCastHeads) {
            const prio = await dg.getPriority(head)
            this.logger('prio', prio)
            await this.heads.add(head, prio)
          }
        }

        for (const head of bCastHeads) {
          if (this.options.multiHeadProcessing) {
            processHead(head).catch((err) => {
              this.logger.error(err)
            })
          } else {
            await processHead(head)
          }

          await this.seenHeadsMux.runExclusive(async () => {
            this.seenHeads.set(head, true)
          })
        }
      } catch (err) {
        if (err === ErrNoMoreBroadcast || this.ctx.signal.aborted) {
          return
        }
        this.logger.error('error parsing broadcast', err)
      }
    })
  }

  private enqueueJob (job: DagJob): void {
    if (this.ctx.signal.aborted) {
      return
    }

    this.jobQueue.push(job)
  }

  private enqueueSendJob (job: DagJob): void {
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
    const job = await this.dequeueJob()
    if (job === null) return

    this.logger('dagWorker has job')
    let children: CID[]
    try {
      children = await this.processNode(
        job.root,
        job.rootPrio,
        job.delta,
        job.node
      )
    } catch (err: any) {
      this.logger.error(`Error processing node: ${err}`)
      await this.markDirty()
      job.session.release()
      return
    }

    try {
      await this.sendNewJobs(
        job.session,
        job.nodeGetter,
        job.root,
        job.rootPrio,
        children
      )
    } catch (error) {
      this.logger.error(error)
      await this.markDirty()
    }
    job.session.release()
  }

  private async sendJobWorker (): Promise<void> {
    const job = await this.dequeueSendJob()
    if (job === null) return

    this.logger('sendJobWorker has job')
    this.enqueueJob(job)
  }

  private async repair (): Promise<void> {
    if (this.options.repairInterval === 0) return

    let timer = setTimeout(() => {
      this.repairDAG().catch((err) => {
        this.logger.error('Error in repairDAG:', err)
      })
    }, 0) // Fire immediately on start

    while (!this.ctx.signal.aborted) {
      await new Promise((resolve) =>
        setTimeout(resolve, this.options.repairInterval)
      )
      clearTimeout(timer)
      timer = setTimeout(() => {
        this.repairDAG().catch((err) => {
          this.logger.error('Error in repairDAG:', err)
        })
      }, this.options.repairInterval) // Fire immediately on start
    }

    clearTimeout(timer)
  }

  private async repairDAG (): Promise<void> {
    const start = Date.now()

    const heads = await this.heads.list()
    const getter = new CRDTNodeGetter(
      this.dagService.blockstore,
      this.prefixedLogger.forComponent('ipld')
    )

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

      for (const link of node.links()) {
        if (!queued.has(link[1].toString())) {
          queued.add(link[1].toString())
          nodes.push({ head, node: link[1] })
        }
      }

      // visitedNodes++
      // lastPriority = delta.priority
      // queuedNodes = nodes.length
    }

    this.logger.trace(`DAG repair finished. Took ${Date.now() - start} ms`)
    await this.markClean()
  }

  private dirtyKey (): Key {
    return this.namespace.child(new Key(dirtyBitKey))
  }

  public async markDirty (): Promise<void> {
    this.logger.error('Marking datastore as dirty')
    await this.store.put(this.dirtyKey(), new Uint8Array())
  }

  public async isDirty (): Promise<boolean> {
    return this.store.has(this.dirtyKey())
  }

  public async markClean (): Promise<void> {
    this.logger('Marking datastore as clean')
    await this.store.delete(this.dirtyKey())
  }

  public async logStats (): Promise<void> {
    const interval = 5 * 60 * 1000 // 5 minutes

    while (!this.ctx.signal.aborted) {
      await new Promise((resolve) => setTimeout(resolve, interval))

      const heads = await this.heads.list()
      this.logger(
        `Number of heads: ${heads.heads.length}. Max height: ${heads.maxHeight}. Queued jobs: ${this.jobQueue.length}. Dirty: ${await this.isDirty()}`
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

    await this.seenHeadsMux.runExclusive(async () => {
      for (let i = 0; i < heads.heads.length; i++) {
        if (!this.seenHeads.has(heads.heads[i])) {
          headsToBroadcast.push(heads.heads[i])
        }
      }
    })

    await this.broadcast(headsToBroadcast)

    await this.seenHeadsMux.runExclusive(async () => {
      this.seenHeads.clear()
    })
  }

  private async broadcast (cids: CID[]): Promise<void> {
    if (cids.length === 0) return

    const bcastBytes = this.encodeBroadcast(cids)
    this.logger(`Broadcasting ${cids}`)

    await this.broadcaster.broadcast(bcastBytes)
  }

  private async handleBlock (c: CID): Promise<void> {
    // this.logger('handling block', c.toString())
    try {
      const isProcessed = await this.isProcessed(c)
      if (isProcessed) {
        // this.logger(`${c} is known. Skip walking tree`)
        return
      }
    } catch (err) {
      this.logger.error(`Error checking if block ${c} is processed: ${err}`)
      throw err
    }

    await this.handleBranch(c, c)
  }

  public async handleBranch (head: CID, c: CID): Promise<void> {
    this.logger('handling branch', head.toString(), c.toString())
    const dg = new CRDTNodeGetter(
      this.dagService.blockstore,
      this.prefixedLogger.forComponent('ipld')
    )
    const session = new Mutex()

    await this.sendNewJobs(session, dg, head, 0n, [c])
  }

  private async sendNewJobs (
    session: Mutex,
    ng: CRDTNodeGetter,
    root: CID,
    rootPrio: bigint,
    children: CID[]
  ): Promise<void> {
    this.logger(
      'sending new jobs',
      root.toString(),
      rootPrio.toString(),
      children.map((c) => c.toString())
    )
    if (children.length === 0) {
      this.logger('children.length === 0')
      return
    }

    // Special case for root
    if (rootPrio === 0n) {
      const prio = await ng.getPriority(children[0])
      rootPrio = prio
    }

    const goodDeltas = new Set<string>()

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

      goodDeltas.add(deltaOpt.node.cid.toString())
      this.logger('goodDeltas.set', deltaOpt.node.cid.toString())

      const job = new DagJob(
        session,
        ng,
        root,
        rootPrio,
        deltaOpt.delta,
        deltaOpt.node,
        []
      )
      this.enqueueSendJob(job)
    }

    // This is a safe-guard in case GetDeltas() returns less deltas than
    // asked for. It clears up any children that could not be fetched from
    // the queue. The rest will remove themselves in processNode().
    // Hector: as far as I know, this should not execute unless errors
    // happened.
    for (const child of children) {
      if (!goodDeltas.has(child.toString())) {
        this.logger.error(
          'GetDeltas did not include all children',
          child.toString()
        )
        await this.queuedChildren.remove(child)
      }
    }
  }

  public async isProcessed (c: CID): Promise<boolean> {
    const key = this.processedBlockKey(c)
    const isProcessed = await this.store.has(key)
    return isProcessed
  }

  private processedBlockKey (c: CID): Key {
    return this.namespace
      .child(new Key(processedBlocksNs))
      .child(new Key(c.toString()))
  }

  private async markProcessed (c: CID): Promise<void> {
    try {
      const key = this.processedBlockKey(c)
      await this.store.put(key, new Uint8Array())
    } catch (err) {
      this.logger.error(`Error marking block ${c} as processed: ${err}`)
      throw err
    }
  }

  public async query (q: Query): Promise<Pair[]> {
    // TODO needs work
    return this.set.elements(q)
  }

  public async get (key: Key): Promise<Uint8Array | null> {
    this.logger('getting key', key.toString())
    const result = await this.set.element(key.toString())
    return result
  }

  public async put (key: Key, value: Uint8Array): Promise<void> {
    this.logger('putting key', key.toString())
    const delta = this.set.add(key.toString(), value)
    await this.publish(delta)
  }

  public async delete (key: Key): Promise<void> {
    this.logger('deleting key', key.toString())
    const delta = await this.set.remove(key.toString())
    if (delta.tombstones.length === 0) return
    await this.publish(delta)
  }

  private async publish (delta: dpb.delta.Delta): Promise<void> {
    this.logger('publishing delta', delta)
    const c = await this.addDAGNode(delta)
    await this.broadcast([c])
  }

  private async addDAGNode (delta: dpb.delta.Delta): Promise<CID> {
    let heads
    try {
      heads = await this.heads.list()
    } catch (err) {
      this.logger.error(`Error getting heads: ${err}`)
      throw err
    }

    const height = heads.maxHeight + 1n

    delta.priority = height

    let nd
    try {
      nd = await this.putBlock(heads.heads, height, delta)
    } catch (err) {
      this.logger.error(`Error putting block: ${err}`)
      throw err
    }

    let children: CID[]
    try {
      children = await this.processNode(nd.cid, height, delta, nd)
      if (children.length !== 0) {
        this.logger.error('bug: created a block to unknown children')
      }
    } catch (err: any) {
      this.logger.error(`Error processing node ${nd.cid}: ${err}`)
      await this.markDirty()
    }

    return nd.cid
  }

  private async putBlock (
    heads: CID[],
    height: bigint,
    delta: dpb.delta.Delta
  ): Promise<BlockView> {
    this.logger('putting block', height.toString())
    if (delta != null) {
      delta.priority = height
    }

    const node = await CRDTNodeGetter.makeNode(delta, heads)
    await this.dagService.blockstore.put(node.cid, node.bytes)

    return node
  }

  public async processNode (
    root: CID,
    rootPrio: bigint,
    delta: dpb.delta.Delta,
    node: BlockView
  ): Promise<CID[]> {
    const current = node.cid
    const blockKey = multihashToDsKey(current.multihash.bytes).toString()

    try {
      // First, merge the delta in this node.
      await this.set.merge(delta, blockKey)
    } catch (err: any) {
      this.logger.error(`Error merging delta: ${err}`)
      return []
    }

    try {
      // Record that we have processed the node so that any other worker can skip it.
      await this.markProcessed(current)
    } catch (err: any) {
      this.logger.error(`Error marking processed: ${err}`)
      return []
    }

    // Remove from the set that has the children which are queued for processing.
    try {
      await this.queuedChildren.remove(node.cid)
    } catch (err: any) {
      this.logger.error(`Error removing from queuedChildren: ${err}`)
      return []
    }

    this.logger(
      `Merged delta from node ${current} (priority: ${delta.priority})`
    )

    try {
      const links = Array.from(node.links())
      // const links = node.links()
      const children: CID[] = []

      // We reached the bottom. Our head must become a new head.
      if (links.length === 0) {
        await this.heads.add(root, rootPrio)
      }

      // Return children that:
      // a) Are not processed
      // b) Are not going to be processed by someone else.
      // For every other child, add our node as Head.

      let addedAsHead = false // small optimization to avoid adding as head multiple times.

      for (const link of links) {
        const child = link[1] // cid

        const { isHead } = await this.heads.isHead(child)
        const isProcessed = await this.isProcessed(child)

        if (isHead) {
          this.logger('isHead', child.toString())
          // Reached one of the current heads. Replace it with the tip of this branch.
          await this.heads.replace(child, root, rootPrio)
          addedAsHead = true

          // If this head was already processed, continue this
          // protects the case when something is a head but was
          // not processed (potentially could happen during
          // first sync when heads are set before processing, a
          // both a node and its child are heads - which I'm not
          // sure if it can happen at all, but good to safeguard
          // for it).
          if (isProcessed) {
            continue
          }
        }

        // If the child has already been processed or someone else has reserved it for processing,
        // then we can make ourselves a head right away because we are not meant to replace an existing head.
        if (isProcessed || !await this.queuedChildren.visit(child)) {
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

      this.logger('children', children)
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

  public async close (): Promise<void> {
    if (await this.isDirty()) {
      this.logger.error('Datastore closed while marked as dirty')
    }
  }

  public async printDAG (): Promise<void> {
    const heads = await this.heads.list()
    const getter = new CRDTNodeGetter(
      this.dagService.blockstore,
      this.prefixedLogger.forComponent('ipld')
    )

    const set = new Set<string>()

    for (const head of heads.heads) {
      await this.printDAGRec(head, 0n, getter, set)
    }
  }

  private async printDAGRec (
    from: CID,
    depth: bigint,
    getter: CRDTNodeGetter,
    set: Set<string>
  ): Promise<void> {
    let padding = ''
    for (let i = 0n; i < depth; i++) {
      padding += ' '
    }

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
    for (const link of node.links()) {
      line += `${link[1].toString().slice(-4)},`
    }

    line += '}:'
    // eslint-disable-next-line no-console
    console.log(line)

    for (const link of node.links()) {
      await this.printDAGRec(link[1], depth + 1n, getter, set)
    }
  }

  public async dotDAG (w: any): Promise<void> {
    const heads = await this.heads.list()

    // w.write('digraph CRDTDAG {\n')
    // eslint-disable-next-line no-console
    console.log('digraph CRDTDAG {\n')

    const ng = new CRDTNodeGetter(this.dagService.blockstore, this.prefixedLogger.forComponent('ipld'))

    const set = new CidSafeSet()

    // w.write('subgraph heads {\n')
    // eslint-disable-next-line no-console
    console.log('subgraph heads {\n')
    for (const h of heads.heads) {
      // w.write(`${h}\n`)
      // eslint-disable-next-line no-console
      console.log(`${h}\n`)
    }
    // w.write('}\n')
    // eslint-disable-next-line no-console
    console.log('}\n')

    for (const h of heads.heads) {
      await this.dotDAGRec(w, h, 0, ng, set)
    }
    // w.write('}\n')
    // eslint-disable-next-line no-console
    console.log('}\n')
  }

  async dotDAGRec (
    w: any,
    from: CID,
    depth: number,
    ng: CRDTNodeGetter,
    set: CidSafeSet
  ): Promise<void> {
    const cidLong = from.toString()
    const cidShort = cidLong.slice(-4)

    await set.add(from)

    const { node, delta } = await ng.getDelta(from)

    // w.write(`${cidLong} [label="${delta.priority} | ${cidShort}: +${delta.elements.length} -${delta.tombstones.length}"]\n`)
    // eslint-disable-next-line no-console
    console.log(`${cidLong} [label="${delta.priority} | ${cidShort}: +${delta.elements.length} -${delta.tombstones.length}"]\n`)
    // w.write(`${cidLong} -> {`)
    // eslint-disable-next-line no-console
    console.log(`${cidLong} -> {`)
    for (const l of node.links()) {
      // w.write(`${l[1]} `)
      // eslint-disable-next-line no-console
      console.log(`${l[1]} `)
    }
    // w.write('}\n')
    // eslint-disable-next-line no-console
    console.log('}\n')

    // w.write(`subgraph sg_${cidLong} {\n`)
    // eslint-disable-next-line no-console
    console.log(`subgraph sg_${cidLong} {\n`)
    for (const l of node.links()) {
      // w.write(`${l[1]}\n`)
      // eslint-disable-next-line no-console
      console.log(`${l[1]}\n`)
    }
    // w.write('}\n')
    // eslint-disable-next-line no-console
    console.log('}\n')

    for (const l of node.links()) {
      await this.dotDAGRec(w, l[1], depth + 1, ng, set)
    }
  }

  public async internalStats (): Promise<Stats> {
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
    let size: number = 0

    await this.curDeltaMutex.runExclusive(() => {
      if (this.curDelta !== null) {
        this.curDelta = this.deltaMerge(this.curDelta, newDelta)
        size = dpb.delta.Delta.encode(this.curDelta).length
      }
    })

    return size
  }

  private deltaMerge (
    d1: dpb.delta.Delta,
    d2: dpb.delta.Delta
  ): dpb.delta.Delta {
    return {
      elements: [
        ...(Array.isArray(d1.elements) ? d1.elements : []),
        ...(Array.isArray(d2.elements) ? d2.elements : [])
      ],
      tombstones: [
        ...(Array.isArray(d1.tombstones) ? d1.tombstones : []),
        ...(Array.isArray(d2.tombstones) ? d2.tombstones : [])
      ],
      priority: d2.priority > d1.priority ? d2.priority : d1.priority
    }
  }

  private async updateDeltaWithRemove (
    key: string,
    newDelta: dpb.delta.Delta
  ): Promise<number> {
    let size: number = 0

    await this.curDeltaMutex.runExclusive(() => {
      if (this.curDelta !== null) {
        const elements = this.curDelta.elements.filter((e) => e.key !== key)
        this.curDelta.elements = elements
      } else {
        this.curDelta = newDelta
      }

      this.curDelta = this.deltaMerge(this.curDelta, newDelta)
      size = dpb.delta.Delta.encode(this.curDelta).length
    })

    return size
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

  public async keyHistory (key: Key): Promise<string[]> {
    this.logger('getting key history for', key.toString())
    const history: string[] = []
    const heads = await this.heads.list()
    const getter = new CRDTNodeGetter(
      this.dagService.blockstore,
      this.prefixedLogger.forComponent('ipld')
    )
    const visited = new CidSafeSet()

    for (const head of heads.heads) {
      await this.keyHistoryRec(key, head, getter, visited, history)
    }

    return history
  }

  private async keyHistoryRec (
    key: Key,
    from: CID,
    getter: CRDTNodeGetter,
    visited: CidSafeSet,
    history: Array<string | null>
  ): Promise<void> {
    if (await visited.has(from)) {
      return
    }

    await visited.add(from)

    const { node, delta } = await getter.getDelta(from)

    // Check if the delta elements contains the key
    const element = delta.elements.find(e => e.key === key.toString())
    if (element !== undefined) {
      if (element.value === null) {
        history.push(null)
      } else {
        history.push(new TextDecoder().decode(element.value))
      }
    }

    const tombstone = delta.tombstones.find(e => e.key === key.toString())
    if (tombstone !== undefined) {
      history.push(null)
    }

    // Recursively visit linked nodes (parents)
    for (const link of node.links()) {
      await this.keyHistoryRec(key, link[1], getter, visited, history)
    }
  }
}
