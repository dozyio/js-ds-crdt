import { prefixLogger } from '@libp2p/logger'
import { Mutex } from 'async-mutex'
import {
  Key,
  type Datastore as DSDatastore,
  type Query,
  type Pair
} from 'interface-datastore'
// import drain from 'it-drain'
import drain from 'it-drain'
import { CID } from 'multiformats/cid'
// import debug from 'weald'
import PQueue from 'p-queue'
import { CidSafeSet } from './cid-safe-set'
import { DatastoreBatch } from './datastore-batch'
import { Heads } from './heads'
import { CRDTNodeGetter } from './ipld'
import * as bpb from './pb/bcast'
import * as dpb from './pb/delta'
import { CRDTSet } from './set'
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
    rebroadcastInterval: 5 * 1000, // 5 seconds in milliseconds
    putHook: undefined,
    deleteHook: undefined,
    numWorkers: 5,
    dagSyncerTimeout: 5 * 60 * 1000, // 5 minutes in milliseconds
    maxBatchDeltaSize: 1024 * 1024, // 1MB
    repairInterval: 60 * 60 * 1000, // 1 hour in milliseconds
    logInterval: 60 * 1000, // 1 minute in milliseconds
    multiHeadProcessing: false
  }
}

class DagJob {
  constructor (
    public session: Mutex,
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
  private readonly queuedChildren: CidSafeSet
  private readonly nodeGetter: CRDTNodeGetter
  private readonly dagJobQueue: PQueue
  private scheduledRebroadcast!: NodeJS.Timeout | number
  private scheduledLogStats!: NodeJS.Timeout | number
  private scheduledRepair!: NodeJS.Timeout | number

  constructor (
    store: DSDatastore,
    namespace: Key,
    dagSyncer: HeliaLibp2p<Libp2p<CRDTLibp2pServices>>,
    broadcaster: Broadcaster,
    options?: Partial<Options>
  ) {
    let opts
    if (options !== undefined) {
      opts = { ...defaultOptions(), ...options }
    } else {
      opts = defaultOptions()
    }

    if (opts.rebroadcastInterval < 1) {
      throw new Error('rebroadcastInterval must be greater than 0')
    }

    if (opts.numWorkers < 1) {
      throw new Error('numWorkers must be greater than 0')
    }

    if (opts.dagSyncerTimeout < 1) {
      throw new Error('dagSyncerTimeout must be greater than 0')
    }

    if (opts.maxBatchDeltaSize < 1) {
      throw new Error('maxBatchDeltaSize must be greater than 0')
    }

    if (opts.repairInterval < 1) {
      throw new Error('repairInterval must be greater than 0')
    }

    if (opts.logInterval < 1) {
      throw new Error('logInterval must be greater than 0')
    }

    this.ctx = new AbortController()
    this.options = opts
    this.prefixedLogger = prefixLogger(this.options.loggerPrefix)
    this.logger = this.prefixedLogger.forComponent('crdt')
    this.store = store
    this.namespace = namespace
    this.dagService = dagSyncer
    this.broadcaster = broadcaster
    this.seenHeads = new Map<CID, boolean>()
    this.queuedChildren = new CidSafeSet()
    this.nodeGetter = new CRDTNodeGetter(
      this.dagService.blockstore,
      this.prefixedLogger.forComponent('ipld')
    )
    this.dagJobQueue = new PQueue({ concurrency: this.options.numWorkers })

    // debug.enable(`${this.options.loggerPrefix}*`) // 'crdt*,*crdt:trace')
    // debug.enable('*,*trace')

    // Initialize the CRDTSet and heads
    this.set = new CRDTSet(
      store,
      namespace.child(new Key(setNs)),
      this.nodeGetter,
      this.prefixedLogger.forComponent('set'),
      this.options.putHook,
      this.options.deleteHook
    )

    this.heads = new Heads(
      store,
      namespace.child(new Key(headsNs)),
      this.prefixedLogger.forComponent('heads')
    )

    // console.log('TODO migrations')

    this.handleNext()

    void this.scheduleRebroadcast()
    void this.scheduleRepair()
    void this.scheduleLogStats()
  }

  private async scheduleRebroadcast (): Promise<void> {
    try {
      this.logger('running rebroadcast')
      await this.rebroadcast()
    } catch (err) {
      // eslint-disable-next-line no-console
      this.logger.error('Error in rebroadcast:', err)
    } finally {
      this.scheduledRebroadcast = setTimeout(() => {
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
      this.scheduledRepair = setTimeout(() => {
        void this.scheduleRepair()
      }, this.options.repairInterval)
    }
  }

  private async scheduleLogStats (): Promise<void> {
    try {
      await this.logStats()
    } catch (err) {
      this.logger.error('Error in logStats:', err)
    } finally {
      this.scheduledLogStats = setTimeout(() => {
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
          for (const head of bCastHeads) {
            const prio = await this.nodeGetter.getPriority(head)
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

  // Enqueue a DAG job
  private async enqueueJob (job: DagJob): Promise<void> {
    if (this.ctx.signal.aborted) {
      return
    }

    await this.dagJobQueue.add(async () => {
      try {
        await this.processDagJob(job)
      } catch (err) {
        this.logger.error('Error in processDagJob:', err)
      }
    })
  }

  // Process a DAG job
  private async processDagJob (job: DagJob): Promise<void> {
    this.logger('Processing DAG job')
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

  public async repair (): Promise<void> {
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
      const { node } = await this.nodeGetter.getDelta(cur)

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
    await this.store.put(this.dirtyKey(), new Uint8Array([1]))
  }

  public async isDirty (): Promise<boolean> {
    return this.store.has(this.dirtyKey())
  }

  public async markClean (): Promise<void> {
    this.logger('Marking datastore as clean')
    await this.store.delete(this.dirtyKey())
  }

  public async logStats (): Promise<void> {
    const heads = await this.heads.list()
    // console.log(
    //   `Number of heads: ${heads.heads.length}. Max height: ${heads.maxHeight}. Queued DAG jobs: ${this.dagJobQueue.size}. Running DAG jobs: ${this.dagJobQueue.pending}. Dirty: ${await this.isDirty()}`
    // )
    this.logger(
      `Number of heads: ${heads.heads.length}. Max height: ${heads.maxHeight}. Queued DAG jobs: ${this.dagJobQueue.size}. Running DAG jobs: ${this.dagJobQueue.pending}. Dirty: ${await this.isDirty()}`
    )
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
    this.logger('handling block', c.toString())
    try {
      const isProcessed = await this.isProcessed(c)
      if (isProcessed) {
        this.logger(`${c} is known. Skip walking tree`)
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
    const session = new Mutex()

    await this.sendNewJobs(session, head, 0n, [c])
  }

  private async sendNewJobs (
    session: Mutex,
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
      const prio = await this.nodeGetter.getPriority(children[0])
      rootPrio = prio
    }

    const goodDeltas = new Set<string>()

    for await (const deltaOpt of this.nodeGetter.getDeltas(children)) {
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
        root,
        rootPrio,
        deltaOpt.delta,
        deltaOpt.node,
        []
      )

      // await this.enqueueSendJob(job)
      void this.enqueueJob(job)
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
    // eslint-disable-next-line no-console
    this.logger('checking isProcessed', key.toString())
    const isProcessed = await this.store.has(key)
    this.logger('isProcessed', key.toString(), isProcessed)
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
    return this.set.element(key.toString())
  }

  public async has (key: Key): Promise<boolean> {
    this.logger('has key', key.toString())
    return this.set.inSet(key.toString())
  }

  public async getSize (key: Key): Promise<number> {
    this.logger('get size ', key.toString())
    try {
      const result = await this.set.element(key.toString())
      if (result === null) {
        return 0
      }
      return result.length
    } catch (error: unknown) {
      return -1
    }
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
    this.logger(`putting block with height ${height.toString()}`)
    if (delta != null) {
      delta.priority = height
    }

    const node = await CRDTNodeGetter.makeNode(delta, heads)

    // Add the block to the blockstore
    await this.dagService.blockstore.put(node.cid, node.bytes, {
      onProgress: (evt) => {
        this.logger('blockstore put', evt.type, evt.detail)
      }
    })

    // Pin the block so it doesn't get garbage collected
    try {
      await drain(this.dagService.pins.add(node.cid, {
        onProgress: (evt) => {
          this.logger('dagService pin', evt.type, evt.detail)
        }
      }))
    } catch (err) {
      this.logger.error(`Error pinning block ${node.cid}: ${err}`)
    }

    // Inform the network (DHT) that we can provide the CID
    const signal = AbortSignal.timeout(5000)

    void (async () => {
      try {
        await this.dagService.routing.provide(node.cid, {
          onProgress: (evt) => {
            this.logger('dagService routing provide', evt.type, evt.detail)
          },
          signal
        })
      } catch (err: any) {
        if (err.name === 'AbortError') {
          this.logger(`Provide operation aborted for block ${node.cid}`)
        } else {
          this.logger.error(`Error providing block ${node.cid}: ${err}`)
        }
      }
    })()

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

  public async batch (): Promise<DatastoreBatch> {
    return new DatastoreBatch(this)
  }

  public async sync (prefix: Key): Promise<void> {
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
    if (this.scheduledRebroadcast !== undefined) {
      globalThis.clearTimeout(this.scheduledRebroadcast)
    }
    if (this.scheduledRepair !== undefined) {
      globalThis.clearTimeout(this.scheduledRepair)
    }
    if (this.scheduledLogStats !== undefined) {
      globalThis.clearTimeout(this.scheduledLogStats)
    }

    if (this.dagJobQueue.size > 0) {
      // we left something in the queue
      await this.markDirty()
    }

    if (await this.isDirty()) {
      this.logger.error('Datastore closed while marked as dirty')
    }
  }

  public async printDAG (): Promise<void> {
    const heads = await this.heads.list()
    const set = new Set<string>()

    for (const head of heads.heads) {
      await this.printDAGRec(head, 0n, set)
    }
  }

  private async printDAGRec (
    from: CID,
    depth: bigint,
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

    const { node, delta } = await this.nodeGetter.getDelta(from)
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

    line += '}'

    const processed = await this.isProcessed(node.cid)
    if (!processed) {
      line += ' Unprocessed!'
    }

    line += ':'

    // eslint-disable-next-line no-console
    console.log(line)

    for (const link of node.links()) {
      await this.printDAGRec(link[1], depth + 1n, set)
    }
  }

  public async dotDAG (writer: (data: string) => void): Promise<void> {
    const heads = await this.heads.list()
    const set = new CidSafeSet()

    writer('digraph CRDTDAG {\n')

    writer('subgraph heads {\n')
    for (const h of heads.heads) {
      writer(`${h}\n`)
    }
    writer('}\n')

    for (const h of heads.heads) {
      await this.dotDAGRec(writer, h, 0, set)
    }

    writer('}\n')
  }

  async dotDAGRec (
    writer: (data: string) => void,
    from: CID,
    depth: number,
    set: CidSafeSet
  ): Promise<void> {
    const cidLong = from.toString()
    const cidShort = cidLong.slice(-4)

    await set.add(from)

    const { node, delta } = await this.nodeGetter.getDelta(from)

    writer(`${cidLong} [label="${delta.priority} | ${cidShort}: +${delta.elements.length} -${delta.tombstones.length}"]\n`)
    writer(`${cidLong} -> {`)
    for (const l of node.links()) {
      writer(`${l[1]} `)
    }
    writer('}\n')

    writer(`subgraph sg_${cidLong} {\n`)
    for (const l of node.links()) {
      writer(`${l[1]}\n`)
    }
    writer('}\n')

    for (const l of node.links()) {
      await this.dotDAGRec(writer, l[1], depth + 1, set)
    }
  }

  public async internalStats (): Promise<Stats> {
    const heads = await this.heads.list()
    return {
      heads: heads.heads,
      maxHeight: heads.maxHeight,
      queuedJobs: this.dagJobQueue.size
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

  public async keyHistory (key: Key): Promise<Array<Uint8Array | null>> {
    this.logger('getting key history for', key.toString())
    const history: Array<Uint8Array | null> = []
    const heads = await this.heads.list()
    const visited = new CidSafeSet()

    for (const head of heads.heads) {
      await this.keyHistoryRec(key, head, visited, history)
    }

    return history
  }

  private async keyHistoryRec (
    key: Key,
    from: CID,
    visited: CidSafeSet,
    history: Array<Uint8Array | null>
  ): Promise<void> {
    if (await visited.has(from)) {
      return
    }

    await visited.add(from)

    const { node, delta } = await this.nodeGetter.getDelta(from)

    // Check if the delta elements contains the key
    const element = delta.elements.find(e => e.key === key.toString())
    if (element !== undefined) {
      if (element.value === null) {
        history.push(null)
      } else {
        history.push(element.value)
      }
    }

    const tombstone = delta.tombstones.find(e => e.key === key.toString())
    if (tombstone !== undefined) {
      history.push(null)
    }

    // Recursively visit linked nodes (parents)
    for (const link of node.links()) {
      await this.keyHistoryRec(key, link[1], visited, history)
    }
  }
}
