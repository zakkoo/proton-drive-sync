/**
 * Sync harness for tests: a real temp sync root, the fake remote, a real state
 * store, and the full cycle (scan -> list -> reconcile -> execute) wired the
 * way the engine wires it. Supports "crash and reopen" for fault injection.
 */
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { AuditLog } from '../audit/logger.js';
import { SecretRegistry } from '../audit/redact.js';
import { DEFAULTS } from '../config/schema.js';
import { baselineRowToItem, listRemoteTree, localViewFromSnapshot, needsDigest, remoteViewFromNodes } from '../engine/views.js';
import { Executor } from '../execute/executor.js';
import { recoverJournal, type RecoveryReport } from '../execute/recovery.js';
import type { ExecutionSummary, ExecutorConfig, ExecutorContext, ExecutorEvent, ExecutorHooks } from '../execute/types.js';
import { DigestCache } from '../local/digest.js';
import { createIgnoreMatcher } from '../local/ignore.js';
import { scanLocalTree } from '../local/snapshot.js';
import { reconcile } from '../reconcile/reconcile.js';
import type { BaselineItem, Operation, Plan, ReconcileInput } from '../reconcile/types.js';
import { createLogger, silentSink } from '../remote/proton/logger.js';
import { QuarantineService } from '../safety/quarantine.js';
import { RecycleBin } from '../safety/recycle.js';
import { BaselineRepo } from '../state/baseline.ts';
import { JournalRepo } from '../state/journal.ts';
import { QuarantineRepo } from '../state/misc.ts';
import { StateStore } from '../state/store.ts';
import type { ContentWorld } from './assertNoUserContentLost.js';
import { FakeRemote } from './fakeRemote.js';

export interface HarnessOptions {
  config?: Partial<ExecutorConfig>;
  hooks?: ExecutorHooks;
  onEvent?: (e: ExecutorEvent) => void;
  sleep?: (ms: number) => Promise<void>;
}

export class SyncHarness {
  readonly base: string;
  readonly root: string;
  readonly fake: FakeRemote;
  readonly remoteRootUid: string;
  readonly audit: AuditLog;
  readonly registry = new SecretRegistry();
  store!: StateStore;
  baseline!: BaselineRepo;
  journal!: JournalRepo;
  quarantine!: QuarantineService;
  recycle!: RecycleBin;
  digests!: DigestCache;
  ctx!: ExecutorContext;
  private readonly options: HarnessOptions;
  private now = 1_700_000_000_000;

  private constructor(options: HarnessOptions) {
    this.options = options;
    this.base = mkdtempSync(path.join(os.tmpdir(), 'pds-harness-'));
    this.root = path.join(this.base, 'root');
    mkdirSync(this.root);
    this.fake = new FakeRemote();
    this.remoteRootUid = this.fake.seedFolder(this.fake.rootUid, 'Sync').uid;
    this.audit = new AuditLog({ dir: path.join(this.base, 'audit'), registry: this.registry });
  }

  static create(options: HarnessOptions = {}): SyncHarness {
    const h = new SyncHarness(options);
    h.open();
    return h;
  }

  private open(): void {
    this.store = StateStore.open(path.join(this.base, 'state.db'), { now: () => ++this.now });
    this.baseline = new BaselineRepo(this.store);
    this.journal = new JournalRepo(this.store);
    this.quarantine = new QuarantineService(new QuarantineRepo(this.store), this.audit);
    this.recycle = new RecycleBin(this.root, 30, this.audit, () => ++this.now);
    this.digests = new DigestCache(this.root);
    this.ctx = {
      root: this.root,
      remoteRootUid: this.remoteRootUid,
      remote: this.fake,
      store: this.store,
      baseline: this.baseline,
      journal: this.journal,
      quarantine: this.quarantine,
      recycle: this.recycle,
      audit: this.audit,
      logger: createLogger('exec', silentSink),
      digests: this.digests,
      config: { concurrency: 1, maxRetries: 2, dryRun: false, ...this.options.config },
      ...(this.options.onEvent !== undefined ? { onEvent: this.options.onEvent } : {}),
      ...(this.options.hooks !== undefined ? { hooks: this.options.hooks } : {}),
      sleep: this.options.sleep ?? (() => Promise.resolve()),
      now: () => ++this.now,
    };
  }

  /** Simulate a process restart: close the store and reopen everything on the same files. */
  reopen(options: Partial<HarnessOptions> = {}): void {
    this.store.close();
    Object.assign(this.options, options);
    if (options.hooks === undefined) delete this.options.hooks;
    this.open();
  }

  dispose(): void {
    this.store.close();
    rmSync(this.base, { recursive: true, force: true });
  }

  // ---- local helpers -----------------------------------------------------

  write(relPath: string, content: string): void {
    const abs = path.join(this.root, relPath);
    mkdirSync(path.dirname(abs), { recursive: true });
    writeFileSync(abs, content);
    // Keep mtimes strictly increasing and stable across fast writes.
    const t = new Date(++this.now);
    utimesSync(abs, t, t);
  }

  mkdir(relPath: string): void {
    mkdirSync(path.join(this.root, relPath), { recursive: true });
  }

  localFiles(): Map<string, string> {
    const out = new Map<string, string>();
    const walk = (dir: string, rel: string): void => {
      for (const e of readdirSync(dir, { withFileTypes: true })) {
        if (rel === '' && e.name === '.proton-sync') continue;
        const r = rel === '' ? e.name : `${rel}/${e.name}`;
        if (e.isDirectory()) walk(path.join(dir, e.name), r);
        else out.set(r, readFileSync(path.join(dir, e.name), 'utf8'));
      }
    };
    walk(this.root, '');
    return out;
  }

  recycledContents(): string[] {
    return this.recycle.list().filter((i) => i.kind === 'file').map((i) => readFileSync(i.absolutePath, 'utf8'));
  }

  /** Every place user content can currently rest, for {@link assertNoUserContentLost}. */
  world(): ContentWorld {
    return {
      localFiles: () => this.localFiles(),
      remoteFiles: () => this.remoteFiles(),
      recycledContents: () => this.recycledContents(),
      remoteTrashedContents: () => this.remoteTrashedContents(),
      supersededContents: () => this.fake.supersededContents(),
    };
  }

  // ---- remote helpers ----------------------------------------------------

  remoteFiles(): Map<string, string> {
    const out = new Map<string, string>();
    for (const n of this.fake.allNodes()) {
      if (n.type !== 'file' || n.isTrashed) continue;
      const p = this.fake.pathOf(n.uid);
      if (!p.startsWith('/Sync/')) continue;
      out.set(p.slice('/Sync/'.length), this.fake.contentOf(n.uid)?.toString() ?? '');
    }
    return out;
  }

  remoteTrashedContents(): string[] {
    return this.fake.trashedUids().flatMap((uid) => {
      const c = this.fake.contentOf(uid);
      return c === undefined ? [] : [c.toString()];
    });
  }

  remotePathToUid(relPath: string): string | undefined {
    return this.fake.allNodes().find((n) => !n.isTrashed && this.fake.pathOf(n.uid) === `/Sync/${relPath}`)?.uid;
  }

  // ---- cycle -------------------------------------------------------------

  /**
   * `digest: 'engine'` (the default) hashes only what the engine would (see `needsDigest`), so a
   * baseline row that wrongly vouches for changed content shows up as missing work, as it would in
   * production. `digest: 'all'` hashes every file, which can repair such a row and hide the defect.
   */
  async buildInput(options: { digest?: 'all' | 'engine' } = {}): Promise<ReconcileInput> {
    const baseline = new Map<string, BaselineItem>();
    for (const row of this.baseline.all()) baseline.set(row.relPath, baselineRowToItem(row));
    const ignored = createIgnoreMatcher(DEFAULTS.ignore);
    const snapshot = await scanLocalTree(this.root, { ignore: ignored });
    const rule = options.digest === 'all' ? () => true : (relPath: string) => needsDigest(this.baseline.byPath(relPath), snapshot.entries.get(relPath));
    const local = await localViewFromSnapshot(snapshot, this.digests, rule);
    const nodes = await listRemoteTree(this.fake, this.remoteRootUid);
    const remote = remoteViewFromNodes(nodes, this.remoteRootUid);
    const sets = this.quarantine.sets();
    return { baseline, local, remote, quarantinedPaths: sets.paths, quarantinedUids: sets.uids, ignored };
  }

  async plan(options: { digest?: 'all' | 'engine' } = {}): Promise<Plan> {
    return reconcile(await this.buildInput(options));
  }

  /** Execute operations that depend on each other, stopping at the first that does not complete. */
  executeDependent(operations: Operation[]): Promise<ExecutionSummary> {
    return new Executor(this.ctx).execute({ operations, dependent: true });
  }

  async execute(plan: Plan, options: { includeWithheld?: boolean } = {}): Promise<{ executor: Executor; summary: ExecutionSummary }> {
    const executor = new Executor(this.ctx);
    const operations = options.includeWithheld === true ? [...plan.operations, ...plan.withheld.map((w) => w.operation)] : plan.operations;
    const summary = await executor.execute({ operations });
    return { executor, summary };
  }

  async cycle(): Promise<{ plan: Plan; summary: ExecutionSummary }> {
    const plan = await this.plan();
    const { summary } = await this.execute(plan);
    return { plan, summary };
  }

  recover(): Promise<RecoveryReport> {
    return recoverJournal(this.ctx);
  }

  /** Cycle until the plan is empty (or give up). */
  async settle(maxCycles = 6): Promise<Plan> {
    let plan = await this.plan();
    for (let i = 0; i < maxCycles && (plan.operations.length > 0 || plan.conflicts.length > 0); i++) {
      await this.execute(plan);
      plan = await this.plan();
    }
    return plan;
  }

  /**
   * Every baseline row must describe an existing local item and a live remote node
   * with the recorded revision, and a file's row must vouch only for content both sides
   * really hold: its local digest is the file's actual digest, and the remote node has the
   * same bytes. Only meaningful once the world has settled: while changes are pending,
   * rows legitimately lag behind the side that changed.
   */
  assertBaselineConsistent(ignore: ReadonlySet<string> = new Set()): void {
    for (const row of this.baseline.all()) {
      if (ignore.has(row.relPath)) continue;
      const st = statSync(path.join(this.root, row.relPath));
      if (st.ino !== row.localIno) throw new Error(`baseline ${row.relPath}: local inode ${String(row.localIno)} but file has ${String(st.ino)}`);
      if (row.kind === 'file' && (st.size !== row.localSize || st.mtimeMs !== row.localMtimeMs)) throw new Error(`baseline ${row.relPath}: local size/mtime differ from the file`);
      const rec = this.fake.record(row.nodeUid);
      if (rec === undefined || rec.trashed) throw new Error(`baseline ${row.relPath}: remote node ${row.nodeUid} missing or trashed`);
      if (row.kind === 'file' && rec.revisionUid !== row.revisionUid) throw new Error(`baseline ${row.relPath}: remote revision ${String(rec.revisionUid)} differs from ${String(row.revisionUid)}`);
      if (row.kind === 'file') {
        const local = readFileSync(path.join(this.root, row.relPath));
        const localSha1 = createHash('sha1').update(local).digest('hex');
        if (row.localSha1 !== null && row.localSha1 !== localSha1) throw new Error(`baseline ${row.relPath}: recorded local digest ${row.localSha1} but the file's content is ${localSha1}`);
        const remote = this.fake.contentOf(row.nodeUid);
        if (remote?.equals(local) !== true) throw new Error(`baseline ${row.relPath}: the two sides hold different content`);
      }
    }
  }
}
