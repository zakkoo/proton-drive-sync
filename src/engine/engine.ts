/**
 * SyncEngine: the cycle scheduler and state machine.
 *
 * A cycle is: gather local snapshot and remote view -> compute digests for
 * candidates -> reconcile -> preflight -> handle conflicts -> brake -> execute.
 * Preflight (root identity, store, disk, remote root) passes before anything
 * changes files or state: journal recovery, conflict handling, a confirmed held
 * plan and a conflict resolution included.
 * Cycles are single-flight; triggers (local changes, remote events, timers,
 * "sync now") set a dirty flag and the loop re-runs once the current cycle
 * ends. Only user actions change pause state.
 */
import { EventEmitter } from 'node:events';

import type { AuditLog } from '../audit/logger.js';
import type { SyncConfig } from '../config/schema.js';
import type { ConflictHandler, Resolution } from '../conflict/handler.js';
import { Executor } from '../execute/executor.js';
import { recoverJournal } from '../execute/recovery.js';
import type { ExecutionSummary, ExecutorContext, ExecutorEvent } from '../execute/types.js';
import type { DigestProvider } from '../local/digest.js';
import { createIgnoreMatcher, type IgnoreMatcher } from '../local/ignore.js';
import type { LocalSnapshot } from '../local/snapshot.js';
import type { LocalWatcher, LocalWatcherEvent } from '../local/watcher.js';
import { reconcile } from '../reconcile/reconcile.js';
import type { BaselineItem, LocalView, Operation, Plan, RemoteView } from '../reconcile/types.js';
import type { RemoteChangeFeed } from '../remote/events.js';
import { RemoteError } from '../remote/interface.js';
import type { Logger } from '../remote/proton/logger.js';
import type { SessionState } from '../remote/proton/sessionState.js';
import { type HeldPlan, heldAffected, type PlanGate } from '../safety/brake.js';
import { blockUnsyncableTargets } from '../safety/pathGuard.js';
import type { PreflightResult } from '../safety/preflight.js';
import type { QuarantineService } from '../safety/quarantine.js';
import type { BaselineRepo } from '../state/baseline.ts';
import type { ConflictRepo, ScanRepo } from '../state/misc.ts';
import type { RemoteMirror } from './remoteMirror.js';
import { canTransition, IllegalStateTransitionError, initialStatus, summarize, type EngineState, type EngineStatus, type TransferStatus } from './status.js';
import { baselineRowToItem, localViewFromSnapshot, needsDigest } from './views.js';

export interface EngineDeps {
  config: SyncConfig;
  executorContext: Omit<ExecutorContext, 'onEvent' | 'config'>;
  baseline: BaselineRepo;
  conflictRepo: ConflictRepo;
  scans: ScanRepo;
  quarantine: QuarantineService;
  conflicts: ConflictHandler;
  gate: PlanGate;
  mirror: RemoteMirror;
  watcher: LocalWatcher;
  feed: RemoteChangeFeed | null;
  digests: DigestProvider;
  audit: AuditLog;
  logger: Logger;
  preflight: (plannedDownloadBytes: number) => Promise<PreflightResult>;
  session?: SessionState;
  now?: () => number;
  /** Debounce for coalescing triggers into one cycle. */
  triggerDebounceMs?: number;
}

export interface CycleResult {
  plan: Plan;
  summary: ExecutionSummary | null;
  held: boolean;
  skipped: string | null;
}

export class SyncEngine extends EventEmitter {
  private status: EngineStatus;
  private readonly now: () => number;
  private cycleRunning: Promise<CycleResult | null> | null = null;
  private dirty = false;
  /** The end of the last cycle, conflict resolution or confirmed plan to start; see `exclusive`. */
  private exclusiveTail: Promise<void> = Promise.resolve();
  private triggerTimer: NodeJS.Timeout | null = null;
  private listingTimer: NodeJS.Timeout | null = null;
  private executor: Executor | null = null;
  private userPaused: boolean;
  private stopped = false;
  private lastSnapshot: LocalSnapshot | null = null;
  private localRootAvailable = true;
  /**
   * Set the moment a remote call is rejected for auth, before the session finishes clearing the
   * stored credentials (which takes a secret-store write); cleared by the next login. No cycle
   * runs while it is set.
   */
  private sessionRejected = false;
  /** Startup has begun (journal recovery, watcher, remote listing); a later login only re-syncs. */
  private startedUp = false;
  /** Journal recovery waits for a passing preflight, so it may run in a later cycle than startup. */
  private journalRecovered = false;
  private readonly transfers = new Map<string, TransferStatus>();
  private lastRemoteFailure: string | null = null;
  private readonly ignoreMatcher: IgnoreMatcher;
  /** While file operations run, status publishes reuse this instead of walking the trees again. */
  private libraryFrozen = false;
  private libraryCache: { counts: EngineStatus['counts']; protonDocumentPaths: string[]; protonDocumentModifiedAt: Record<string, number> } | null = null;

  constructor(private readonly deps: EngineDeps) {
    super();
    this.now = deps.now ?? Date.now;
    this.userPaused = deps.config.startPaused;
    this.status = initialStatus(deps.config.dryRun, this.now());
    this.ignoreMatcher = createIgnoreMatcher(deps.config.ignore);
    // One listener for the engine's lifetime: a session lost at any time stops sync, and a new
    // login resumes it, whether the engine started logged in or not.
    deps.session?.onChange((s) => {
      this.onSessionStatus(s);
    });
  }

  private onSessionStatus(s: 'logged_in' | 'needs_login'): void {
    if (s === 'needs_login') {
      this.sessionRejected = true;
      // No further operation of a run in progress: it would use a session that is gone.
      this.executor?.pause();
      if (this.status.state !== 'needs_login' && this.status.state !== 'stopped') this.setStateSafely('needs_login', 'session rejected');
      return;
    }
    this.sessionRejected = false;
    if (this.stopped || this.status.state !== 'needs_login') return;
    if (!this.startedUp) void this.start(); // which also honours a pause
    // Logged in, but the user's pause still holds: show it again and sync nothing yet.
    else if (this.userPaused) this.setStateSafely('paused', 'paused by user');
    else this.trigger('logged in');
  }

  /**
   * Pick up a sign-in or sign-out made by another process (the CLI's login or logout, which
   * the panel's sign-in runs): credentials live in each process's memory, so this one re-reads
   * the shared store.
   */
  async reloadSession(): Promise<void> {
    if (this.deps.session === undefined) return;
    const status = await this.deps.session.reload();
    // Unchanged session status but an engine still waiting on a login (a rejection the session
    // never recorded): the reload is the user's answer to it.
    if (status === 'logged_in' && this.status.state === 'needs_login') this.onSessionStatus('logged_in');
  }

  /**
   * Tag baseline paths that vanished from the scan only because they are now
   * ignored or unsyncable, so the reconciler does not read their absence as a
   * deletion.
   */
  private withHiddenPaths(view: LocalView, snapshot: LocalSnapshot, baseline: ReadonlyMap<string, BaselineItem>): LocalView {
    const hidden = new Set<string>();
    for (const relPath of baseline.keys()) {
      if (view.items.has(relPath)) continue;
      if (this.ignoreMatcher(relPath) || snapshot.unsyncable.some((u) => u.relPath === relPath)) hidden.add(relPath);
    }
    return hidden.size > 0 ? { ...view, hidden } : view;
  }

  // ---- status ------------------------------------------------------------

  getStatus(): EngineStatus {
    // Recompute counts and attention live so a surface never shows a stale empty default while
    // the engine already has real values (e.g. counts right after a cycle, before the next publish).
    const counts = this.computeCounts();
    const live = {
      ...this.status,
      lastFullSyncAt: this.deps.mirror.lastFullListingAt,
      transfers: [...this.transfers.values()],
      attention: this.computeAttention(),
      counts,
      protonDocumentPaths: this.libraryCache?.protonDocumentPaths ?? [],
      protonDocumentModifiedAt: this.libraryCache?.protonDocumentModifiedAt ?? {},
    };
    return { ...live, summaryLines: summarize(live) };
  }

  private computeAttention(): EngineStatus['attention'] {
    const held = this.deps.gate.current;
    return {
      conflicts: this.deps.conflictRepo.open().length,
      quarantined: this.deps.quarantine.open().length,
      heldPlan: held === null ? null : { id: held.id, reason: held.verdict.reason ?? 'confirmation required', affected: heldAffected(held).map(describeOp) },
    };
  }

  private refreshLibraryCache(): void {
    const kinds = this.deps.baseline.countByKind();
    const pairedFilePaths = new Set(this.deps.baseline.filePaths());
    const remote = this.deps.mirror.library();
    const localPaths: string[] = [];
    if (this.lastSnapshot !== null) {
      for (const entry of this.lastSnapshot.entries.values()) if (entry.kind === 'file') localPaths.push(entry.relPath);
    }
    let onlyLocal = 0;
    for (const rel of localPaths) if (!pairedFilePaths.has(rel)) onlyLocal++;
    let onlyRemote = 0;
    for (const rel of remote.syncableFilePaths) if (!pairedFilePaths.has(rel)) onlyRemote++;
    this.libraryCache = {
      protonDocumentPaths: remote.protonDocumentPaths,
      protonDocumentModifiedAt: remote.protonDocumentModifiedAt,
      counts: {
        baseline: kinds.file + kinds.dir,
        localFiles: localPaths.length,
        remoteFiles: remote.files,
        pairedFiles: kinds.file,
        pairedFolders: kinds.dir,
        protonDocuments: remote.protonDocumentPaths.length,
        onlyLocal,
        onlyRemote,
      },
    };
  }

  private computeCounts(): EngineStatus['counts'] {
    if (!this.libraryFrozen || this.libraryCache === null) this.refreshLibraryCache();
    return this.libraryCache?.counts ?? this.status.counts;
  }

  private setState(state: EngineState, reason: string | null = null): void {
    if (!canTransition(this.status.state, state)) throw new IllegalStateTransitionError(this.status.state, state);
    if (this.status.state !== state || this.status.reason !== reason) {
      this.deps.audit.append({ kind: 'engine', op: 'state', message: `${this.status.state} -> ${state}${reason !== null ? ` (${reason})` : ''}` });
    }
    this.status = { ...this.status, state, reason, since: this.now() };
    this.publish();
  }

  private publish(): void {
    this.refreshAttention();
    this.emit('status', this.getStatus());
  }

  private refreshAttention(): void {
    this.status = { ...this.status, attention: this.computeAttention(), counts: this.computeCounts() };
  }

  /** The resting state after a cycle: attention if anything needs the user, else idle. */
  private restingState(): EngineState {
    if (this.deps.gate.current !== null) return 'awaiting_confirmation';
    if (this.deps.conflictRepo.open().length > 0 || this.deps.quarantine.open().length > 0) return 'attention';
    return 'idle';
  }

  // ---- lifecycle ---------------------------------------------------------

  async start(): Promise<void> {
    if (this.deps.session !== undefined && this.deps.session.current !== 'logged_in') {
      // The session listener (constructor) starts the engine after a login.
      this.setState('needs_login', 'no stored session');
      return;
    }
    if (this.startedUp || (this.status.state !== 'starting' && this.status.state !== 'needs_login')) return;
    this.startedUp = true;
    this.setState('scanning', 'recovering journal');
    await this.recoverJournalIfSafe();

    this.deps.watcher.requestFullScan('startup');
    await this.deps.watcher.start();
    // The watcher's own onEvent (wired by the factory) forwards to onLocalEvent.
    this.lastSnapshot = this.deps.watcher.currentSnapshot;

    try {
      await this.deps.mirror.fullRefresh();
    } catch (error) {
      this.remoteFailure(error);
    }
    this.deps.feed?.start();
    const listingMs = this.deps.config.timing.remoteListingIntervalMinutes * 60_000;
    this.listingTimer = setInterval(() => {
      void this.deps.mirror
        .fullRefresh()
        .then(() => { this.trigger('periodic remote listing'); })
        .catch((error: unknown) => {
          this.remoteFailure(error);
        });
    }, listingMs);
    this.listingTimer.unref();

    if (this.userPaused) {
      this.setState('paused', 'started paused');
      return;
    }
    await this.runCycle('startup');
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.triggerTimer !== null) clearTimeout(this.triggerTimer);
    if (this.listingTimer !== null) clearInterval(this.listingTimer);
    this.executor?.pause();
    await this.deps.feed?.stop();
    await this.deps.watcher.stop();
    await this.cycleRunning;
    // A resolution or confirmed plan waiting its turn: let it finish (or refuse to start).
    await this.exclusiveTail;
    if (this.status.state !== 'stopped') this.setState('stopped');
  }

  private ctx(): ExecutorContext {
    return {
      ...this.deps.executorContext,
      config: { concurrency: this.deps.config.transfers.concurrency, maxRetries: this.deps.config.transfers.maxRetries, dryRun: this.deps.config.dryRun },
      onEvent: (e) => { this.onExecutorEvent(e); },
      // Our own changes on Proton reach the remote view at once, not when their events arrive.
      onRemoteChange: (change) => { this.deps.mirror.applyOwnChange(change); },
    };
  }

  // ---- inputs from watcher and feed --------------------------------------

  async onLocalEvent(event: LocalWatcherEvent): Promise<void> {
    switch (event.type) {
      case 'changes':
        this.lastSnapshot = event.snapshot;
        this.localRootAvailable = true;
        if (event.changes.length > 0 || event.source === 'scan') this.trigger('local changes');
        break;
      case 'root_unavailable':
        this.localRootAvailable = false;
        this.deps.audit.append({ kind: 'safety', message: `local sync root unavailable: ${event.error.message}`, outcome: 'failed' });
        this.setStateSafely('error', 'sync root unavailable');
        break;
      case 'root_restored':
        this.lastSnapshot = event.snapshot;
        this.localRootAvailable = true;
        this.trigger('sync root restored');
        break;
      case 'rescan':
        this.deps.audit.append({ kind: 'engine', message: `local rescan: ${event.reason}` });
        break;
    }
    await Promise.resolve();
  }

  async onRemoteEvent(event: Parameters<RemoteMirror['applyEvent']>[0]): Promise<void> {
    await this.deps.mirror.applyEvent(event);
    this.trigger('remote event');
  }

  async onRemoteRefreshRequired(reason: string): Promise<void> {
    this.deps.audit.append({ kind: 'engine', message: `remote full listing: ${reason}` });
    try {
      await this.deps.mirror.fullRefresh();
      this.trigger(`remote refresh (${reason})`);
    } catch (error) {
      this.remoteFailure(error);
    }
  }

  onFeedPollComplete(startedAt: number): void {
    this.deps.mirror.markPolled(startedAt);
  }

  onFeedStatus(status: 'live' | 'degraded' | 'stopped'): void {
    this.status = { ...this.status, degraded: status === 'degraded' };
    this.publish();
  }

  onThrottle(state: 'throttled' | 'unthrottled'): void {
    if (state === 'throttled' && (this.status.state === 'syncing' || this.status.state === 'idle' || this.status.state === 'scanning')) this.setStateSafely('throttled', 'server asked us to slow down');
    if (state === 'unthrottled' && this.status.state === 'throttled') this.setStateSafely(this.cycleRunning !== null ? 'syncing' : this.restingState());
  }

  /**
   * The remote rejected the session. Stop syncing now: clearing the stored session is
   * asynchronous, and until it finishes the session still reads as logged in.
   */
  private rejectSession(error: unknown): void {
    if (this.deps.session !== undefined) {
      this.sessionRejected = true;
      void this.deps.session.handleRemoteError(error).then((rejected) => {
        // The session did not count it as a rejection and stays logged in: carry on syncing.
        if (!rejected && this.deps.session?.current === 'logged_in') {
          this.sessionRejected = false;
          this.trigger('session kept');
        }
      });
    }
    this.setStateSafely('needs_login', 'session rejected');
  }

  private remoteFailure(error: unknown): void {
    const message = error instanceof Error ? error.message : String(error);
    this.lastRemoteFailure = message;
    if (error instanceof RemoteError && error.kind === 'auth') {
      this.rejectSession(error);
      return;
    }
    if (error instanceof RemoteError && (error.kind === 'connection' || error.kind === 'server' || error.kind === 'rate_limited')) {
      this.deps.mirror.markUnavailable();
      this.setStateSafely('offline', message);
      return;
    }
    this.deps.mirror.markUnavailable();
    this.setStateSafely('error', message);
  }

  private setStateSafely(state: EngineState, reason: string | null = null): void {
    if (this.status.state === 'stopped') return;
    if (canTransition(this.status.state, state)) this.setState(state, reason);
    else this.deps.logger.warn(`ignored state change ${this.status.state} -> ${state}`);
  }

  // ---- triggers and cycles -----------------------------------------------

  /** Coalesce triggers into one cycle after a short debounce. */
  trigger(reason: string): void {
    if (this.stopped || this.userPaused) return;
    this.dirty = true;
    if (this.triggerTimer !== null) clearTimeout(this.triggerTimer);
    this.triggerTimer = setTimeout(() => {
      this.triggerTimer = null;
      void this.runCycle(reason);
    }, this.deps.triggerDebounceMs ?? 500);
    this.triggerTimer.unref();
  }

  /** Run one cycle now (or join the running one). Loops while triggers arrived during the run. */
  runCycle(reason: string): Promise<CycleResult | null> {
    if (this.cycleRunning !== null) {
      this.dirty = true;
      return this.cycleRunning;
    }
    this.cycleRunning = (async () => {
      let result: CycleResult | null = null;
      do {
        this.dirty = false;
        try {
          result = await this.exclusive(() => this.cycleOnce(reason));
        } catch (error) {
          if (error instanceof EngineStoppingError) {
            result = null;
            break;
          }
          this.deps.logger.error('cycle failed', error);
          this.deps.audit.append({ kind: 'engine', message: `cycle failed: ${error instanceof Error ? error.message : String(error)}`, outcome: 'failed' });
          this.remoteFailure(error);
          result = null;
        }
      } while (this.shouldRerun());
      return result;
    })().finally(() => {
      this.cycleRunning = null;
    });
    return this.cycleRunning;
  }

  /**
   * Run `work` once the cycle, conflict resolution or confirmed plan before it has finished, and
   * make the next one wait for it. Each reads the baseline and both trees and changes them; one
   * that ran during another would plan from a half-done change (a cycle in the middle of a
   * resolution saw the restored file beside the trashed original and recorded a new conflict).
   */
  private async exclusive<T>(work: () => Promise<T>): Promise<T> {
    const previous = this.exclusiveTail;
    let finished!: () => void;
    this.exclusiveTail = new Promise<void>((resolve) => {
      finished = resolve;
    });
    try {
      await previous;
      // Once stopping has begun, nothing more may change files or state.
      if (this.stopped) throw new EngineStoppingError();
      return await work();
    } finally {
      finished();
    }
  }

  /** Read through a method: the flag is mutated by triggers while a cycle awaits. */
  private shouldRerun(): boolean {
    return this.dirty && !this.stopped && !this.userPaused;
  }

  private async cycleOnce(reason: string): Promise<CycleResult> {
    if (this.userPaused) return { plan: emptyPlan(), summary: null, held: false, skipped: 'paused' };
    if (this.sessionRejected || (this.deps.session !== undefined && this.deps.session.current !== 'logged_in')) return { plan: emptyPlan(), summary: null, held: false, skipped: 'needs login' };
    this.status = { ...this.status, progress: null };
    this.setStateSafely('scanning', reason);
    this.status = { ...this.status, lastCycleAt: this.now() };

    // Inputs.
    let snapshot = this.deps.watcher.currentSnapshot ?? this.lastSnapshot;
    if (snapshot === null) return { plan: emptyPlan(), summary: null, held: false, skipped: 'no local snapshot yet' };
    this.lastSnapshot = snapshot;
    if (!this.deps.mirror.isComplete) {
      try {
        await this.deps.mirror.fullRefresh();
      } catch (error) {
        this.remoteFailure(error);
        return { plan: emptyPlan(), summary: null, held: false, skipped: `remote unavailable: ${this.lastRemoteFailure ?? ''}` };
      }
    }
    if (!this.journalRecovered) {
      const recovery = await this.recoverJournalIfSafe();
      if (recovery === 'blocked') return { plan: emptyPlan(), summary: null, held: false, skipped: 'preflight: journal recovery deferred' };
      // Recovery may have changed local files after the snapshot was taken.
      snapshot = await this.rescanLocal();
    }
    this.refreshLibraryCache();
    const baselineRows = this.deps.baseline.all();
    const baseline = new Map<string, BaselineItem>();
    for (const row of baselineRows) baseline.set(row.relPath, baselineRowToItem(row));
    const needDigest = (relPath: string): boolean => needsDigest(this.deps.baseline.byPath(relPath), snapshot.entries.get(relPath));
    const local = this.withHiddenPaths(await localViewFromSnapshot(snapshot, this.deps.digests, needDigest, this.localRootAvailable), snapshot, baseline);
    const remote = this.deps.mirror.view();
    const sets = this.deps.quarantine.sets();
    // Nothing is written at or under a path the scanner did not sync (e.g. a symlink).
    const plan = blockUnsyncableTargets(reconcile({ baseline, local, remote, quarantinedPaths: sets.paths, quarantinedUids: sets.uids, ignored: this.ignoreMatcher }), snapshot.unsyncable);
    this.deps.scans.markCompleted('local', snapshot.scannedAt);
    this.status = {
      ...this.status,
      pending: {
        uploads: plan.operations.filter((o) => o.kind === 'upload').length,
        downloads: plan.operations.filter((o) => o.kind === 'download').length,
        other: plan.operations.filter((o) => o.kind !== 'upload' && o.kind !== 'download').length,
      },
    };
    for (const b of plan.blocked) this.deps.audit.append({ kind: 'safety', op: 'blocked', message: `${b.reason}: ${b.detail}`, ...(b.relPath !== undefined ? { path: b.relPath } : {}), ...(b.remoteUid !== undefined ? { nodeUid: b.remoteUid } : {}), outcome: 'skipped' });

    // Conflicts first (they only rename locally and detach baseline rows).
    if (plan.conflicts.length > 0 && this.deps.config.dryRun) {
      // Handling a conflict renames the local file and records state before the executor (which is
      // where dry run is otherwise enforced) is reached. A preview only reports what it would do.
      for (const c of plan.conflicts) {
        this.deps.audit.append({ kind: 'conflict', op: c.kind, message: `dry run: would handle a ${c.kind} conflict on ${c.relPath}`, path: c.relPath, ...(c.remoteUid !== undefined ? { nodeUid: c.remoteUid } : {}), outcome: 'skipped' });
      }
      return this.gateAndExecute(plan);
    }
    // Real conflict handling happens only in the configured root: the root must pass preflight
    // before even a rename.
    if (plan.conflicts.length > 0) {
      const failure = await this.preflightFailure(plannedDownloadBytes(plan, this.deps.mirror.view()));
      if (failure !== null) return { plan, summary: null, held: false, skipped: `preflight: ${failure}` };
      await this.deps.conflicts.handleNew(plan.conflicts);
      // Re-plan so the renamed copies are included in this cycle.
      const snapshot2 = await this.rescanLocal();
      const base2 = new Map<string, BaselineItem>();
      for (const row of this.deps.baseline.all()) base2.set(row.relPath, baselineRowToItem(row));
      const local2 = this.withHiddenPaths(await localViewFromSnapshot(snapshot2, this.deps.digests, needDigest, this.localRootAvailable), snapshot2, base2);
      const replanned = blockUnsyncableTargets(reconcile({ baseline: base2, local: local2, remote: this.deps.mirror.view(), quarantinedPaths: sets.paths, quarantinedUids: sets.uids, ignored: this.ignoreMatcher }), snapshot2.unsyncable);
      return this.gateAndExecute(replanned);
    }
    return this.gateAndExecute(plan);
  }

  private async rescanLocal(): Promise<LocalSnapshot> {
    this.deps.watcher.requestFullScan('after conflict handling');
    await this.deps.watcher.flush();
    const snap = this.deps.watcher.currentSnapshot;
    if (snap === null) throw new Error('local snapshot unavailable after rescan');
    this.lastSnapshot = snap;
    return snap;
  }

  private async gateAndExecute(plan: Plan): Promise<CycleResult> {
    if (plan.operations.length === 0 && plan.withheld.length === 0) {
      // Nothing left to confirm: drop a held plan that no longer applies (one a cycle planned
      // before a confirmed plan ran can hold the same, already applied deletes again).
      if (this.deps.gate.current !== null) this.deps.gate.evaluate(plan, this.deps.baseline.count());
      this.finishCycle(true);
      return { plan, summary: null, held: false, skipped: null };
    }
    const failure = await this.preflightFailure(plannedDownloadBytes(plan, this.deps.mirror.view()));
    if (failure !== null) return { plan, summary: null, held: false, skipped: `preflight: ${failure}` };
    const gate = this.deps.gate.evaluate(plan, this.deps.baseline.count());
    if (gate.status === 'held') {
      // Unaffected operations still run.
      const affected = new Set(gate.held.verdict.affected.map((o) => o.id));
      const safe = plan.operations.filter((o) => !affected.has(o.id));
      let summary: ExecutionSummary | null = null;
      if (safe.length > 0) summary = await this.executePlan({ ...plan, operations: safe });
      this.setStateSafely('awaiting_confirmation', gate.held.verdict.reason);
      this.publish();
      return { plan, summary, held: true, skipped: null };
    }
    const summary = await this.executePlan(gate.plan);
    this.finishCycle(summary.failed === 0 && summary.stoppedEarly === null);
    return { plan, summary, held: false, skipped: null };
  }

  /**
   * Run the preflight checks. On failure, audit it, enter the error state and return the
   * reason; null when it is safe to change files and state.
   */
  private async preflightFailure(plannedDownloadBytes: number): Promise<string | null> {
    const preflight = await this.deps.preflight(plannedDownloadBytes);
    if (preflight.ok) return null;
    this.deps.audit.append({ kind: 'safety', op: 'preflight', message: `preflight failed: ${preflight.reason}: ${preflight.detail}`, outcome: 'failed' });
    this.setStateSafely('error', `${preflight.reason}: ${preflight.detail}`);
    return preflight.reason;
  }

  /** Journal recovery redoes or rolls back file changes, so it too needs a passing preflight. */
  private async recoverJournalIfSafe(): Promise<'recovered' | 'blocked'> {
    if (this.deps.config.dryRun) {
      // Recovery updates the baseline, quarantine and temp files from a previous run's journal;
      // a preview leaves that to the next real run.
      this.deps.audit.append({ kind: 'recovery', message: 'dry run: journal recovery deferred to the next real run', outcome: 'skipped' });
      this.journalRecovered = true;
      return 'recovered';
    }
    try {
      if ((await this.preflightFailure(0)) !== null) return 'blocked';
    } catch (error) {
      this.remoteFailure(error);
      return 'blocked';
    }
    const report = await recoverJournal(this.ctx());
    this.deps.audit.append({ kind: 'recovery', message: `journal recovery: ${String(report.completed)} completed, ${String(report.failed)} failed, ${String(report.abandoned)} abandoned`, details: { ...report } });
    this.journalRecovered = true;
    return 'recovered';
  }

  private async executePlan(plan: Plan, options: { dependent?: boolean } = {}): Promise<ExecutionSummary> {
    const fileTotal = plan.operations.filter((o) => o.kind === 'upload' || o.kind === 'download').length;
    this.refreshLibraryCache();
    this.libraryFrozen = true;
    this.status = { ...this.status, progress: fileTotal > 0 ? { done: 0, total: fileTotal } : null };
    this.setStateSafely('syncing');
    this.executor = new Executor(this.ctx());
    if (this.userPaused || this.sessionRejected) this.executor.pause();
    try {
      const summary = await this.executor.execute({ operations: plan.operations, ...(options.dependent === true ? { dependent: true } : {}) });
      if (summary.stoppedEarly === 'disk_full') this.setStateSafely('error', 'disk full');
      if (summary.stoppedEarly === 'auth') {
        // A transfer that the server rejected clears the session, exactly as a rejected listing does,
        // so a later login in the same process transitions back out of needs-login and resumes.
        this.rejectSession(summary.stoppedError);
      }
      // Our own local writes: refresh the snapshot now so the next cycle never sees a stale view.
      const touched = new Set<string>();
      for (const o of plan.operations) {
        if (o.kind === 'move_local' || o.kind === 'move_remote') {
          touched.add(o.from);
          touched.add(o.to);
        } else touched.add(o.relPath);
      }
      await this.deps.watcher.refreshNow([...touched]);
      this.lastSnapshot = this.deps.watcher.currentSnapshot ?? this.lastSnapshot;
      return summary;
    } finally {
      this.executor = null;
      this.transfers.clear();
      this.libraryFrozen = false;
    }
  }

  private finishCycle(success: boolean): void {
    if (success) {
      const copied = this.status.progress?.done ?? 0;
      this.status = { ...this.status, lastSuccessfulSyncAt: this.now(), lastRunFilesCopied: copied };
      const backups = this.deps.executorContext.store.discardPendingBackups();
      if (backups.length > 0) this.deps.audit.append({ kind: 'engine', message: `discarded ${String(backups.length)} migration backup(s) after a successful cycle` });
    }
    this.libraryFrozen = false;
    this.refreshLibraryCache();
    if (this.status.state === 'error' || this.status.state === 'needs_login' || this.status.state === 'paused' || this.status.state === 'stopped') {
      this.publish();
      return;
    }
    this.status = { ...this.status, progress: null };
    this.setStateSafely(this.restingState());
  }

  private onExecutorEvent(e: ExecutorEvent): void {
    switch (e.type) {
      case 'operation_started':
        if (e.operation.kind === 'upload' || e.operation.kind === 'download') {
          this.transfers.set(e.operation.id, { id: e.operation.id, kind: e.operation.kind, relPath: e.operation.relPath, bytes: 0, total: undefined, startedAt: this.now(), speed: 0 });
        }
        break;
      case 'transfer_progress': {
        const t = this.transfers.get(e.operation.id);
        if (t !== undefined) {
          const elapsed = Math.max(1, this.now() - t.startedAt) / 1000;
          this.transfers.set(t.id, { ...t, bytes: e.bytes, total: e.total, speed: e.bytes / elapsed });
        }
        break;
      }
      case 'operation_completed':
      case 'operation_failed':
      case 'operation_skipped':
        if (e.type === 'operation_completed' && (e.operation.kind === 'upload' || e.operation.kind === 'download')) {
          const progress = this.status.progress;
          if (progress !== null) this.status = { ...this.status, progress: { done: progress.done + 1, total: progress.total } };
        }
        if (e.type !== 'operation_failed' || !e.retryable) this.transfers.delete(e.operation.id);
        break;
      case 'paused_for':
      case 'quarantined':
        break;
    }
    this.emit('status', this.getStatus());
    if (e.type === 'quarantined') this.emit('notify', { kind: 'quarantined', message: `Quarantined ${e.relPath ?? e.nodeUid ?? 'item'}: ${e.reason}` });
  }

  // ---- user controls -----------------------------------------------------

  pause(): void {
    this.userPaused = true;
    this.executor?.pause();
    this.deps.audit.append({ kind: 'user', op: 'pause', message: 'user paused syncing' });
    if (this.cycleRunning === null) this.setStateSafely('paused', 'paused by user');
    else this.setStateSafely('paused', 'pausing after the current operation');
  }

  resume(): void {
    if (!this.userPaused) return;
    this.userPaused = false;
    this.deps.audit.append({ kind: 'user', op: 'resume', message: 'user resumed syncing' });
    if (this.status.state === 'paused') this.setStateSafely('idle');
    void this.runCycle('resumed');
  }

  syncNow(): Promise<CycleResult | null> {
    this.deps.audit.append({ kind: 'user', op: 'sync_now', message: 'user requested a sync' });
    if (this.userPaused) return Promise.resolve(null);
    return this.runCycle('sync now');
  }

  async confirmHeldPlan(id: string): Promise<CycleResult | null> {
    // Take turns with cycles, and take the plan inside the turn: a cycle that ran meanwhile has
    // either held the same work again under the same id, or a different plan whose new id refuses
    // this confirmation (and a plan taken before waiting could be held again by a running cycle).
    return this.exclusive(() => {
      const held = this.deps.gate.current;
      return this.runConfirmedPlan(held, this.deps.gate.confirm(id));
    });
  }

  private async runConfirmedPlan(held: HeldPlan | null, plan: Plan): Promise<CycleResult | null> {
    let failure: string | null;
    try {
      failure = await this.preflightFailure(plannedDownloadBytes(plan, this.deps.mirror.view()));
    } catch (error) {
      if (held !== null) this.deps.gate.restore(held);
      throw error;
    }
    if (failure !== null) {
      // Refused, not rejected: the plan stays held for the user to confirm later.
      if (held !== null) this.deps.gate.restore(held);
      return { plan, summary: null, held: true, skipped: `preflight: ${failure}` };
    }
    const summary = await this.executePlan(plan);
    this.finishCycle(summary.failed === 0);
    return { plan, summary, held: false, skipped: null };
  }

  rejectHeldPlan(id: string): Operation[] {
    const affected = this.deps.gate.reject(id);
    this.setStateSafely(this.restingState());
    return affected;
  }

  async resolveConflict(id: number, choice: Resolution): Promise<void> {
    // Resolving rewrites baseline rows and closes the conflict directly; only its file operations
    // go through the (dry-run) executor, so in a preview it would record changes that never happen.
    if (this.deps.config.dryRun) throw new Error('Dry run: conflicts are not resolved; turn dry run off to resolve them');
    // A cycle running meanwhile would plan from a half-done resolution: take turns with cycles.
    await this.exclusive(() => this.resolveConflictNow(id, choice));
  }

  private async resolveConflictNow(id: number, choice: Resolution): Promise<void> {
    // Resolving changes the baseline and plans moves, recycles and trashes: refuse it for the wrong root.
    const failure = await this.preflightFailure(0);
    if (failure !== null) throw new Error(`cannot resolve conflict ${String(id)}: ${failure}`);
    // A resolution's steps depend on each other: execution stops at the first one that does not
    // complete, and the conflict closes only once every step has.
    const run = async (ops: Operation[]): Promise<boolean> => (await this.executePlan({ ...emptyPlan(), operations: ops }, { dependent: true })).completed === ops.length;
    try {
      await this.deps.conflicts.resolve(id, choice, run);
    } finally {
      this.trigger('conflict resolved');
      this.publish();
    }
  }

  releaseQuarantine(id: number): void {
    this.deps.quarantine.release(id);
    this.trigger('quarantine released');
    this.publish();
  }
}

/** Work refused because the engine is stopping. */
export class EngineStoppingError extends Error {
  constructor() {
    super('The engine is stopping');
    this.name = 'EngineStoppingError';
  }
}

function emptyPlan(): Plan {
  return { operations: [], conflicts: [], blocked: [], withheld: [], requiresConfirmation: null, firstSync: false, stats: { deletes: 0, replaces: 0, transfers: 0 } };
}

/** Bytes a plan downloads, for the preflight's free-space check. */
function plannedDownloadBytes(plan: Plan, remote: RemoteView): number {
  return plan.operations.reduce((n, o) => (o.kind === 'download' ? n + (remote.items.get(o.remoteUid)?.size ?? 0) : n), 0);
}

function describeOp(o: Operation): string {
  return o.kind === 'move_local' || o.kind === 'move_remote' ? `${o.kind} ${o.from} -> ${o.to}` : `${o.kind} ${o.relPath}`;
}
