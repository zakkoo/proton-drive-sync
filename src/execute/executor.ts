/**
 * Executor: runs a Plan with a write-ahead journal.
 *
 * Per operation: journal(planned) -> precheck -> journal(in_progress) -> act
 * -> verify -> commit [journal(completed) + baseline] in one transaction.
 * Any failure leaves the baseline untouched and marks the entry failed; the
 * item is re-reconciled next cycle. Transfers run with bounded concurrency;
 * structural operations run in plan order. Pause and cancel take effect at
 * operation boundaries.
 */
import { existsSync, mkdirSync, statSync } from 'node:fs';
import { mkdir, rename, stat } from 'node:fs/promises';
import path from 'node:path';

import type { BaselineRow } from '../state/baseline.ts';
import type { Operation, RemoteFingerprint } from '../reconcile/types.js';
import { RemoteError, type RemoteNode } from '../remote/interface.js';
import { defaultSleep } from '../remote/proton/apiClient.js';
import { uploadVerified } from '../remote/transfer.js';
import { atomicDownload, DiskFullError, fingerprintMismatch, TargetChangedError } from './localWrite.js';
import { assertWritableInsideRoot } from '../safety/pathGuard.js';
import { SimulatedCrashError, type ExecutionSummary, type ExecutorContext, type ExecutorEvent, type ExecutorStep, type PlanForExecution, type RemoteChange } from './types.js';

export class PreconditionError extends Error {
  constructor(detail: string) {
    super(`Precondition changed: ${detail}`);
    this.name = 'PreconditionError';
  }
}

export class VerificationError extends Error {
  constructor(detail: string) {
    super(`Verification failed: ${detail}`);
    this.name = 'VerificationError';
  }
}

interface ActResult {
  /** Remote nodes changed by this operation, reported to the caller at commit. */
  remoteChanges?: RemoteChange[];
  /** Rows to upsert at commit. */
  upserts: BaselineRow[];
  /** Paths to remove from the baseline at commit (subtree). */
  removeSubtrees: string[];
  /** Baseline renames (from -> to) at commit. */
  renames: { from: string; to: string }[];
  outcome: Record<string, unknown>;
}

const TRANSFER_KINDS = new Set<Operation['kind']>(['upload', 'download']);

function describe(o: Operation): string {
  return o.kind === 'move_local' || o.kind === 'move_remote' ? `${o.kind} ${o.from} -> ${o.to}` : `${o.kind} ${o.relPath}`;
}

function relPathOf(o: Operation): string {
  return o.kind === 'move_local' || o.kind === 'move_remote' ? o.to : o.relPath;
}

function parentOf(p: string): string {
  const i = p.lastIndexOf('/');
  return i === -1 ? '' : p.slice(0, i);
}

function nameOf(p: string): string {
  const i = p.lastIndexOf('/');
  return i === -1 ? p : p.slice(i + 1);
}

function remoteFingerprintMismatch(node: RemoteNode | null, expected: RemoteFingerprint): string | null {
  if (node === null) return `remote node ${expected.uid} is missing`;
  if (node.isTrashed) return `remote node ${expected.uid} is trashed`;
  // Location too: a rename or move keeps the revision and digest, so they alone cannot tell.
  if ((node.parentUid ?? undefined) !== (expected.parentUid ?? undefined)) return `moved to another folder (${expected.parentUid ?? 'none'} -> ${node.parentUid ?? 'none'})`;
  if (node.name !== expected.name) return `renamed (${expected.name} -> ${node.name})`;
  if ((node.revisionUid ?? undefined) !== (expected.revisionUid ?? undefined)) return `revision changed (${expected.revisionUid ?? 'none'} -> ${node.revisionUid ?? 'none'})`;
  if (expected.sha1 !== undefined && node.claimedSha1 !== undefined && node.claimedSha1 !== expected.sha1) return 'remote digest changed';
  return null;
}

export class Executor {
  private paused = false;
  /** The remote error that stopped the run (auth), so the engine can clear the session. */
  private stoppingError: unknown = undefined;
  private readonly cancel = new AbortController();
  private readonly createdRemoteDirs = new Map<string, string>();
  private readonly sleep: (ms: number, signal?: AbortSignal) => Promise<void>;
  private readonly now: () => number;

  constructor(private readonly ctx: ExecutorContext) {
    this.sleep = ctx.sleep ?? defaultSleep;
    this.now = ctx.now ?? Date.now;
  }

  /** Stop starting new operations; in-flight ones finish. */
  pause(): void {
    this.paused = true;
  }

  /** Abort in-flight transfers and stop. Temp files are cleaned; targets untouched. */
  cancelAll(): void {
    this.paused = true;
    this.cancel.abort(new Error('cancelled'));
  }

  private emit(event: ExecutorEvent): void {
    this.ctx.onEvent?.(event);
  }

  private async step(step: ExecutorStep, op: Operation, attempt: number): Promise<void> {
    await this.ctx.hooks?.beforeStep?.(step, op, attempt);
  }

  /** Execute a plan. Operations run in order; consecutive transfers run concurrently. */
  async execute(plan: PlanForExecution): Promise<ExecutionSummary> {
    const summary: ExecutionSummary = { completed: 0, skipped: 0, failed: 0, stoppedEarly: null };
    const ops = plan.operations;
    let i = 0;
    while (i < ops.length) {
      if (this.cancel.signal.aborted) {
        summary.stoppedEarly = 'cancelled';
        break;
      }
      if (this.paused) {
        summary.stoppedEarly = 'paused';
        break;
      }
      const op = ops[i];
      if (op === undefined) break;
      if (TRANSFER_KINDS.has(op.kind)) {
        // Gather a batch of consecutive transfers up to the concurrency limit.
        const batch: Operation[] = [];
        while (i < ops.length && batch.length < Math.max(1, this.ctx.config.concurrency)) {
          const next = ops[i];
          if (next === undefined || !TRANSFER_KINDS.has(next.kind)) break;
          batch.push(next);
          i++;
        }
        const results = await Promise.all(batch.map((b) => this.runOne(b)));
        for (const r of results) this.tally(summary, r);
        if (results.some((r) => r === 'disk_full')) {
          summary.stoppedEarly = 'disk_full';
          break;
        }
        if (results.some((r) => r === 'auth')) {
          summary.stoppedEarly = 'auth';
          summary.stoppedError = this.stoppingError;
          break;
        }
        // After the stop reasons above, which the engine acts on.
        if (plan.dependent === true && results.some((r) => r !== 'completed')) break;
      } else {
        const r = await this.runOne(op);
        this.tally(summary, r);
        i++;
        if (r === 'disk_full' || r === 'auth') {
          summary.stoppedEarly = r;
          if (r === 'auth') summary.stoppedError = this.stoppingError;
          break;
        }
        // After the stop reasons above, which the engine acts on.
        if (plan.dependent === true && r !== 'completed') break;
      }
    }
    return summary;
  }

  private tally(summary: ExecutionSummary, r: RunResult): void {
    switch (r) {
      case 'completed':
        summary.completed++;
        break;
      case 'skipped':
        summary.skipped++;
        break;
      case 'failed':
      case 'disk_full':
      case 'auth':
        summary.failed++;
        break;
    }
  }

  private async runOne(op: Operation): Promise<RunResult> {
    if (this.ctx.config.dryRun) {
      this.ctx.audit.append({ kind: 'would_do', op: op.kind, message: `would ${describe(op)}`, path: relPathOf(op), outcome: 'would_do', details: { evidence: op.evidence } });
      this.emit({ type: 'operation_skipped', operation: op, reason: 'dry run' });
      return 'skipped';
    }
    if (op.kind === 'update_baseline' || op.kind === 'remove_baseline') return this.runBookkeeping(op);

    await this.step('journal_planned', op, 0);
    const entry = this.ctx.journal.plan({
      op: op.kind,
      relPath: relPathOf(op),
      previousRelPath: op.kind === 'move_local' || op.kind === 'move_remote' ? op.from : null,
      nodeUid: 'remoteUid' in op ? (op.remoteUid ?? null) : null,
      intended: op,
      preState: { local: 'expectedLocal' in op ? op.expectedLocal : undefined, remote: 'expectedRemote' in op ? op.expectedRemote : undefined },
    });
    this.ctx.audit.append({ kind: 'plan', op: op.kind, message: `planned ${describe(op)}`, path: relPathOf(op), ...(entry.nodeUid !== null ? { nodeUid: entry.nodeUid } : {}), details: { journalId: entry.id, evidence: op.evidence } });

    for (let attempt = 1; ; attempt++) {
      try {
        await this.step('precheck', op, attempt);
        const pre = await this.precheck(op);
        if (pre !== null) {
          await this.step('journal_failed', op, attempt);
          // Never started: the entry is abandoned (planned -> abandoned), and the item is replanned.
          this.ctx.journal.abandon(entry.id, `precondition: ${pre}`);
          this.ctx.audit.append({ kind: 'execute', op: op.kind, message: `skipped ${describe(op)}: ${pre}`, path: relPathOf(op), outcome: 'skipped', details: { journalId: entry.id } });
          this.emit({ type: 'operation_skipped', operation: op, reason: pre });
          return 'skipped';
        }
        if (attempt === 1) {
          await this.step('journal_started', op, attempt);
          this.ctx.journal.start(entry.id);
        }
        this.emit({ type: 'operation_started', operation: op, attempt });

        await this.step('act', op, attempt);
        const result = await this.act(op, attempt);

        await this.step('verify', op, attempt);
        await this.step('commit', op, attempt);
        this.commit(entry.id, result);
        await this.step('journal_completed', op, attempt);
        this.ctx.audit.append({ kind: 'execute', op: op.kind, message: `completed ${describe(op)}`, path: relPathOf(op), ...(entry.nodeUid !== null ? { nodeUid: entry.nodeUid } : {}), outcome: 'ok', details: { journalId: entry.id, ...result.outcome } });
        this.emit({ type: 'operation_completed', operation: op });
        return 'completed';
      } catch (error) {
        if (error instanceof SimulatedCrashError) throw error;
        const message = error instanceof Error ? error.message : String(error);
        const current = this.ctx.journal.get(entry.id);
        if (error instanceof DiskFullError) {
          if (current.status === 'in_progress') this.ctx.journal.fail(entry.id, message);
          else if (current.status === 'planned') this.ctx.journal.abandon(entry.id, message);
          this.ctx.audit.append({ kind: 'safety', op: op.kind, message: `paused: ${message}`, path: relPathOf(op), outcome: 'failed', details: { journalId: entry.id } });
          this.emit({ type: 'paused_for', reason: 'disk_full' });
          return 'disk_full';
        }
        const remoteErr = error instanceof RemoteError ? error : null;
        if (remoteErr?.kind === 'auth') {
          if (current.status === 'in_progress') this.ctx.journal.fail(entry.id, message);
          else if (current.status === 'planned') this.ctx.journal.abandon(entry.id, message);
          this.stoppingError = error;
          this.emit({ type: 'paused_for', reason: 'auth' });
          return 'auth';
        }
        const retryable = (remoteErr?.retryable ?? false) && !this.cancel.signal.aborted && attempt <= this.ctx.config.maxRetries;
        if (retryable) {
          const wait = Math.min(30_000, 500 * 2 ** (attempt - 1));
          this.ctx.logger.warn(`${describe(op)} failed (${message}); retrying in ${String(wait)}ms after re-checking the destination`);
          this.emit({ type: 'operation_failed', operation: op, error: message, retryable: true });
          try {
            await this.sleep(wait, this.cancel.signal);
          } catch {
            // cancelled while waiting
          }
          // Before retrying a mutation, check whether it already took effect.
          const landed = await this.alreadyApplied(op).catch(() => null);
          if (landed !== null) {
            this.commit(entry.id, landed);
            this.ctx.audit.append({ kind: 'execute', op: op.kind, message: `completed ${describe(op)} (already applied before retry)`, path: relPathOf(op), outcome: 'ok', details: { journalId: entry.id, ...landed.outcome } });
            this.emit({ type: 'operation_completed', operation: op });
            return 'completed';
          }
          // Journal stays in_progress across attempts; a crash here is handled by recovery.
          this.ctx.journal.recordRetry(entry.id);
          continue;
        }
        if (error instanceof VerificationError || remoteErr?.kind === 'integrity') {
          this.ctx.quarantine.quarantine({ relPath: relPathOf(op), nodeUid: 'remoteUid' in op ? (op.remoteUid ?? null) : null, reason: 'verification_failed', details: { error: message, op: op.kind } });
          this.emit({ type: 'quarantined', relPath: relPathOf(op), nodeUid: 'remoteUid' in op ? (op.remoteUid ?? null) : null, reason: message });
        }
        if (current.status === 'in_progress') {
          await this.step('journal_failed', op, attempt);
          this.ctx.journal.fail(entry.id, message);
        } else if (current.status === 'planned') {
          this.ctx.journal.abandon(entry.id, message);
        }
        this.ctx.audit.append({ kind: 'execute', op: op.kind, message: `failed ${describe(op)}: ${message}`, path: relPathOf(op), outcome: 'failed', error: message, details: { journalId: entry.id, attempt } });
        this.emit({ type: 'operation_failed', operation: op, error: message, retryable: false });
        return 'failed';
      }
    }
  }

  private runBookkeeping(op: Extract<Operation, { kind: 'update_baseline' | 'remove_baseline' }>): RunResult {
    if (op.kind === 'remove_baseline') {
      this.ctx.baseline.removeSubtree(op.relPath);
      this.ctx.audit.append({ kind: 'execute', op: op.kind, message: `baseline row removed for ${op.relPath}`, path: op.relPath, outcome: 'ok', details: { evidence: op.evidence } });
      return 'completed';
    }
    let st;
    try {
      st = statSync(path.join(this.ctx.root, op.relPath));
    } catch {
      this.emit({ type: 'operation_skipped', operation: op, reason: 'local item vanished' });
      return 'skipped';
    }
    // The whole planned fingerprint must still hold, not just the inode: an in-place edit
    // keeps the inode but changes size and mtime, and the planned digest only vouches for
    // the planned content. Recording it with new stat values would make the stat fast
    // path skip hashing for good, so the edit would never sync.
    const changed = fingerprintMismatch(op.relPath, op.local, this.ctx.root);
    if (changed !== null) {
      this.emit({ type: 'operation_skipped', operation: op, reason: `local item changed: ${changed}` });
      return 'skipped';
    }
    this.ctx.baseline.upsert({
      relPath: op.relPath,
      kind: op.itemKind,
      localDev: st.dev,
      localIno: st.ino,
      localSize: st.size,
      localMtimeMs: st.mtimeMs,
      localSha1: op.local.sha1 ?? null,
      nodeUid: op.remote.uid,
      parentUid: op.remote.parentUid ?? null,
      remoteName: op.remote.name,
      revisionUid: op.remote.revisionUid ?? null,
      remoteSha1: op.remote.sha1 ?? null,
      syncedAt: this.now(),
    });
    this.ctx.audit.append({ kind: 'execute', op: op.kind, message: `baseline updated for ${op.relPath}`, path: op.relPath, nodeUid: op.remote.uid, outcome: 'ok', details: { evidence: op.evidence } });
    return 'completed';
  }

  /**
   * Before trashing, the item must still sit at `relPath` under the sync root: every folder
   * up to the root keeps its planned name. A reason to skip, or null.
   */
  private async remotePathMismatch(node: RemoteNode, relPath: string): Promise<string | null> {
    const parts = relPath.split('/');
    let current = node;
    for (let i = parts.length - 1; i >= 0; i--) {
      if (current.name !== parts[i]) return `remote path changed: ${relPath} is no longer where it was planned`;
      const parentUid = current.parentUid;
      if (parentUid === undefined) return `remote item ${relPath} is no longer under the sync root`;
      if (i === 0) return parentUid === this.ctx.remoteRootUid ? null : `remote item ${relPath} is no longer under the sync root`;
      const parent = await this.ctx.remote.getNode(parentUid);
      if (parent === null || parent.isTrashed) return `remote folder ${parts.slice(0, i).join('/')} is gone`;
      current = parent;
    }
    return null;
  }

  /** Re-check both sides against the fingerprints recorded at planning. Returns a reason to skip, or null. */
  private async precheck(op: Operation): Promise<string | null> {
    switch (op.kind) {
      case 'upload': {
        const local = fingerprintMismatch(op.relPath, op.expectedLocal, this.ctx.root);
        if (local !== null) return `local ${local}`;
        if (op.mode === 'revision' && op.remoteUid !== undefined && op.expectedRemote !== undefined) {
          const remote = remoteFingerprintMismatch(await this.ctx.remote.getNode(op.remoteUid), op.expectedRemote);
          if (remote !== null) return remote;
        }
        return null;
      }
      case 'download': {
        const local = fingerprintMismatch(op.relPath, op.expectedLocal, this.ctx.root);
        if (local !== null) return `local ${local}`;
        return remoteFingerprintMismatch(await this.ctx.remote.getNode(op.remoteUid), op.expectedRemote);
      }
      case 'move_local': {
        const src = fingerprintMismatch(op.from, op.expectedLocal, this.ctx.root);
        if (src !== null) return `local source ${src}`;
        return fingerprintMismatch(op.to, undefined, this.ctx.root) === null ? null : `local destination ${op.to} is occupied`;
      }
      case 'move_remote':
        return remoteFingerprintMismatch(await this.ctx.remote.getNode(op.remoteUid), op.expectedRemote);
      case 'recycle_local': {
        const local = fingerprintMismatch(op.relPath, op.expectedLocal, this.ctx.root);
        return local === null ? null : `local ${local}`;
      }
      case 'trash_remote': {
        const node = await this.ctx.remote.getNode(op.remoteUid);
        const changed = remoteFingerprintMismatch(node, op.expectedRemote);
        if (changed !== null || node === null) return changed;
        // The node's own parent and name can match while an ancestor folder was renamed or moved.
        return this.remotePathMismatch(node, op.relPath);
      }
      case 'create_remote_folder':
      case 'create_local_folder':
      case 'update_baseline':
      case 'remove_baseline':
        return null;
    }
  }

  private remoteParentUid(relPath: string): string {
    const parent = parentOf(relPath);
    if (parent === '') return this.ctx.remoteRootUid;
    const created = this.createdRemoteDirs.get(parent);
    if (created !== undefined) return created;
    const row = this.ctx.baseline.byPath(parent);
    if (row === null) throw new PreconditionError(`remote parent folder ${parent} is not known yet`);
    return row.nodeUid;
  }

  private localRow(relPath: string, kind: 'file' | 'dir', node: RemoteNode, sha1: string | null): BaselineRow {
    const st = statSync(path.join(this.ctx.root, relPath));
    return {
      relPath,
      kind,
      localDev: st.dev,
      localIno: st.ino,
      localSize: kind === 'file' ? st.size : 0,
      localMtimeMs: st.mtimeMs,
      localSha1: sha1,
      nodeUid: node.uid,
      parentUid: node.parentUid ?? null,
      remoteName: node.name,
      revisionUid: node.revisionUid ?? null,
      remoteSha1: node.claimedSha1 ?? null,
      syncedAt: this.now(),
    };
  }

  private async act(op: Operation, attempt: number): Promise<ActResult> {
    const { remote, root } = this.ctx;
    switch (op.kind) {
      case 'create_remote_folder': {
        const parentUid = this.remoteParentUid(op.relPath);
        const st = statSync(path.join(root, op.relPath));
        const node = await remote.createFolder(parentUid, nameOf(op.relPath), new Date(st.mtimeMs));
        this.createdRemoteDirs.set(op.relPath, node.uid);
        return { remoteChanges: [{ type: 'upsert', node }], upserts: [this.localRow(op.relPath, 'dir', node, null)], removeSubtrees: [], renames: [], outcome: { nodeUid: node.uid } };
      }
      case 'create_local_folder': {
        const node = await remote.getNode(op.remoteUid);
        if (node === null) throw new PreconditionError(`remote folder ${op.remoteUid} vanished`);
        assertWritableInsideRoot(root, op.relPath);
        await mkdir(path.join(root, op.relPath), { recursive: false });
        return { upserts: [this.localRow(op.relPath, 'dir', node, null)], removeSubtrees: [], renames: [], outcome: { nodeUid: node.uid } };
      }
      case 'upload': {
        const abs = path.join(root, op.relPath);
        const total = op.expectedLocal.size;
        const target = op.mode === 'new' || op.remoteUid === undefined ? ({ kind: 'new', parentUid: this.remoteParentUid(op.relPath), name: nameOf(op.relPath) } as const) : ({ kind: 'revision', nodeUid: op.remoteUid } as const);
        const uploaded = await uploadVerified(remote, target, abs, {
          onProgress: (bytes) => { this.emit({ type: 'transfer_progress', operation: op, bytes, total }); },
          signal: this.cancel.signal,
        });
        const node = await remote.getNode(uploaded.nodeUid);
        if (node === null) throw new VerificationError(`uploaded node ${uploaded.nodeUid} cannot be read back`);
        // The local file must still be what we hashed.
        const local = fingerprintMismatch(op.relPath, op.expectedLocal, root);
        if (local !== null) throw new PreconditionError(`local file changed during upload: ${local}`);
        return { remoteChanges: [{ type: 'upsert', node }], upserts: [this.localRow(op.relPath, 'file', node, uploaded.sha1)], removeSubtrees: [], renames: [], outcome: { nodeUid: node.uid, revisionUid: uploaded.revisionUid, sha1: uploaded.sha1, size: uploaded.size, attempt } };
      }
      case 'download': {
        const node = await remote.getNode(op.remoteUid);
        if (node === null) throw new PreconditionError(`remote node ${op.remoteUid} vanished`);
        const result = await atomicDownload(root, remote, node, op.relPath, op.expectedLocal, this.ctx.recycle, {
          onProgress: (bytes) => { this.emit({ type: 'transfer_progress', operation: op, bytes, total: node.claimedSize }); },
          signal: this.cancel.signal,
        });
        return { upserts: [this.localRow(op.relPath, 'file', node, result.sha1)], removeSubtrees: [], renames: [], outcome: { sha1: result.sha1, size: result.size, verifiedAgainstClaim: result.verifiedAgainstClaim, recycledPrevious: result.recycledPrevious } };
      }
      case 'move_local': {
        const from = path.join(root, op.from);
        const to = path.join(root, op.to);
        assertWritableInsideRoot(root, op.from);
        assertWritableInsideRoot(root, op.to);
        mkdirSync(path.dirname(to), { recursive: true });
        assertWritableInsideRoot(root, op.to);
        await rename(from, to);
        const node = await remote.getNode(op.remoteUid);
        if (node === null) throw new VerificationError(`remote node ${op.remoteUid} vanished after local move`);
        const st = await stat(to);
        const row = this.ctx.baseline.byPath(op.baselineFrom ?? op.from);
        const kind = st.isDirectory() ? 'dir' : 'file';
        return {
          upserts: [{ ...this.localRow(op.to, kind, node, row?.localSha1 ?? null) }],
          removeSubtrees: [],
          renames: [{ from: op.baselineFrom ?? op.from, to: op.to }],
          outcome: { from: op.from, to: op.to },
        };
      }
      case 'move_remote': {
        const targetParent = this.remoteParentUid(op.to);
        let node = await remote.getNode(op.remoteUid);
        if (node === null) throw new PreconditionError(`remote node ${op.remoteUid} vanished`);
        if (node.parentUid !== targetParent) node = await remote.move(op.remoteUid, targetParent);
        if (node.name !== nameOf(op.to)) node = await remote.rename(op.remoteUid, nameOf(op.to));
        const verify = await remote.getNode(op.remoteUid);
        if (verify?.parentUid !== targetParent || verify.name !== nameOf(op.to)) throw new VerificationError(`remote node ${op.remoteUid} is not at ${op.to} after move`);
        // Remote first, local follows (conflict resolution plans a move_local right after): until
        // the local item is at `to`, the baseline keeps describing it at `from`, and the move_local
        // records the new location. A failed local move then still reads as a remote rename.
        if (!existsSync(path.join(root, op.to))) return { remoteChanges: [{ type: 'upsert', node: verify }], upserts: [], removeSubtrees: [], renames: [], outcome: { from: op.from, to: op.to, localMoveFollows: true } };
        const row = this.ctx.baseline.byPath(op.baselineFrom ?? op.from);
        const kind = verify.type === 'folder' ? 'dir' : 'file';
        return { remoteChanges: [{ type: 'upsert', node: verify }], upserts: [this.localRow(op.to, kind, verify, row?.localSha1 ?? null)], removeSubtrees: [], renames: [{ from: op.baselineFrom ?? op.from, to: op.to }], outcome: { from: op.from, to: op.to } };
      }
      case 'recycle_local': {
        const item = this.ctx.recycle.recycle(op.relPath, 'deleted remotely');
        return { upserts: [], removeSubtrees: [op.relPath], renames: [], outcome: { recycledTo: item.absolutePath } };
      }
      case 'trash_remote': {
        await remote.trash([op.remoteUid]);
        const verify = await remote.getNode(op.remoteUid);
        if (verify !== null && !verify.isTrashed) throw new VerificationError(`remote node ${op.remoteUid} is not trashed after trash`);
        return { remoteChanges: [verify === null ? { type: 'remove', uid: op.remoteUid } : { type: 'upsert', node: verify }], upserts: [], removeSubtrees: [op.relPath], renames: [], outcome: { nodeUid: op.remoteUid } };
      }
      case 'update_baseline':
      case 'remove_baseline':
        throw new Error('bookkeeping operations do not go through act()');
    }
  }

  /** After a retryable failure of unknown outcome: did the mutation already land? */
  private async alreadyApplied(op: Operation): Promise<ActResult | null> {
    const { remote } = this.ctx;
    switch (op.kind) {
      case 'upload': {
        if (op.mode === 'revision' && op.remoteUid !== undefined) {
          const node = await remote.getNode(op.remoteUid);
          if (node !== null && node.claimedSha1 === op.expectedLocal.sha1 && op.expectedLocal.sha1 !== undefined) {
            return { remoteChanges: [{ type: 'upsert', node }], upserts: [this.localRow(op.relPath, 'file', node, node.claimedSha1 ?? null)], removeSubtrees: [], renames: [], outcome: { nodeUid: node.uid, revisionUid: node.revisionUid, landedBeforeRetry: true } };
          }
          return null;
        }
        const parentUid = this.remoteParentUid(op.relPath);
        const children = await remote.listChildren(parentUid);
        const match = children.find((c) => c.name === nameOf(op.relPath) && !c.isTrashed && c.type === 'file');
        const expectedSha1 = op.expectedLocal.sha1;
        if (match !== undefined && expectedSha1 !== undefined && match.claimedSha1 === expectedSha1) {
          return { remoteChanges: [{ type: 'upsert', node: match }], upserts: [this.localRow(op.relPath, 'file', match, expectedSha1)], removeSubtrees: [], renames: [], outcome: { nodeUid: match.uid, revisionUid: match.revisionUid, landedBeforeRetry: true } };
        }
        return null;
      }
      case 'trash_remote': {
        const node = await remote.getNode(op.remoteUid);
        if (node !== null && !node.isTrashed) return null;
        return { remoteChanges: [node === null ? { type: 'remove', uid: op.remoteUid } : { type: 'upsert', node }], upserts: [], removeSubtrees: [op.relPath], renames: [], outcome: { nodeUid: op.remoteUid, landedBeforeRetry: true } };
      }
      case 'move_remote': {
        const node = await remote.getNode(op.remoteUid);
        if (node !== null && node.parentUid === this.remoteParentUid(op.to) && node.name === nameOf(op.to)) {
          if (!existsSync(path.join(this.ctx.root, op.to))) return { remoteChanges: [{ type: 'upsert', node }], upserts: [], removeSubtrees: [], renames: [], outcome: { landedBeforeRetry: true, localMoveFollows: true } };
          const row = this.ctx.baseline.byPath(op.baselineFrom ?? op.from);
          return { remoteChanges: [{ type: 'upsert', node }], upserts: [this.localRow(op.to, node.type === 'folder' ? 'dir' : 'file', node, row?.localSha1 ?? null)], removeSubtrees: [], renames: [{ from: op.baselineFrom ?? op.from, to: op.to }], outcome: { landedBeforeRetry: true } };
        }
        return null;
      }
      case 'create_remote_folder': {
        const parentUid = this.remoteParentUid(op.relPath);
        const children = await remote.listChildren(parentUid);
        const match = children.find((c) => c.name === nameOf(op.relPath) && !c.isTrashed && c.type === 'folder');
        if (match === undefined) return null;
        this.createdRemoteDirs.set(op.relPath, match.uid);
        return { remoteChanges: [{ type: 'upsert', node: match }], upserts: [this.localRow(op.relPath, 'dir', match, null)], removeSubtrees: [], renames: [], outcome: { nodeUid: match.uid, landedBeforeRetry: true } };
      }
      case 'create_local_folder':
      case 'download':
      case 'move_local':
      case 'recycle_local':
      case 'update_baseline':
      case 'remove_baseline':
        // Local-side or non-mutating-remote operations are simply redone; nothing to re-check remotely.
        return null;
    }
  }

  /** Journal completion and baseline update in one transaction, then report remote changes. */
  private commit(journalId: number, result: ActResult): void {
    this.ctx.store.transaction(() => {
      for (const r of result.renames) this.ctx.baseline.rename(r.from, r.to);
      for (const p of result.removeSubtrees) this.ctx.baseline.removeSubtree(p);
      for (const row of result.upserts) this.ctx.baseline.upsert(row);
      this.ctx.journal.complete(journalId, result.outcome);
    });
    for (const change of result.remoteChanges ?? []) this.ctx.onRemoteChange?.(change);
  }
}

type RunResult = 'completed' | 'skipped' | 'failed' | 'disk_full' | 'auth';

export { TargetChangedError };
