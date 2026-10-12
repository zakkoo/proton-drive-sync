/**
 * Startup recovery over the journal. Runs before any new plan.
 *
 * For every unresolved entry, both sides are inspected to decide whether the
 * operation took effect:
 *  - took effect        -> complete (baseline updated) without redoing it
 *  - clearly did not    -> failed (the item is simply replanned)
 *  - cannot be decided  -> abandoned and quarantined; nothing is deleted
 * Entries still 'planned' were never started: abandoned.
 * Leftover temporary download files are removed (they are ours).
 */
import { lstatSync, readdirSync, statSync, type Stats } from 'node:fs';
import { unlink } from 'node:fs/promises';
import path from 'node:path';

import { INTERNAL_DIR_NAME } from '../config/paths.js';
import { assertWritableInsideRoot, UnsafePathError } from '../safety/pathGuard.js';
import type { BaselineRow } from '../state/baseline.ts';
import type { JournalEntry } from '../state/journal.js';
import { ConflictRepo } from '../state/misc.js';
import type { LocalFingerprint, Operation, OperationKind } from '../reconcile/types.js';
import type { RemoteNode } from '../remote/interface.js';
import { evidenceRow, hashedEvidence, movedEvidence, statEvidence, type LocalEvidence } from './evidence.js';
import { TEMP_FILE_PREFIX, tempDir } from './localWrite.js';
import type { ExecutorContext } from './types.js';

export interface RecoveryReport {
  completed: number;
  failed: number;
  abandoned: number;
  tempFilesRemoved: number;
}

type Verdict = { kind: 'completed'; upserts: BaselineRow[]; removeSubtrees: string[]; renames: { from: string; to: string }[]; note: string } | { kind: 'failed'; note: string } | { kind: 'abandoned'; note: string };

function nameOf(p: string): string {
  const i = p.lastIndexOf('/');
  return i === -1 ? p : p.slice(i + 1);
}

/**
 * Remove our own temp files from interrupted downloads. Only from the real temp folder
 * inside the root: if the internal or temp folder is a symlink, what it points to is not
 * ours, and nothing there is touched. Returns how many files were removed.
 */
async function removeLeftoverTempFiles(ctx: ExecutorContext): Promise<number> {
  const dir = tempDir(ctx.root);
  try {
    assertWritableInsideRoot(ctx.root, `${INTERNAL_DIR_NAME}/tmp/${TEMP_FILE_PREFIX}`);
    if (!lstatSync(dir).isDirectory()) return 0;
  } catch (error) {
    if (error instanceof UnsafePathError) ctx.audit.append({ kind: 'safety', op: 'temp_cleanup', message: `left the temp folder alone: ${error.message}`, outcome: 'skipped' });
    return 0; // no temp folder yet, or not one we may clean
  }
  let removed = 0;
  // Dirent types come from lstat: a symlinked entry is never a file here, and is left alone.
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.startsWith(TEMP_FILE_PREFIX)) continue;
    await unlink(path.join(dir, entry.name));
    removed++;
  }
  return removed;
}

export async function recoverJournal(ctx: ExecutorContext): Promise<RecoveryReport> {
  const report: RecoveryReport = { completed: 0, failed: 0, abandoned: 0, tempFilesRemoved: 0 };

  report.tempFilesRemoved = await removeLeftoverTempFiles(ctx);

  for (const entry of ctx.journal.unresolved()) {
    // Conflict handling journals its local rename with its own entry kind.
    if (entry.op === 'conflict_rename_local' && entry.status === 'in_progress') {
      report[await recoverConflictRename(ctx, entry)]++;
      continue;
    }
    const op = entry.intended as Operation;
    if (entry.status === 'planned') {
      ctx.journal.abandon(entry.id, 'never started before restart');
      ctx.audit.append({ kind: 'recovery', op: op.kind, message: `abandoned planned ${op.kind} (never started)`, ...(entry.relPath !== null ? { path: entry.relPath } : {}), details: { journalId: entry.id } });
      report.abandoned++;
      continue;
    }
    let verdict: Verdict;
    try {
      // An entry this version cannot inspect is closed like an unclear outcome, never left to crash startup.
      verdict = SUPPORTED.has(op.kind) ? await inspect(ctx, op) : { kind: 'abandoned', note: `unsupported journal entry kind ${op.kind}` };
    } catch (error) {
      verdict = { kind: 'abandoned', note: `inspection failed: ${error instanceof Error ? error.message : String(error)}` };
    }
    switch (verdict.kind) {
      case 'completed':
        ctx.store.transaction(() => {
          for (const r of verdict.renames) ctx.baseline.rename(r.from, r.to);
          for (const p of verdict.removeSubtrees) ctx.baseline.removeSubtree(p);
          for (const row of verdict.upserts) ctx.baseline.upsert(row);
          ctx.journal.complete(entry.id, { recovered: true, note: verdict.note });
        });
        report.completed++;
        break;
      case 'failed':
        ctx.journal.fail(entry.id, `recovery: ${verdict.note}`);
        report.failed++;
        break;
      case 'abandoned':
        ctx.journal.abandon(entry.id, `recovery: ${verdict.note}`);
        ctx.quarantine.quarantine({ relPath: entry.relPath, nodeUid: entry.nodeUid, reason: 'unknown_outcome', details: { journalId: entry.id, op: op.kind, note: verdict.note } });
        report.abandoned++;
        break;
    }
    ctx.audit.append({
      kind: 'recovery',
      op: op.kind,
      message: `recovered in-progress ${op.kind}: ${verdict.kind} (${verdict.note})`,
      ...(entry.relPath !== null ? { path: entry.relPath } : {}),
      ...(entry.nodeUid !== null ? { nodeUid: entry.nodeUid } : {}),
      outcome: verdict.kind === 'completed' ? 'ok' : verdict.kind === 'failed' ? 'failed' : 'abandoned',
      details: { journalId: entry.id },
    });
  }
  return report;
}

const SUPPORTED: ReadonlySet<string> = new Set<OperationKind>([
  'create_remote_folder', 'create_local_folder', 'upload', 'download', 'move_local', 'move_remote', 'recycle_local', 'trash_remote', 'update_baseline', 'remove_baseline',
]);

/**
 * Finish or close a conflict rename interrupted by a crash (see ConflictHandler.handleContent),
 * idempotently. The renamed item is recognised by inode: the device number of a btrfs volume
 * changes across the reboot that usually precedes recovery.
 */
async function recoverConflictRename(ctx: ExecutorContext, entry: JournalEntry): Promise<'completed' | 'failed' | 'abandoned'> {
  const intended = entry.intended as { from?: unknown; to?: unknown; conflictKind?: unknown };
  const pre = (entry.preState as { local?: LocalFingerprint } | null)?.local;
  const from = typeof intended.from === 'string' ? intended.from : null;
  const to = typeof intended.to === 'string' ? intended.to : null;
  const isOriginal = (relPath: string | null): boolean => {
    if (relPath === null || pre === undefined) return false;
    const st = localExists(ctx, relPath);
    return st !== null && st.ino === pre.ino;
  };
  let outcome: 'completed' | 'failed' | 'abandoned';
  let note: string;
  if (from !== null && to !== null && isOriginal(to)) {
    // The rename happened: drop the original path's row and make sure the conflict is in the inbox.
    let remote: { uid: string; revisionUid: string | undefined; sha1: string | undefined; size: number | undefined } | null = null;
    if (entry.nodeUid !== null) {
      const node = await ctx.remote.getNode(entry.nodeUid).catch(() => null);
      if (node !== null) remote = { uid: node.uid, revisionUid: node.revisionUid, sha1: node.claimedSha1, size: node.claimedSize };
    }
    const conflicts = new ConflictRepo(ctx.store);
    ctx.store.transaction(() => {
      ctx.baseline.remove(from);
      if (conflicts.openForPath(from) === null) {
        conflicts.add({ relPath: from, nodeUid: entry.nodeUid, kind: typeof intended.conflictKind === 'string' ? intended.conflictKind : 'content', local: { path: to, fingerprint: pre ?? null }, remote });
      }
      ctx.journal.complete(entry.id, { recovered: true, renamedTo: to });
    });
    outcome = 'completed';
    note = `local version kept as ${to}; conflict recorded for ${from}`;
  } else if (from !== null && to !== null && isOriginal(from) && localExists(ctx, to) === null) {
    // Nothing happened: the next cycle detects the conflict again.
    ctx.journal.fail(entry.id, 'recovery: the conflict rename never happened; the next cycle re-detects the conflict');
    outcome = 'failed';
    note = 'the rename never happened';
  } else {
    ctx.journal.abandon(entry.id, 'recovery: cannot tell where the conflicting file went');
    ctx.quarantine.quarantine({ relPath: from ?? entry.previousRelPath, nodeUid: entry.nodeUid, reason: 'unknown_outcome', details: { journalId: entry.id, op: entry.op, note: 'interrupted conflict rename' } });
    outcome = 'abandoned';
    note = 'cannot tell where the conflicting file went';
  }
  ctx.audit.append({
    kind: 'recovery',
    op: entry.op,
    message: `recovered in-progress conflict rename: ${outcome} (${note})`,
    ...(entry.relPath !== null ? { path: entry.relPath } : {}),
    ...(entry.nodeUid !== null ? { nodeUid: entry.nodeUid } : {}),
    outcome: outcome === 'completed' ? 'ok' : outcome,
    details: { journalId: entry.id },
  });
  return outcome;
}

/** A baseline row from `local`: a stat and the digest of the content the file held with it (see evidence.ts). */
function row(ctx: ExecutorContext, relPath: string, kind: 'file' | 'dir', node: RemoteNode, local: LocalEvidence): BaselineRow {
  return evidenceRow(relPath, kind, node, local, ctx.now?.() ?? Date.now());
}

function localExists(ctx: ExecutorContext, relPath: string): Stats | null {
  try {
    return statSync(path.join(ctx.root, relPath));
  } catch {
    return null;
  }
}

async function inspect(ctx: ExecutorContext, op: Operation): Promise<Verdict> {
  const { remote } = ctx;
  switch (op.kind) {
    case 'upload': {
      const local = localExists(ctx, op.relPath);
      if (local === null) return { kind: 'abandoned', note: 'local source is gone; cannot verify what was uploaded' };
      const evidence = await hashedEvidence(path.join(ctx.root, op.relPath));
      if (evidence === null) return { kind: 'failed', note: 'local file changed while it was checked; replanned' };
      const { sha1 } = evidence;
      if (op.mode === 'revision' && op.remoteUid !== undefined) {
        const node = await remote.getNode(op.remoteUid);
        if (node?.claimedSha1 === sha1) return { kind: 'completed', upserts: [row(ctx, op.relPath, 'file', node, evidence)], removeSubtrees: [], renames: [], note: 'remote already holds the uploaded revision' };
        return { kind: 'failed', note: 'remote does not hold the uploaded content' };
      }
      const parentRow = op.relPath.includes('/') ? ctx.baseline.byPath(op.relPath.slice(0, op.relPath.lastIndexOf('/'))) : null;
      const parentUid = op.relPath.includes('/') ? parentRow?.nodeUid : ctx.remoteRootUid;
      if (parentUid === undefined) return { kind: 'failed', note: 'remote parent unknown; will be replanned' };
      const match = (await remote.listChildren(parentUid)).find((c) => c.name === nameOf(op.relPath) && !c.isTrashed && c.type === 'file');
      if (match?.claimedSha1 === sha1) return { kind: 'completed', upserts: [row(ctx, op.relPath, 'file', match, evidence)], removeSubtrees: [], renames: [], note: 'remote already holds the uploaded file' };
      return { kind: 'failed', note: match === undefined ? 'upload did not land' : 'a different file is at the destination' };
    }
    case 'download': {
      const node = await remote.getNode(op.remoteUid);
      const local = localExists(ctx, op.relPath);
      if (node === null) return { kind: 'failed', note: 'remote node gone; replanned' };
      if (local?.isFile() !== true) return { kind: 'failed', note: 'download did not complete; replanned' };
      const previous = op.expectedLocal;
      if (local.ino === previous?.ino && local.mtimeMs === previous.mtimeMs && local.size === previous.size) {
        return { kind: 'failed', note: 'target still holds the previous version; replanned' };
      }
      const evidence = await hashedEvidence(path.join(ctx.root, op.relPath));
      if (evidence === null) return { kind: 'failed', note: 'local file changed while it was checked; replanned' };
      const { sha1 } = evidence;
      if (node.claimedSha1 !== undefined && node.claimedSha1 === sha1) return { kind: 'completed', upserts: [row(ctx, op.relPath, 'file', node, evidence)], removeSubtrees: [], renames: [], note: 'downloaded content is in place' };
      if (node.claimedSha1 === undefined) return { kind: 'abandoned', note: 'no remote digest to verify the downloaded file against' };
      return { kind: 'abandoned', note: 'local file matches neither the previous nor the remote version' };
    }
    case 'move_local': {
      const src = localExists(ctx, op.from);
      const dst = localExists(ctx, op.to);
      if (src !== null && dst === null) return { kind: 'failed', note: 'move not performed; replanned' };
      if (src === null && dst !== null && dst.ino === op.expectedLocal.ino) {
        const node = await remote.getNode(op.remoteUid);
        if (node === null) return { kind: 'abandoned', note: 'moved locally but the remote node is gone' };
        const old = ctx.baseline.byPath(op.baselineFrom ?? op.from);
        return { kind: 'completed', upserts: [row(ctx, op.to, dst.isDirectory() ? 'dir' : 'file', node, movedEvidence(old, ctx.root, op.to))], removeSubtrees: [], renames: [{ from: op.baselineFrom ?? op.from, to: op.to }], note: 'local move already done' };
      }
      return { kind: 'abandoned', note: 'neither the source nor the expected destination is in the planned state' };
    }
    case 'move_remote': {
      const node = await remote.getNode(op.remoteUid);
      if (node === null) return { kind: 'abandoned', note: 'remote node gone during move' };
      const parentRow = op.to.includes('/') ? ctx.baseline.byPath(op.to.slice(0, op.to.lastIndexOf('/'))) : null;
      const targetParent = op.to.includes('/') ? parentRow?.nodeUid : ctx.remoteRootUid;
      if (targetParent !== undefined && node.parentUid === targetParent && node.name === nameOf(op.to)) {
        const old = ctx.baseline.byPath(op.baselineFrom ?? op.from);
        const local = localExists(ctx, op.to);
        if (local === null) return { kind: 'abandoned', note: 'remote moved but the local item is not at the destination' };
        return { kind: 'completed', upserts: [row(ctx, op.to, node.type === 'folder' ? 'dir' : 'file', node, movedEvidence(old, ctx.root, op.to))], removeSubtrees: [], renames: [{ from: op.baselineFrom ?? op.from, to: op.to }], note: 'remote move already done' };
      }
      return { kind: 'failed', note: 'remote move not performed; replanned' };
    }
    case 'recycle_local': {
      const local = localExists(ctx, op.relPath);
      if (local !== null) return { kind: 'failed', note: 'item still present; replanned' };
      const recycled = ctx.recycle.list().some((r) => r.relPath === op.relPath);
      if (recycled) return { kind: 'completed', upserts: [], removeSubtrees: [op.relPath], renames: [], note: 'item is in the recycle bin' };
      return { kind: 'abandoned', note: 'item is gone but not in the recycle bin' };
    }
    case 'trash_remote': {
      const node = await remote.getNode(op.remoteUid);
      if (node === null || node.isTrashed) return { kind: 'completed', upserts: [], removeSubtrees: [op.relPath], renames: [], note: 'remote node is trashed' };
      return { kind: 'failed', note: 'remote node not trashed; replanned' };
    }
    case 'create_remote_folder': {
      const parentRow = op.relPath.includes('/') ? ctx.baseline.byPath(op.relPath.slice(0, op.relPath.lastIndexOf('/'))) : null;
      const parentUid = op.relPath.includes('/') ? parentRow?.nodeUid : ctx.remoteRootUid;
      if (parentUid === undefined) return { kind: 'failed', note: 'remote parent unknown; replanned' };
      const match = (await remote.listChildren(parentUid)).find((c) => c.name === nameOf(op.relPath) && !c.isTrashed && c.type === 'folder');
      if (match !== undefined && localExists(ctx, op.relPath) !== null) return { kind: 'completed', upserts: [row(ctx, op.relPath, 'dir', match, statEvidence(ctx.root, op.relPath))], removeSubtrees: [], renames: [], note: 'remote folder exists' };
      return { kind: 'failed', note: 'remote folder not created; replanned' };
    }
    case 'create_local_folder': {
      const node = await remote.getNode(op.remoteUid);
      const local = localExists(ctx, op.relPath);
      if (node !== null && local?.isDirectory() === true) return { kind: 'completed', upserts: [row(ctx, op.relPath, 'dir', node, statEvidence(ctx.root, op.relPath))], removeSubtrees: [], renames: [], note: 'local folder exists' };
      return { kind: 'failed', note: 'local folder not created; replanned' };
    }
    case 'update_baseline':
    case 'remove_baseline':
      return { kind: 'failed', note: `bookkeeping ${op.kind} is never journaled` };
  }
}
