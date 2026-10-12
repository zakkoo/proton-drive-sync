/**
 * Baseline rows built from evidence.
 *
 * A file's row vouches that its local stat and its digest describe the same content. The
 * stat fast path (needsDigest) trusts that pairing for as long as the file keeps the recorded
 * stat, so the stat must be the one the digest was computed from, never a later one. A file
 * edited in between then differs from its row, is hashed on the next scan, and the edit syncs.
 * Pairing a later stat with an earlier digest would hide that edit for good.
 */
import { statSync } from 'node:fs';
import { stat } from 'node:fs/promises';
import path from 'node:path';

import type { RemoteNode } from '../remote/interface.js';
import { sha1File } from '../remote/transfer.js';
import type { BaselineRow } from '../state/baseline.ts';

/** A local stat and the digest of the content the file held with that stat (null: none known). */
export interface LocalEvidence {
  dev: number;
  ino: number;
  size: number;
  mtimeMs: number;
  sha1: string | null;
}

export function evidenceRow(relPath: string, kind: 'file' | 'dir', node: RemoteNode, local: LocalEvidence, syncedAt: number): BaselineRow {
  return {
    relPath,
    kind,
    localDev: local.dev,
    localIno: local.ino,
    localSize: kind === 'file' ? local.size : 0,
    localMtimeMs: local.mtimeMs,
    localSha1: kind === 'file' ? local.sha1 : null,
    nodeUid: node.uid,
    parentUid: node.parentUid ?? null,
    remoteName: node.name,
    revisionUid: node.revisionUid ?? null,
    remoteSha1: node.claimedSha1 ?? null,
    syncedAt,
  };
}

/** The current stat, vouching for no content: a folder, or a file whose digest is unknown. */
export function statEvidence(root: string, relPath: string): LocalEvidence {
  const st = statSync(path.join(root, relPath));
  return { dev: st.dev, ino: st.ino, size: st.size, mtimeMs: st.mtimeMs, sha1: null };
}

/** Hash a file and keep the stat it had throughout; null if it changed while being hashed. */
export async function hashedEvidence(absPath: string): Promise<LocalEvidence | null> {
  const before = await stat(absPath);
  const { sha1, size } = await sha1File(absPath);
  const after = await stat(absPath);
  if (size !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ino !== before.ino) return null;
  return { dev: before.dev, ino: before.ino, size: before.size, mtimeMs: before.mtimeMs, sha1 };
}

/**
 * Evidence for an item moved to `relPath`: a file keeps its previous row's stat and digest. A
 * rename keeps the inode, size and mtime, so an untouched file still matches its row; one
 * edited since no longer does and is hashed again. The move vouches for the new location
 * only, not for the content.
 */
export function movedEvidence(previous: BaselineRow | null, root: string, relPath: string): LocalEvidence {
  const st = statSync(path.join(root, relPath));
  if (previous?.kind === 'file' && previous.localSha1 !== null && st.isFile()) {
    return { dev: previous.localDev, ino: previous.localIno, size: previous.localSize, mtimeMs: previous.localMtimeMs, sha1: previous.localSha1 };
  }
  return { dev: st.dev, ino: st.ino, size: st.size, mtimeMs: st.mtimeMs, sha1: null };
}
