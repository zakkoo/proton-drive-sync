/**
 * Verified transfers on top of RemoteDrive.
 *
 * Download: bytes stream into a temporary file while SHA1 is computed; the
 * digest is compared with the remote claim before the file is handed over.
 * Upload: the local digest and size are computed first, sent with the upload,
 * and the remote node is re-read afterwards to confirm the active revision
 * carries exactly that digest and size.
 */
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream, openSync } from 'node:fs';
import { open, stat, unlink, type FileHandle } from 'node:fs/promises';
import { Writable } from 'node:stream';

import { RemoteError, type RemoteDrive, type RemoteNode, type RemoteUploadSource, type TransferOptions } from './interface.js';

export interface VerifiedDownload {
  sha1: string;
  size: number;
  /** False when the remote carried no digest claim; the local sha1 is then the reference. */
  verifiedAgainstClaim: boolean;
}

export interface VerifiedUpload {
  nodeUid: string;
  revisionUid: string;
  sha1: string;
  size: number;
  /** The local file's stat while it was hashed and uploaded; `sha1` is the digest of that content. */
  local: { dev: number; ino: number; size: number; mtimeMs: number };
}

/** Hex SHA1 of a file, streamed. */
export async function sha1File(filePath: string): Promise<{ sha1: string; size: number }> {
  const hash = createHash('sha1');
  let size = 0;
  for await (const chunk of createReadStream(filePath)) {
    const buf = chunk as Buffer;
    hash.update(buf);
    size += buf.length;
  }
  return { sha1: hash.digest('hex'), size };
}

/**
 * Download `node` into `tempPath` (created exclusively; must not exist) and
 * verify it. On any failure the temp file is removed and nothing else changes.
 */
export async function downloadVerified(remote: RemoteDrive, node: RemoteNode, tempPath: string, options: TransferOptions = {}): Promise<VerifiedDownload> {
  if (node.type !== 'file') throw new RemoteError(`Cannot download ${node.uid}: not a file`, 'validation', false);
  if (node.isProtonDocument) throw new RemoteError(`Cannot download ${node.uid}: Proton document without file content`, 'unsupported', false);

  const hash = createHash('sha1');
  let size = 0;
  // Open exclusively and synchronously so EEXIST surfaces immediately and the
  // file exists before any cleanup path could try to remove it.
  const fd = openSync(tempPath, 'wx', 0o600);
  // The stream owns the descriptor from here on and closes it exactly once.
  const file = createWriteStream(tempPath, { fd, autoClose: true });
  file.on('error', () => undefined);
  const closed = new Promise<void>((resolve) => {
    file.once('close', () => { resolve(); });
  });
  const sink = Writable.toWeb(
    new Writable({
      write(chunk: Buffer, _enc, cb) {
        hash.update(chunk);
        size += chunk.length;
        if (!file.write(chunk)) file.once('drain', () => { cb(); });
        else cb();
      },
      final(cb) {
        file.end(() => { cb(); });
      },
      destroy(err, cb) {
        file.destroy();
        cb(err);
      },
    }),
  ) as WritableStream<Uint8Array>;

  try {
    await remote.downloadToStream(node.uid, sink, options);
    if (!file.closed) {
      await new Promise<void>((resolve, reject) => {
        file.close((e) => {
          if (e) reject(e);
          else resolve();
        });
      });
    }
    const sha1 = hash.digest('hex');
    if (node.claimedSize !== undefined && node.claimedSize !== size) {
      throw new RemoteError(`Download of ${node.uid} has ${size} bytes but the remote claims ${node.claimedSize}`, 'integrity', false, {
        details: { expectedSize: node.claimedSize, actualSize: size },
      });
    }
    if (node.claimedSha1 !== undefined && node.claimedSha1 !== sha1) {
      throw new RemoteError(`Download of ${node.uid} has digest ${sha1} but the remote claims ${node.claimedSha1}`, 'integrity', false, {
        details: { expectedSha1: node.claimedSha1, actualSha1: sha1 },
      });
    }
    return { sha1, size, verifiedAgainstClaim: node.claimedSha1 !== undefined };
  } catch (error) {
    file.destroy();
    await closed;
    await unlink(tempPath).catch(() => undefined);
    throw error;
  }
}

const UPLOAD_CHUNK = 1024 * 1024;

/**
 * Pull-based web stream over a file handle. Node's `Readable.toWeb` over an
 * fs stream can close the descriptor twice (EBADF) when the consumer finishes
 * and cancels; this owns the handle and closes it exactly once.
 */
function openFileStream(filePath: string): ReadableStream<Uint8Array> {
  let handle: FileHandle | null = null;
  let position = 0;
  let finished = false;
  const finish = async (): Promise<void> => {
    if (finished) return;
    finished = true;
    const h = handle;
    handle = null;
    if (h !== null) await h.close();
  };
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        if (finished) return;
        handle ??= await open(filePath, 'r');
        const buf = new Uint8Array(UPLOAD_CHUNK);
        const { bytesRead } = await handle.read(buf, 0, UPLOAD_CHUNK, position);
        if (bytesRead === 0) {
          await finish();
          controller.close();
          return;
        }
        position += bytesRead;
        controller.enqueue(buf.subarray(0, bytesRead));
      },
      cancel: () => finish(),
    },
    { highWaterMark: 2 },
  );
}

export function fileUploadSource(filePath: string, sha1: string, size: number, modifiedAt: Date, mediaType?: string): RemoteUploadSource {
  return {
    size,
    sha1,
    modifiedAt,
    ...(mediaType !== undefined ? { mediaType } : {}),
    open: () => openFileStream(filePath),
  };
}

export type UploadTarget = { kind: 'new'; parentUid: string; name: string } | { kind: 'revision'; nodeUid: string };

/**
 * Upload a local file and confirm the remote active revision matches its
 * digest and size. The file's mtime and size are re-checked after hashing so a
 * file modified during the upload is rejected rather than uploaded torn.
 */
export async function uploadVerified(remote: RemoteDrive, target: UploadTarget, filePath: string, options: TransferOptions & { mediaType?: string } = {}): Promise<VerifiedUpload> {
  const before = await stat(filePath);
  const { sha1, size } = await sha1File(filePath);
  if (size !== before.size) throw new RemoteError(`${filePath} changed while hashing`, 'validation', true);

  const source = fileUploadSource(filePath, sha1, size, before.mtime, options.mediaType);
  const result = target.kind === 'new'
    ? await remote.uploadNewFile(target.parentUid, target.name, source, options)
    : await remote.uploadNewRevision(target.nodeUid, source, options);

  const after = await stat(filePath).catch(() => undefined);
  if (after?.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ino !== before.ino) {
    throw new RemoteError(`${filePath} changed during upload; the uploaded revision ${result.revisionUid} may be stale`, 'validation', true, {
      details: { nodeUid: result.nodeUid, revisionUid: result.revisionUid },
    });
  }

  const node = await remote.getNode(result.nodeUid);
  if (node === null) throw new RemoteError(`Uploaded node ${result.nodeUid} cannot be read back`, 'integrity', false);
  const problems: string[] = [];
  if (node.revisionUid !== result.revisionUid) problems.push(`active revision is ${node.revisionUid ?? 'none'}, expected ${result.revisionUid}`);
  if (node.claimedSha1 !== sha1) problems.push(`remote digest is ${node.claimedSha1 ?? 'none'}, expected ${sha1}`);
  if (node.claimedSize !== size) problems.push(`remote size is ${node.claimedSize ?? 'none'}, expected ${size}`);
  if (problems.length > 0) {
    throw new RemoteError(`Post-upload verification failed for ${result.nodeUid}: ${problems.join('; ')}`, 'integrity', false, {
      details: { nodeUid: result.nodeUid, revisionUid: result.revisionUid, sha1, size },
    });
  }
  return { nodeUid: result.nodeUid, revisionUid: result.revisionUid, sha1, size, local: { dev: before.dev, ino: before.ino, size: before.size, mtimeMs: before.mtimeMs } };
}
