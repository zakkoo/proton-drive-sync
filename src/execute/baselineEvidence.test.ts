import { renameSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import { SyncHarness } from '../testing/harness.js';
import { SimulatedCrashError } from './types.js';

/**
 * A baseline row vouches that both sides hold the same content: the engine skips hashing a
 * file whose stat matches its row (needsDigest), so a row that pairs a file's current stat with
 * the digest of other content hides an edit for good. These journeys use the engine's own
 * hashing rule (the harness default) and settle, then require every row to match the bytes
 * on both sides (assertBaselineConsistent).
 *
 * In each journey the user edits a file between the moment its digest is known and the moment
 * the baseline row is written. A row must then keep the stat that the digest belongs to, so the
 * next scan sees the edit.
 */

let h: SyncHarness;
afterEach(() => {
  h.dispose();
});

/** a.txt synced on both sides with `content`. */
async function syncedFile(content: string, options: Parameters<typeof SyncHarness.create>[0] = {}): Promise<void> {
  h = SyncHarness.create(options);
  h.write('a.txt', content);
  expect((await h.settle()).operations).toEqual([]);
  h.assertBaselineConsistent();
}

describe('a baseline row vouches only for content both sides hold', () => {
  it('after a local rename plus an edit, both sides end up with the edit', async () => {
    await syncedFile('one');
    renameSync(path.join(h.root, 'a.txt'), path.join(h.root, 'b.txt'));
    writeFileSync(path.join(h.root, 'b.txt'), 'two, edited after the rename');
    await h.settle();
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('b.txt')).toBe('two, edited after the rename');
  });

  it('an edit made after a rename was planned still reaches the other side', async () => {
    await syncedFile('one');
    renameSync(path.join(h.root, 'a.txt'), path.join(h.root, 'b.txt'));
    const plan = await h.plan();
    // The user edits the renamed file before the planned move runs.
    writeFileSync(path.join(h.root, 'b.txt'), 'two, edited after planning');
    await h.execute(plan);
    await h.settle();
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('b.txt')).toBe('two, edited after planning');
  });

  /** The next upload of `relPath` lands on Proton, but its response is lost; the user edits the file right then. */
  function loseUploadResponseThenEdit(method: 'uploadNewRevision' | 'uploadNewFile', relPath: string, edit: string): { edited: () => boolean } {
    let edited = false;
    const fake = h.fake as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>;
    const upload = fake[method]?.bind(h.fake);
    if (upload === undefined) throw new Error(`no ${method}`);
    h.fake.injectFault('upload', { kind: 'unknown_outcome' });
    fake[method] = async (...args: unknown[]) => {
      try {
        return await upload(...args);
      } catch (error) {
        if (!edited) {
          edited = true;
          writeFileSync(path.join(h.root, relPath), edit);
        }
        throw error;
      }
    };
    return { edited: () => edited };
  }

  it('an edit made while a lost upload response is checked still reaches the other side', async () => {
    await syncedFile('one');
    writeFileSync(path.join(h.root, 'a.txt'), 'two');
    const lost = loseUploadResponseThenEdit('uploadNewRevision', 'a.txt', 'three, edited before the retry');
    await h.settle();
    expect(lost.edited()).toBe(true);
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('a.txt')).toBe('three, edited before the retry');
  });

  it('an edit made while a lost new-file upload response is checked still reaches the other side', async () => {
    await syncedFile('one');
    h.write('new.txt', 'new');
    const lost = loseUploadResponseThenEdit('uploadNewFile', 'new.txt', 'new, edited before the retry');
    await h.settle();
    expect(lost.edited()).toBe(true);
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('new.txt')).toBe('new, edited before the retry');
  });

  /** Crash just before `kind` commits, edit `relPath`, then restart and recover. */
  async function crashBeforeCommitThenEdit(kind: string, relPath: string, edit: string): Promise<void> {
    h.reopen({
      hooks: {
        beforeStep: (s, op) => {
          if (s === 'commit' && op.kind === kind) throw new SimulatedCrashError(`commit of ${kind}`);
        },
      },
    });
    await expect(h.execute(await h.plan())).rejects.toBeInstanceOf(SimulatedCrashError);
    writeFileSync(path.join(h.root, relPath), edit);
    h.reopen();
    const report = await h.recover();
    expect(report.completed).toBe(1);
  }

  it('recovering a local rename that crashed before its commit keeps an edit made meanwhile', async () => {
    await syncedFile('one');
    renameSync(path.join(h.root, 'a.txt'), path.join(h.root, 'b.txt'));
    await crashBeforeCommitThenEdit('move_remote', 'b.txt', 'two, edited after the crash');
    await h.settle();
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('b.txt')).toBe('two, edited after the crash');
  });

  it('recovering a rename from Proton that crashed before its commit keeps an edit made meanwhile', async () => {
    await syncedFile('one');
    await h.fake.rename(h.remotePathToUid('a.txt') ?? '', 'b.txt');
    await crashBeforeCommitThenEdit('move_local', 'b.txt', 'two, edited after the crash');
    await h.settle();
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('b.txt')).toBe('two, edited after the crash');
  });
});
