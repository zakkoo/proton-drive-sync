import { renameSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import { SyncHarness } from '../testing/harness.js';

/**
 * A baseline row vouches that both sides hold the same content: the engine skips hashing a
 * file whose stat matches its row (needsDigest), so a row that pairs a file's current stat with
 * the digest of other content hides an edit for good. These journeys use the engine's own
 * hashing rule (the harness default) and settle, then require every row to match the bytes
 * on both sides (assertBaselineConsistent).
 *
 * Known defect: each journey below ends with a row that pairs the file's current stat with the
 * digest of older content, so the edit is never synced. They are marked `it.fails` until the
 * baseline commits are built from verified evidence; a fix makes them fail as written, and they
 * become plain `it` tests then.
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
  it.fails('after a local rename plus an edit, both sides end up with the edit', async () => {
    await syncedFile('one');
    renameSync(path.join(h.root, 'a.txt'), path.join(h.root, 'b.txt'));
    writeFileSync(path.join(h.root, 'b.txt'), 'two, edited after the rename');
    await h.settle();
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('b.txt')).toBe('two, edited after the rename');
  });

  it.fails('an edit made after a rename was planned still reaches the other side', async () => {
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

  it.fails('an edit made while a lost upload response is checked still reaches the other side', async () => {
    await syncedFile('one');
    writeFileSync(path.join(h.root, 'a.txt'), 'two');
    const lost = loseUploadResponseThenEdit('uploadNewRevision', 'a.txt', 'three, edited before the retry');
    await h.settle();
    expect(lost.edited()).toBe(true);
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('a.txt')).toBe('three, edited before the retry');
  });

  it.fails('an edit made while a lost new-file upload response is checked still reaches the other side', async () => {
    await syncedFile('one');
    h.write('new.txt', 'new');
    const lost = loseUploadResponseThenEdit('uploadNewFile', 'new.txt', 'new, edited before the retry');
    await h.settle();
    expect(lost.edited()).toBe(true);
    h.assertBaselineConsistent();
    expect(h.remoteFiles().get('new.txt')).toBe('new, edited before the retry');
  });
});
