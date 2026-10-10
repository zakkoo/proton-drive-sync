import { rmSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { BaselineRepo } from '../state/baseline.js';
import { EngineHarness } from '../testing/engineHarness.js';
import { ControlClient, ControlServer } from './control.js';

let h: EngineHarness;
beforeEach(() => {
  h = EngineHarness.create();
});
afterEach(async () => {
  await h.dispose();
});

describe('SyncEngine', () => {
  it('starts, scans, syncs both directions through the watcher and the event feed, and rests idle', async () => {
    h.write('local.txt', 'L');
    h.fake.seedFile(h.remoteRootUid, 'remote.txt', 'R');
    await h.start();
    await h.waitForConvergence();
    expect(h.states().slice(0, 2)).toEqual(['scanning', 'syncing']);
    expect(h.states().at(-1)).toBe('idle');
    expect(h.localFiles().get('remote.txt')).toBe('R');
    expect(h.remoteFiles().get('local.txt')).toBe('L');
    // A live local change is picked up by the watcher.
    h.write('later.txt', 'later');
    await h.waitForConvergence();
    expect(h.remoteFiles().get('later.txt')).toBe('later');
    // A remote change arrives through the event feed.
    h.fake.seedFile(h.remoteRootUid, 'fromfeed.txt', 'F');
    await h.waitForConvergence();
    expect(h.localFiles().get('fromfeed.txt')).toBe('F');
    const status = h.bundle?.engine.getStatus();
    expect(status?.lastSuccessfulSyncAt).not.toBeNull();
    expect(status?.counts.baseline).toBe(4);
    expect(status?.progress).toBeNull();
  });

  it('counts finished file transfers in the run and drops the fraction when idle', async () => {
    h.fake.seedFile(h.remoteRootUid, 'a.txt', 'A');
    h.fake.seedFile(h.remoteRootUid, 'b.txt', 'B');
    h.fake.seedFile(h.remoteRootUid, 'c.txt', 'C');
    await h.start();
    await h.waitForConvergence();
    const during = h.statuses.filter((s) => s.state === 'syncing' && s.progress !== null);
    expect(during.length).toBeGreaterThan(0);
    expect(during.every((s) => s.progress?.total === 3)).toBe(true);
    const dones = during.map((s) => s.progress?.done ?? 0);
    expect(Math.max(...dones)).toBeGreaterThan(Math.min(...dones));
    const climbed = during.find((s) => s.progress?.done === 1);
    expect(climbed?.pending.downloads).toBe(3);
    expect(h.bundle?.engine.getStatus().state).toBe('idle');
    expect(h.bundle?.engine.getStatus().progress).toBeNull();
  });

  it('records files copied by a finished check, and a failed check keeps that record', async () => {
    h.fake.seedFile(h.remoteRootUid, 'a.txt', 'A');
    await h.start();
    await h.waitForConvergence();
    expect(h.statuses.some((s) => s.lastRunFilesCopied === 1)).toBe(true);

    const before = h.live.engine.getStatus();
    expect(before.lastFullSyncAt).not.toBeNull();
    await h.live.engine.syncNow();
    await h.waitFor(['idle']);
    const empty = h.live.engine.getStatus();
    expect(empty.lastRunFilesCopied).toBe(0);
    expect(empty.summaryLines).toContainEqual(expect.stringContaining('no files copied'));
    expect(empty.lastFullSyncAt).toBe(before.lastFullSyncAt);
    const keptAt = empty.lastSuccessfulSyncAt;
    expect(keptAt).not.toBeNull();

    // A check that wants to upload, but cannot, must not replace the last finished check.
    h.live.engine.pause();
    await h.waitFor(['paused']);
    h.fake.seedPermanentDelete(h.remoteRootUid);
    h.write('bad.txt', 'B');
    const started = Date.now();
    while (Date.now() - started < 3000 && h.live.engine.getStatus().counts.localFiles < 2) await new Promise((r) => setTimeout(r, 30));
    expect(h.live.engine.getStatus().counts.localFiles).toBeGreaterThanOrEqual(2);
    h.live.engine.resume();
    await h.waitFor(['error']);
    const failed = h.live.engine.getStatus();
    expect(failed.lastSuccessfulSyncAt).toBe(keptAt);
    expect(failed.lastRunFilesCopied).toBe(0);
    expect(failed.lastFullSyncAt).toBe(before.lastFullSyncAt);
  });

  it('splits paired files, Proton documents, and files that exist on only one side', async () => {
    h.write('keep.txt', 'K');
    h.write('gone.txt', 'G');
    await h.start();
    await h.waitForConvergence();
    h.live.engine.pause();
    await h.waitFor(['paused']);

    unlinkSync(path.join(h.root, 'gone.txt'));
    h.write('only-local.txt', 'L');
    h.fake.seedFile(h.remoteRootUid, 'extra.txt', 'E');
    h.fake.seedProtonDocument(h.remoteRootUid, 'Agenda');

    const start = Date.now();
    let status = h.live.engine.getStatus();
    while (Date.now() - start < 5000) {
      status = h.live.engine.getStatus();
      const c = status.counts;
      if (c.localFiles === 2 && c.remoteFiles === 4 && c.protonDocuments === 1 && c.onlyLocal === 1 && c.onlyRemote === 1) break;
      await new Promise((r) => setTimeout(r, 30));
    }
    expect(status.counts).toMatchObject({
      localFiles: 2,
      remoteFiles: 4,
      pairedFiles: 2,
      protonDocuments: 1,
      onlyLocal: 1,
      onlyRemote: 1,
    });
    expect(status.protonDocumentPaths).toEqual(['Agenda']);
    // Each document carries its last modified time.
    expect(Object.keys(status.protonDocumentModifiedAt)).toEqual(['Agenda']);
    expect(status.protonDocumentModifiedAt['Agenda']).toBeGreaterThan(0);
    expect(status.summaryLines).toEqual(expect.arrayContaining([
      'Files: 2 on this computer, 4 on Proton, 2 in sync',
      'Only on this computer: 1 file',
      'Only on Proton: 1 file',
      'Proton documents: 1 on Proton only (Docs and Sheets stay in the browser)',
    ]));
  });

  it('pause stops syncing and resume picks up the backlog; startPaused starts paused', async () => {
    await h.start();
    await h.waitFor(['idle']);
    h.bundle?.engine.pause();
    await h.waitFor(['paused']);
    h.write('while-paused.txt', 'P');
    await new Promise((r) => setTimeout(r, 400));
    expect(h.remoteFiles().has('while-paused.txt')).toBe(false);
    h.bundle?.engine.resume();
    await h.waitForConvergence();
    expect(h.remoteFiles().get('while-paused.txt')).toBe('P');

    const paused = EngineHarness.create({ startPaused: true });
    try {
      await paused.start();
      expect(paused.bundle?.engine.getStatus().state).toBe('paused');
    } finally {
      await paused.dispose();
    }
  });

  it('holds a mass deletion for confirmation, keeps syncing unaffected items, and runs it after confirm', async () => {
    for (let i = 0; i < 10; i++) h.write(`f${String(i)}.txt`, String(i));
    h.config = { ...h.config, safety: { ...h.config.safety, brakeMaxChanges: 3 } };
    await h.start();
    await h.waitForConvergence();
    for (let i = 0; i < 6; i++) await h.fake.trash([h.remotePathToUid(`f${String(i)}.txt`) ?? '']);
    h.write('unaffected.txt', 'U');
    const status = await h.waitFor(['awaiting_confirmation']);
    expect(status.attention.heldPlan?.affected).toHaveLength(6);
    // Unaffected upload runs even though the deletes are held.
    for (let i = 0; i < 100 && !h.remoteFiles().has('unaffected.txt'); i++) await new Promise((r) => setTimeout(r, 30));
    expect(h.remoteFiles().get('unaffected.txt')).toBe('U');
    expect(h.localFiles().has('f0.txt')).toBe(true);
    const id = status.attention.heldPlan?.id ?? '';
    await h.bundle?.engine.confirmHeldPlan(id);
    await h.waitForConvergence();
    expect(h.localFiles().has('f0.txt')).toBe(false);
    expect(h.bundle?.recycle.list().filter((r) => r.kind === 'file')).toHaveLength(6);
  });

  it('rejecting a held plan discards it and the engine returns to rest', async () => {
    for (let i = 0; i < 6; i++) h.write(`g${String(i)}.txt`, String(i));
    h.config = { ...h.config, safety: { ...h.config.safety, brakeMaxChanges: 2 } };
    await h.start();
    await h.waitForConvergence();
    for (let i = 0; i < 4; i++) await h.fake.trash([h.remotePathToUid(`g${String(i)}.txt`) ?? '']);
    const status = await h.waitFor(['awaiting_confirmation']);
    const affected = h.bundle?.engine.rejectHeldPlan(status.attention.heldPlan?.id ?? '') ?? [];
    expect(affected).toHaveLength(4);
    expect(h.localFiles().size).toBe(6);
    expect(['idle', 'attention', 'scanning', 'awaiting_confirmation']).toContain(h.bundle?.engine.getStatus().state);
  });

  it('surfaces conflicts as attention, resolves them through the engine, and returns to idle', async () => {
    h.write('doc.md', 'base');
    await h.start();
    await h.waitForConvergence();
    h.bundle?.engine.pause();
    await h.waitFor(['paused']);
    h.write('doc.md', 'local');
    h.fake.seedRevision(h.remotePathToUid('doc.md') ?? '', 'remote');
    h.bundle?.engine.resume();
    const status = await h.waitFor(['attention']);
    expect(status.attention.conflicts).toBe(1);
    const conflicts = h.bundle?.controlTarget.listConflicts() ?? [];
    expect(conflicts[0]?.kind).toBe('content');
    // The control target adds the remote file's current modified time, which the record lacks.
    const remote = conflicts[0]?.remote as Record<string, unknown> | undefined;
    expect(typeof remote?.['mtimeMs']).toBe('number');
    expect(remote?.['mtimeMs']).toBeGreaterThan(0);
    await h.waitForConvergence();
    expect(h.localFiles().size).toBe(2);
    await h.bundle?.engine.resolveConflict(conflicts[0]?.id ?? 0, 'keep_both');
    await h.waitFor(['idle']);
    expect(h.bundle?.controlTarget.listConflicts()).toEqual([]);
  });

  it('resolves keep_local through the engine: every operation completes and the conflict closes', async () => {
    h.write('doc.md', 'base');
    await h.start();
    await h.waitForConvergence();
    h.bundle?.engine.pause();
    await h.waitFor(['paused']);
    h.write('doc.md', 'local');
    h.fake.seedRevision(h.remotePathToUid('doc.md') ?? '', 'remote');
    h.bundle?.engine.resume();
    await h.waitFor(['attention']);
    await h.waitForConvergence();
    const conflict = h.bundle?.controlTarget.listConflicts()[0];
    // keep_local needs both versions synced (it refuses until then): wait for the original's row.
    const baseline = new BaselineRepo(h.live.store);
    for (let i = 0; i < 200 && baseline.byPath('doc.md') === null; i++) await new Promise((r) => setTimeout(r, 20));
    expect(baseline.byPath('doc.md')).not.toBeNull();

    await h.bundle?.engine.resolveConflict(conflict?.id ?? 0, 'keep_local');
    await h.waitForConvergence();

    expect(h.bundle?.controlTarget.listConflicts()).toEqual([]);
    expect([...h.localFiles().entries()]).toEqual([['doc.md', 'local']]);
    expect(h.remoteFiles().get('doc.md')).toBe('local');
  });

  /**
   * Proton's event stream under the test's control: while `hold()` is in effect, polls return no
   * events, as when events lag behind the engine's own changes under load.
   */
  function controlEvents(): { hold: () => void; release: () => void; heldPolls: () => number } {
    const fake = h.fake as unknown as { iterateEvents: (...args: unknown[]) => AsyncIterable<unknown> };
    const iterate = fake.iterateEvents.bind(h.fake);
    let held = false;
    let heldPolls = 0;
    // An event stream that ends at once: no events this poll.
    const none: AsyncIterable<never> = { [Symbol.asyncIterator]: () => ({ next: () => Promise.resolve({ done: true as const, value: undefined }) }) };
    fake.iterateEvents = (...args: unknown[]) => {
      if (!held) return iterate(...args);
      heldPolls++;
      return none;
    };
    return { hold: () => { held = true; }, release: () => { held = false; }, heldPolls: () => heldPolls };
  }

  /** A content conflict on doc.md whose copy is synced and known to the engine's view of Proton. */
  async function contentConflictOnDoc(): Promise<{ id: number; hold: () => void; release: () => void; heldPolls: () => number }> {
    h.write('doc.md', 'base');
    await h.start();
    await h.waitForConvergence();
    const fake = h.fake as unknown as { calls: { op: string }[] };
    const events = controlEvents();
    h.bundle?.engine.pause();
    await h.waitFor(['paused']);
    h.write('doc.md', 'local');
    h.fake.seedRevision(h.remotePathToUid('doc.md') ?? '', 'remote');
    h.bundle?.engine.resume();
    await h.waitFor(['attention']);
    await h.waitForConvergence();
    const baseline = new BaselineRepo(h.live.store);
    for (let i = 0; i < 200 && baseline.byPath('doc.md') === null; i++) await new Promise((r) => setTimeout(r, 20));
    // Let the event stream catch up on the copy's upload (two polls after it).
    const uploadedAt = fake.calls.map((c) => c.op).lastIndexOf('upload');
    for (let i = 0; i < 250 && fake.calls.slice(uploadedAt + 1).filter((c) => c.op === 'events').length < 2; i++) await new Promise((r) => setTimeout(r, 20));
    const conflict = h.bundle?.controlTarget.listConflicts()[0];
    expect(conflict?.kind).toBe('content');
    return { id: conflict?.id ?? 0, ...events };
  }

  /** Pause the next rename on Proton until `resume()`; `reached` settles when it is called. */
  function pauseNextRename(): { reached: Promise<void>; resume: () => void } {
    const fake = h.fake as unknown as { rename: (uid: string, name: string) => Promise<unknown> };
    const rename = fake.rename.bind(h.fake);
    let reachedNow!: () => void;
    let resumeNow!: () => void;
    const reached = new Promise<void>((r) => { reachedNow = r; });
    const resumed = new Promise<void>((r) => { resumeNow = r; });
    fake.rename = async (uid: string, name: string) => {
      fake.rename = rename;
      reachedNow();
      await resumed;
      return rename(uid, name);
    };
    return { reached, resume: () => { resumeNow(); } };
  }

  /** After keep_local and convergence: one file, the local version, on both sides, no conflict left. */
  async function expectLocalKept(): Promise<void> {
    await h.waitForConvergence();
    expect(h.bundle?.controlTarget.listConflicts()).toEqual([]);
    expect([...h.localFiles().entries()]).toEqual([['doc.md', 'local']]);
    expect(h.remoteFiles().get('doc.md')).toBe('local');
  }

  it('keep_local is not undone by the next cycle while Proton has not yet reported the resolution', async () => {
    const conflict = await contentConflictOnDoc();
    conflict.hold();
    await h.bundle?.engine.resolveConflict(conflict.id, 'keep_local');
    // The next cycle plans from the engine's view of Proton, which the lagging events have not
    // updated: it must already reflect the trash and the rename the resolution made itself.
    await h.bundle?.engine.runCycle('test: right after the resolution');
    expect(h.bundle?.controlTarget.listConflicts()).toEqual([]);
    expect([...h.localFiles().entries()]).toEqual([['doc.md', 'local']]);
    conflict.release();
    await expectLocalKept();
  });

  it('a cycle requested while a conflict is being resolved waits for the resolution to finish', async () => {
    const conflict = await contentConflictOnDoc();
    // Pause the resolution half way, at its rename of the copy on Proton (after the original is
    // trashed and recycled, before the copy takes its place).
    const rename = pauseNextRename();
    const resolving = h.bundle?.engine.resolveConflict(conflict.id, 'keep_local');
    await rename.reached;
    const started = h.bundle?.engine.getStatus().lastCycleAt;
    const cycle = h.bundle?.engine.runCycle('test: during the resolution');
    await new Promise((r) => setTimeout(r, 300));
    // The cycle has not started: it would plan from a half-done resolution.
    expect(h.bundle?.engine.getStatus().lastCycleAt).toBe(started);
    rename.resume();
    await resolving;
    await cycle;
    expect(h.bundle?.engine.getStatus().lastCycleAt).not.toBe(started);
    await expectLocalKept();
  });

  it('stopping waits for a resolution in progress, and refuses one asked for after it began', async () => {
    const conflict = await contentConflictOnDoc();
    const rename = pauseNextRename();
    let resolutionSettled = false;
    const resolving = (h.bundle?.engine.resolveConflict(conflict.id, 'keep_local') ?? Promise.resolve()).finally(() => { resolutionSettled = true; });
    await rename.reached;
    let stopSettled = false;
    const stopping = (h.bundle?.engine.stop() ?? Promise.resolve()).finally(() => { stopSettled = true; });
    await new Promise((r) => setTimeout(r, 300));
    expect(stopSettled).toBe(false);
    rename.resume();
    await resolving.catch(() => undefined);
    await stopping;
    expect(resolutionSettled).toBe(true);
    await expect(h.bundle?.engine.resolveConflict(conflict.id, 'keep_both')).rejects.toThrow(/stopping/);
  });

  it('the engine knows of a file it just uploaded before Proton reports it, and leaves it in place', async () => {
    await h.start();
    await h.waitForConvergence();
    const events = controlEvents();
    events.hold();
    h.write('new.txt', 'N');
    for (let i = 0; i < 200 && !h.remoteFiles().has('new.txt'); i++) await new Promise((r) => setTimeout(r, 20));
    await h.waitForConvergence();
    // Polls keep completing after the upload, without its event (so the view looks current).
    const polled = events.heldPolls();
    for (let i = 0; i < 250 && events.heldPolls() < polled + 2; i++) await new Promise((r) => setTimeout(r, 20));
    // The view of Proton has not heard of the upload from its event; the engine made it, so it knows.
    await h.bundle?.engine.runCycle('test: right after the upload');
    // Its view of Proton (which the status counts) already has the file.
    expect(h.bundle?.engine.getStatus().counts.remoteFiles).toBe(1);
    expect(h.localFiles().get('new.txt')).toBe('N');
    expect(h.bundle?.recycle.list()).toEqual([]);
    events.release();
    await h.waitForConvergence();
    expect(h.localFiles().get('new.txt')).toBe('N');
    expect(h.remoteFiles().get('new.txt')).toBe('N');
  });

  it('confirming a held plan while a cycle runs takes the plan only when the cycle is done', async () => {
    for (let i = 0; i < 10; i++) h.write(`f${String(i)}.txt`, String(i));
    h.config = { ...h.config, safety: { ...h.config.safety, brakeMaxChanges: 3 } };
    await h.start();
    await h.waitForConvergence();
    for (let i = 0; i < 6; i++) await h.fake.trash([h.remotePathToUid(`f${String(i)}.txt`) ?? '']);
    const status = await h.waitFor(['awaiting_confirmation']);
    const id = status.attention.heldPlan?.id ?? '';
    // A cycle that is running: paused in the upload of a new file.
    let reached!: () => void;
    let resume!: () => void;
    const atUpload = new Promise<void>((r) => { reached = r; });
    const resumed = new Promise<void>((r) => { resume = r; });
    h.fake.beforeUploadCommit = async () => {
      h.fake.beforeUploadCommit = undefined;
      reached();
      await resumed;
    };
    h.write('slow.txt', 'S');
    await atUpload;
    const confirming = h.bundle?.engine.confirmHeldPlan(id);
    await new Promise((r) => setTimeout(r, 300));
    // Still held under the same id: the confirmation waits for the cycle instead of taking the plan.
    expect(h.bundle?.engine.getStatus().attention.heldPlan?.id).toBe(id);
    resume();
    await confirming;
    await h.waitForConvergence();
    expect(h.bundle?.engine.getStatus().attention.heldPlan).toBeNull();
    expect(h.localFiles().has('f0.txt')).toBe(false);
    expect(h.bundle?.recycle.list().filter((r) => r.kind === 'file')).toHaveLength(6);
    expect(h.remoteFiles().get('slow.txt')).toBe('S');
  });

  it('an upload whose response was lost is still known to the engine before Proton reports it', async () => {
    await h.start();
    await h.waitForConvergence();
    const events = controlEvents();
    events.hold();
    // The upload lands on Proton, but the engine sees a timeout and finds it there on retry.
    h.fake.injectFault('upload', { kind: 'unknown_outcome' });
    h.write('new.txt', 'N');
    for (let i = 0; i < 200 && !h.remoteFiles().has('new.txt'); i++) await new Promise((r) => setTimeout(r, 20));
    await h.waitForConvergence();
    const polled = events.heldPolls();
    for (let i = 0; i < 250 && events.heldPolls() < polled + 2; i++) await new Promise((r) => setTimeout(r, 20));
    await h.bundle?.engine.runCycle('test: right after the recovered upload');
    expect(h.bundle?.engine.getStatus().counts.remoteFiles).toBe(1);
    expect(h.localFiles().get('new.txt')).toBe('N');
    expect(h.bundle?.recycle.list()).toEqual([]);
    events.release();
    await h.waitForConvergence();
    expect(h.localFiles().get('new.txt')).toBe('N');
    expect(h.remoteFiles().get('new.txt')).toBe('N');
  });

  it('keep_local is not undone when the response to its rename on Proton was lost', async () => {
    const conflict = await contentConflictOnDoc();
    conflict.hold();
    // The rename lands on Proton, but the engine sees a timeout and finds it done on retry.
    h.fake.injectFault('rename', { kind: 'unknown_outcome' });
    await h.bundle?.engine.resolveConflict(conflict.id, 'keep_local');
    await h.bundle?.engine.runCycle('test: right after the resolution');
    expect(h.bundle?.controlTarget.listConflicts()).toEqual([]);
    expect([...h.localFiles().entries()]).toEqual([['doc.md', 'local']]);
    conflict.release();
    await expectLocalKept();
  });

  it('quarantines a verification failure and re-reconciles after release', async () => {
    await h.start();
    await h.waitFor(['idle']);
    h.fake.injectFault('upload', { kind: 'mismatch_upload' });
    h.write('bad.txt', 'B');
    const status = await h.waitFor(['attention']);
    expect(status.attention.quarantined).toBe(1);
    const q = h.bundle?.controlTarget.listQuarantine()[0];
    h.bundle?.engine.releaseQuarantine(q?.id ?? 0);
    await h.waitForConvergence();
    expect(h.remoteFiles().get('bad.txt')).toBe('B');
  });

  it('enters error when the sync root disappears and never deletes anything', async () => {
    h.write('keep.txt', 'K');
    await h.start();
    await h.waitForConvergence();
    rmSync(h.root, { recursive: true, force: true });
    h.fake.seedFile(h.remoteRootUid, 'poke.txt', 'poke'); // triggers a cycle via the feed
    const status = await h.waitFor(['error']);
    expect(status.reason).toMatch(/sync root/i);
    expect(h.fake.trashedUids()).toEqual([]);
    expect(h.remoteFiles().get('keep.txt')).toBe('K');
  });

  it('goes offline when the remote is unreachable during a listing and recovers', async () => {
    await h.start();
    await h.waitFor(['idle']);
    h.fake.injectFault('list', { kind: 'connection' });
    await h.bundle?.engine.onRemoteRefreshRequired('test');
    expect(h.bundle?.engine.getStatus().state).toBe('offline');
    await h.bundle?.engine.onRemoteRefreshRequired('test again');
    await h.waitFor(['idle']);
  });

  it('survives a restart with a pending change', async () => {
    h.write('a.txt', 'A');
    await h.start();
    await h.waitForConvergence();
    await h.bundle?.engine.stop();
    expect(h.bundle?.engine.getStatus().state).toBe('stopped');
    h.write('b.txt', 'B');
    await h.restart();
    await h.waitForConvergence();
    expect(h.remoteFiles().get('b.txt')).toBe('B');
  });
});

describe('control socket', () => {
  it('drives the engine entirely over the socket and pushes status events', async () => {
    h.write('x.txt', 'X');
    await h.start();
    await h.waitForConvergence();
    const socketPath = h.paths.controlSocket;
    const server = new ControlServer(socketPath, h.bundle?.controlTarget ?? (null as never));
    await server.listen();
    const client = new ControlClient(socketPath);
    const pushed: string[] = [];
    client.onStatus((s) => pushed.push(s.state));
    try {
      await client.connect();
      const status = await client.request<{ state: string }>({ cmd: 'status' });
      expect(status.state).toBe('idle');
      expect(await client.request<{ state: string }>({ cmd: 'pause' })).toMatchObject({ state: 'paused' });
      h.write('y.txt', 'Y');
      await new Promise((r) => setTimeout(r, 300));
      expect(h.remoteFiles().has('y.txt')).toBe(false);
      expect((await client.request<{ state: string }>({ cmd: 'resume' })).state).not.toBe('paused');
      await client.request({ cmd: 'sync_now' });
      await h.waitForConvergence();
      expect(h.remoteFiles().get('y.txt')).toBe('Y');
      expect(await client.request({ cmd: 'conflicts' })).toEqual([]);
      expect(await client.request({ cmd: 'quarantine' })).toEqual([]);
      expect(await client.request({ cmd: 'recycle' })).toEqual([]);
      await expect(client.request({ cmd: 'resolve', args: { id: 1, choice: 'nope' as never } })).rejects.toThrow(/choice must be/);
      await expect(client.request({ cmd: 'confirm', args: { id: 'held-99' } })).rejects.toThrow(/No held plan/);
      expect(pushed.length).toBeGreaterThan(0);
      expect(await ControlClient.probe(socketPath)).toBe(true);
      // A second server on the same live socket is refused.
      await expect(new ControlServer(socketPath, h.bundle?.controlTarget ?? (null as never)).listen()).rejects.toThrow(/Another instance/);
      expect(await client.request({ cmd: 'quit' })).toEqual({ quitting: true });
      await h.waitFor(['stopped']);
    } finally {
      client.close();
      await server.close();
    }
    expect(await ControlClient.probe(socketPath)).toBe(false);
  });
});
