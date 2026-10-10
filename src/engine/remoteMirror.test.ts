import { describe, expect, it } from 'vitest';

import { FakeRemote } from '../testing/fakeRemote.js';
import { RemoteMirror } from './remoteMirror.js';
import { listRemoteTree } from './views.js';

/**
 * A failed remote refresh must never look complete (spec: test-suite / Absence
 * is not a delete — an incomplete or failed listing withholds deletes). The
 * mirror only adopts a listing that finished; a throw leaves it incomplete and
 * keeps no half listing.
 */

function seedTree(): { fake: FakeRemote; root: string } {
  const fake = new FakeRemote();
  const root = fake.seedFolder(fake.rootUid, 'Sync').uid;
  fake.seedFile(root, 'a.txt', 'A');
  const sub = fake.seedFolder(root, 'sub').uid;
  fake.seedFile(sub, 'b.txt', 'B');
  return { fake, root };
}

describe('RemoteMirror and changes the engine made itself', () => {
  it('reflects an own change at once, and a node that is gone takes its descendants with it', async () => {
    const { fake, root } = seedTree();
    const mirror = new RemoteMirror(fake, root, () => 1000);
    await mirror.fullRefresh();
    const a = [...mirror.view().items.values()].find((i) => i.name === 'a.txt');
    if (a === undefined) throw new Error('a.txt not listed');
    mirror.applyOwnChange({ type: 'upsert', node: await fake.rename(a.uid, 'renamed.txt') });
    expect(mirror.view().items.get(a.uid)?.name).toBe('renamed.txt');
    const sub = [...mirror.view().items.values()].find((i) => i.name === 'sub');
    if (sub === undefined) throw new Error('sub not listed');
    mirror.applyOwnChange({ type: 'remove', uid: sub.uid });
    // The sync root itself is in the view too.
    expect([...mirror.view().items.values()].map((i) => i.name).sort()).toEqual(['Sync', 'renamed.txt']);
  });

  it('an event that read a node before an own change does not overwrite that change', async () => {
    const { fake, root } = seedTree();
    const mirror = new RemoteMirror(fake, root, () => 1000);
    await mirror.fullRefresh();
    const a = [...mirror.view().items.values()].find((i) => i.name === 'a.txt');
    if (a === undefined) throw new Error('a.txt not listed');
    // The event's re-read gets the node (still a.txt), then is slow to come back.
    const get = fake.getNode.bind(fake);
    let read!: () => void;
    let resume!: () => void;
    const atRead = new Promise<void>((r) => { read = r; });
    const resumed = new Promise<void>((r) => { resume = r; });
    fake.getNode = async (uid: string) => {
      const node = await get(uid);
      fake.getNode = get;
      read();
      await resumed;
      return node;
    };
    const applying = mirror.applyEvent({ type: 'node_updated', nodeUid: a.uid, parentUid: root, isTrashed: false, eventId: '9', scopeId: 'scope-1' });
    await atRead;
    mirror.applyOwnChange({ type: 'upsert', node: await fake.rename(a.uid, 'renamed.txt') });
    resume();
    await applying;
    expect(mirror.view().items.get(a.uid)?.name).toBe('renamed.txt');
  });

  it('an event that read a node does not bring it back after an own removal of its folder', async () => {
    const { fake, root } = seedTree();
    const mirror = new RemoteMirror(fake, root, () => 1000);
    await mirror.fullRefresh();
    const sub = [...mirror.view().items.values()].find((i) => i.name === 'sub');
    const b = [...mirror.view().items.values()].find((i) => i.name === 'b.txt');
    if (sub === undefined || b === undefined) throw new Error('sub/b.txt not listed');
    // The child's event re-read gets the node, then is slow to come back.
    const get = fake.getNode.bind(fake);
    let read!: () => void;
    let resume!: () => void;
    const atRead = new Promise<void>((r) => { read = r; });
    const resumed = new Promise<void>((r) => { resume = r; });
    fake.getNode = async (uid: string) => {
      const node = await get(uid);
      fake.getNode = get;
      read();
      await resumed;
      return node;
    };
    const applying = mirror.applyEvent({ type: 'node_updated', nodeUid: b.uid, parentUid: sub.uid, isTrashed: false, eventId: '9', scopeId: 'scope-1' });
    await atRead;
    // The engine removes the folder, and with it the child.
    mirror.applyOwnChange({ type: 'remove', uid: sub.uid });
    resume();
    await applying;
    expect(mirror.view().items.has(b.uid)).toBe(false);
  });

  it('keeps an own change made while a full listing was under way', async () => {
    const { fake, root } = seedTree();
    const mirror = new RemoteMirror(fake, root, () => 1000);
    await mirror.fullRefresh();
    const a = [...mirror.view().items.values()].find((i) => i.name === 'a.txt');
    if (a === undefined) throw new Error('a.txt not listed');
    // The listing reads the root's children (a.txt still under its old name), then waits.
    const list = fake.listChildren.bind(fake);
    let listed!: () => void;
    let resume!: () => void;
    const atPause = new Promise<void>((r) => { listed = r; });
    fake.listChildren = async (parentUid: string) => {
      const children = await list(parentUid);
      if (parentUid === root) {
        listed();
        await new Promise<void>((r) => { resume = r; });
      }
      return children;
    };
    const refreshing = mirror.fullRefresh();
    await atPause;
    mirror.applyOwnChange({ type: 'upsert', node: await fake.rename(a.uid, 'renamed.txt') });
    resume();
    await refreshing;
    // The listing's older result does not undo the rename the engine made meanwhile.
    expect(mirror.view().items.get(a.uid)?.name).toBe('renamed.txt');
  });
});

describe('RemoteMirror modifiedAt', () => {
  it('reports when a known item last changed, and nothing for an unknown one', async () => {
    const { fake, root } = seedTree();
    const dated = fake.seedFile(root, 'dated.txt', 'D', { modifiedAt: new Date(1_700_000_000_000) });
    const mirror = new RemoteMirror(fake, root, () => 1000);
    await mirror.fullRefresh();
    expect(mirror.modifiedAt(dated.uid)).toBe((dated.claimedModifiedAt ?? dated.serverModifiedAt).getTime());
    expect(mirror.modifiedAt(dated.uid)).toBeGreaterThan(0);
    expect(mirror.modifiedAt('no-such-uid')).toBeNull();
  });

  it('keeps a Proton document named like an Object member as its own entry', async () => {
    const { fake, root } = seedTree();
    fake.seedProtonDocument(root, '__proto__');
    const mirror = new RemoteMirror(fake, root, () => 1000);
    await mirror.fullRefresh();
    const times = mirror.library().protonDocumentModifiedAt;
    expect(Object.keys(times)).toEqual(['__proto__']);
    // It survives the trip to the page as a number.
    expect(typeof (JSON.parse(JSON.stringify(times)) as Record<string, unknown>)['__proto__']).toBe('number');
  });
});

describe('RemoteMirror completeness', () => {
  it('a fresh mirror is incomplete until a full refresh succeeds', () => {
    const { fake, root } = seedTree();
    const mirror = new RemoteMirror(fake, root, () => 1000);
    expect(mirror.isComplete).toBe(false);
    expect(mirror.view().complete).toBe(false);
  });

  it('a failed full refresh throws, stays incomplete, and keeps no half listing', async () => {
    const { fake, root } = seedTree();
    const mirror = new RemoteMirror(fake, root, () => 1000);

    await mirror.fullRefresh();
    expect(mirror.isComplete).toBe(true);
    const completeSize = mirror.size;
    expect(completeSize).toBeGreaterThan(0);

    // The next listing fails partway (the root list rejects).
    fake.injectFault('list', { kind: 'connection' });
    await expect(mirror.fullRefresh()).rejects.toThrow();

    // The mirror is now marked incomplete and unavailable — a failed refresh cannot look complete.
    expect(mirror.isComplete).toBe(false);
    expect(mirror.view().complete).toBe(false);
    expect(mirror.view().available).toBe(false);
    // It did not adopt a partial listing: the node set was not replaced by a half result.
    expect(mirror.size).toBe(completeSize);
  });

  it('listRemoteTree throws instead of returning a partial tree', async () => {
    const { fake, root } = seedTree();
    fake.injectFault('list', { kind: 'connection' });
    await expect(listRemoteTree(fake, root)).rejects.toThrow();
  });
});
