/**
 * In-memory mirror of the remote tree under the sync root, kept current by
 * the event feed and refreshed by full listings (startup, expiry, interval).
 */
import type { RemoteChange } from '../execute/types.js';
import type { NodeRemoteEvent } from '../remote/events.js';
import type { RemoteDrive, RemoteNode } from '../remote/interface.js';
import type { RemoteView } from '../reconcile/types.js';
import { listRemoteTree, remoteViewFromNodes } from './views.js';

export class RemoteMirror {
  private nodes = new Map<string, RemoteNode>();
  private complete = false;
  private available = true;
  lastFullListingAt: number | null = null;
  /** The mirror reflects the server at least as of this time (start of the last listing or completed poll). */
  private asOf: number | undefined;
  /** The engine's own changes made while each full listing in progress was under way. */
  private readonly ownChangesDuringListings = new Set<RemoteChange[]>();
  /** Counts the engine's own changes; per node, the count when it was last changed so. */
  private ownChanges = 0;
  private readonly ownChangedAt = new Map<string, number>();

  constructor(
    private readonly remote: RemoteDrive,
    private readonly rootUid: string,
    private readonly now: () => number = Date.now,
  ) {}

  get isComplete(): boolean {
    return this.complete;
  }

  get size(): number {
    return this.nodes.size;
  }

  /** Replace the mirror with a complete listing. Throws (and marks unavailable) on failure. */
  async fullRefresh(signal?: AbortSignal): Promise<void> {
    const startedAt = this.now();
    const ownChanges: RemoteChange[] = [];
    this.ownChangesDuringListings.add(ownChanges);
    try {
      const nodes = await listRemoteTree(this.remote, this.rootUid, signal);
      this.nodes = new Map(nodes.map((n) => [n.uid, n]));
      // The listing may have read a node before the engine changed it: keep the change.
      for (const change of ownChanges) this.applyChange(change);
      this.complete = true;
      this.available = true;
      this.lastFullListingAt = this.now();
      this.asOf = startedAt;
    } catch (error) {
      this.complete = false;
      this.available = false;
      throw error;
    } finally {
      this.ownChangesDuringListings.delete(ownChanges);
    }
  }

  /** Apply one node event by re-reading the node (events carry no metadata). */
  async applyEvent(event: NodeRemoteEvent): Promise<void> {
    if (event.type === 'node_deleted') {
      this.nodes.delete(event.nodeUid);
      return;
    }
    const readFrom = this.ownChanges;
    const node = await this.remote.getNode(event.nodeUid);
    // The engine changed this node itself while the read was under way: the read may predate that
    // change, and the change's own event will come and read the node again.
    if ((this.ownChangedAt.get(event.nodeUid) ?? 0) > readFrom) return;
    if (node === null) this.nodes.delete(event.nodeUid);
    else this.nodes.set(node.uid, node);
    this.available = true;
  }

  /**
   * A change the engine itself made on Proton (the executor reports it once verified): reflect it
   * at once instead of waiting for its event. Under load the event arrives after the next cycle
   * has planned, and a plan from the older tree undoes the change or records a conflict with it.
   * The event, when it comes, re-reads the node and agrees.
   */
  applyOwnChange(change: RemoteChange): void {
    // Every node the change touches (a removal takes the descendants too) is marked as changed by
    // the engine, so an event read of any of them that was under way is not stored over it.
    const changedAt = ++this.ownChanges;
    for (const uid of this.applyChange(change)) this.ownChangedAt.set(uid, changedAt);
    for (const during of this.ownChangesDuringListings) during.push(change);
  }

  upsert(node: RemoteNode): void {
    this.nodes.set(node.uid, node);
  }

  /**
   * Forget a node and everything under it (a trashed folder stays as an upsert, marked trashed).
   * Returns the uids forgotten.
   */
  remove(uid: string): string[] {
    const gone = new Set([uid]);
    let grew = true;
    while (grew) {
      grew = false;
      for (const node of this.nodes.values()) {
        if (!gone.has(node.uid) && node.parentUid !== undefined && gone.has(node.parentUid)) {
          gone.add(node.uid);
          grew = true;
        }
      }
    }
    for (const g of gone) this.nodes.delete(g);
    return [...gone];
  }

  /** Apply a change; returns the uids it touched. */
  private applyChange(change: RemoteChange): string[] {
    if (change.type === 'remove') return this.remove(change.uid);
    this.upsert(change.node);
    return [change.node.uid];
  }

  /**
   * When a known, non-trashed item last changed (ms): the content's own time when the
   * saving app recorded one, else Proton's. Null when the item is unknown or trashed.
   */
  modifiedAt(uid: string): number | null {
    const node = this.nodes.get(uid);
    if (node === undefined || node.isTrashed) return null;
    return (node.claimedModifiedAt ?? node.serverModifiedAt).getTime();
  }

  /** A poll of the event stream that started at `startedAt` completed successfully. */
  markPolled(startedAt: number): void {
    if (this.asOf === undefined || startedAt > this.asOf) this.asOf = startedAt;
  }

  markUnavailable(): void {
    this.available = false;
  }

  markAvailable(): void {
    this.available = true;
  }

  view(): RemoteView {
    return remoteViewFromNodes([...this.nodes.values()], this.rootUid, this.complete, this.available, this.asOf);
  }

  files(): number {
    return this.library().files;
  }

  /**
   * Non-trashed files, Proton document paths, and the other file paths.
   * A file under a trashed ancestor has no path and is still counted in `files`.
   */
  library(): { files: number; protonDocumentPaths: string[]; protonDocumentModifiedAt: Record<string, number>; syncableFilePaths: string[] } {
    const protonDocumentPaths: string[] = [];
    // No prototype: a document named like an Object member (e.g. "__proto__") stays an own entry.
    const protonDocumentModifiedAt = Object.create(null) as Record<string, number>;
    const syncableFilePaths: string[] = [];
    let files = 0;
    for (const node of this.nodes.values()) {
      if (node.type !== 'file' || node.isTrashed) continue;
      files++;
      const rel = this.relPath(node);
      if (rel === null) continue;
      if (node.isProtonDocument) {
        protonDocumentPaths.push(rel);
        // The content's own time when the saving app recorded one, else Proton's.
        protonDocumentModifiedAt[rel] = (node.claimedModifiedAt ?? node.serverModifiedAt).getTime();
      } else syncableFilePaths.push(rel);
    }
    protonDocumentPaths.sort();
    syncableFilePaths.sort();
    return { files, protonDocumentPaths, protonDocumentModifiedAt, syncableFilePaths };
  }

  /** Root-relative path, or null when an ancestor is trashed or the chain is broken. */
  private relPath(node: RemoteNode): string | null {
    const parts: string[] = [];
    let current: RemoteNode | undefined = node;
    const seen = new Set<string>();
    while (current !== undefined && current.uid !== this.rootUid) {
      if (seen.has(current.uid)) return null;
      seen.add(current.uid);
      if (current !== node && current.isTrashed) return null;
      parts.push(current.name);
      if (current.parentUid === undefined) return null;
      current = this.nodes.get(current.parentUid);
    }
    if (current === undefined) return null;
    return parts.reverse().join('/');
  }
}
