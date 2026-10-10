import { readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';

const root = path.resolve(import.meta.dirname, '../..');
const readme = readFileSync(path.join(root, 'README.md'), 'utf8');
const manifest = JSON.parse(readFileSync(path.join(root, 'manifest.json'), 'utf8')) as {
  schemaVersion: number;
  id: string;
  name: string;
  version: string;
  author: string;
  license: string;
  kinds: string[];
  entryPoints: { service: string; barWidget: string };
  barWidget: { defaultSection: string; allowMultiple: boolean };
};
const pkg = JSON.parse(readFileSync(path.join(root, 'package.json'), 'utf8')) as { version: string };

describe('Omarchy package', () => {
  it('matches the marketplace manifest contract', () => {
    expect(manifest.schemaVersion).toBe(1);
    expect(manifest.id).toBe('io.github.zakkoo.proton-drive');
    expect(manifest.name).toBe('Proton Drive Sync');
    expect(manifest.author).toBe('zakko');
    expect(manifest.license).toBe('MIT');
    expect(manifest.version).toBe(pkg.version);
    expect(manifest.kinds).toEqual(['service', 'bar-widget']);
    expect(manifest.entryPoints.service).toBe('omarchy/Service.qml');
    expect(manifest.entryPoints.barWidget).toBe('omarchy/BarWidget.qml');
    expect(manifest.barWidget.defaultSection).toBe('right');
    expect(manifest.barWidget.allowMultiple).toBe(false);
  });

  it('speaks to an Omarchy user and leaves personal install notes out', () => {
    expect(readme).toContain('omarchy plugin add https://github.com/zakkoo/proton-drive-sync.git --enable');
    expect(readme).toContain('omarchy plugin remove io.github.zakkoo.proton-drive');
    expect(readme).toContain('omarchy plugin update io.github.zakkoo.proton-drive');
    expect(readme).not.toMatch(/install-engine|systemctl/);
    expect(readme).toMatch(/unofficial/i);
    expect(readme).toMatch(/recycle/i);
    expect(readme).toMatch(/Trash/);
    expect(readme).toMatch(/waits for you/);
    expect(readme).toMatch(/keep both/);
    expect(readme).toMatch(/sync folder/);
    expect(readme).toMatch(/Proton session/);
    expect(readme).toMatch(/config/);
    expect(readme).not.toContain('/home/zakko');
    expect(readme).not.toContain('exec-once');
    expect(readme).not.toMatch(/npm (ci|install)/);
    expect(readme).not.toContain('sudo');
  });
});
