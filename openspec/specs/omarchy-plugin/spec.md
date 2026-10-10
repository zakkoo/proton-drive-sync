# Omarchy Plugin

## Purpose

Make this repository one Omarchy Quattro plugin that the marketplace can list, with a valid manifest, a license, the supplied preview, and a README written for any Omarchy user.

## Requirements

### Requirement: One plugin at the repository root
The repository SHALL contain exactly one Omarchy plugin, identified by a root `manifest.json`. The manifest SHALL set `schemaVersion` to the JSON number `1`, `id` to `io.github.zakkoo.proton-drive`, `name` to `Proton Drive Sync`, `author` to `zakko`, `license` to `MIT`, and `version` to the same version string as `package.json`. `kinds` SHALL be `service` and `bar-widget`. `entryPoints.service` and `entryPoints.barWidget` SHALL be safe relative paths to files that exist in the repository. `barWidget.defaultSection` SHALL be `right` and `barWidget.allowMultiple` SHALL be `false`. The id SHALL NOT use the `omarchy.*` namespace.

#### Scenario: Validator accepts the repository
- **WHEN** `omarchy plugin validate` is run on the repository root
- **THEN** it exits 0

#### Scenario: Reserved or drifting id
- **WHEN** the manifest id is changed, prefixed with `omarchy.`, or does not match `io.github.zakkoo.proton-drive`
- **THEN** the package is not a valid listing of this plugin

### Requirement: Preview is the supplied card
The image supplied as `plugin-card-proton-drive-client-tool.png` SHALL be moved to `preview.png` at the repository root and SHALL NOT remain under the old name. `preview.png` SHALL be a regular file, not a symlink, of type PNG, at most 50 MB, and at most 40 megapixels. The repository root SHALL NOT also contain `preview.jpg`, `preview.jpeg`, `preview.webp`, or `preview.avif`.

#### Scenario: Marketplace preview name
- **WHEN** the repository is packed for the marketplace
- **THEN** the only root preview file is `preview.png` and it is the supplied card

### Requirement: License and dependency notice
The repository root SHALL contain an MIT `LICENSE` whose copyright holder is zakko. The existing Proton copyright notice under the adapted Proton code SHALL remain. The README SHALL state that the plugin is an unofficial integration not affiliated with Proton AG or the Omarchy project.

#### Scenario: A reader looks up the license
- **WHEN** someone opens the repository root
- **THEN** they find an MIT `LICENSE` and a README that states the unofficial status

### Requirement: README is for an Omarchy user
The root README SHALL be the user guide. It SHALL be written in the second person for someone on Omarchy, with no path that belongs to a particular person and no pinned tool-manager install path. It SHALL include these commands exactly:

- `omarchy plugin add https://github.com/zakkoo/proton-drive-sync.git --enable`
- `omarchy plugin remove io.github.zakkoo.proton-drive`

It SHALL say, in plain language, that synced files are not deleted outright, that a large delete or replace waits for confirmation, and that both copies are kept when the two sides disagree. It SHALL state that `omarchy plugin remove` removes the shell plugin and does not delete the sync folder, the Proton session, or the tool's config. It SHALL NOT instruct the user to run a package install inside the plugin checkout, to pipe a download into a shell, or to use sudo. A development section MAY follow the user guide and SHALL be shorter than the user guide.

#### Scenario: New Omarchy user
- **WHEN** an Omarchy user opens the README
- **THEN** they can install the plugin, sign in, choose the two folders, and remove the plugin without following a machine-specific path

#### Scenario: Personal walkthrough is gone
- **WHEN** the README is read
- **THEN** it contains no `/home/zakko` path and no instruction to edit Hyprland configuration

### Requirement: Plugin checkout stays free of symlinks
The files committed in the repository, and the plugin directory after `omarchy plugin add`, SHALL contain no symlink outside `.git`. Enabling the plugin SHALL NOT create `node_modules` or any other symlink in that directory.

#### Scenario: Validate after add
- **WHEN** the plugin has been added and enabled
- **THEN** `omarchy plugin validate` on the plugin directory still exits 0

### Requirement: QML entry points lint
Each QML file named by `entryPoints` SHALL pass `qmllint` against the installed Omarchy shell imports with exit status 0.

#### Scenario: Lint the entry points
- **WHEN** `qmllint` is run with `-I "$OMARCHY_PATH/shell"` on the manifest entry-point files
- **THEN** it exits 0

### Requirement: Marketplace submission waits for the owner
Preparing the package SHALL NOT change the GitHub repository's visibility and SHALL NOT open an issue on `omacom/omarchy-plugin-marketplace`. A submission draft SHALL be produced for the owner to review, using category `Productivity`, tags `bar`, `system`, and `security`, the repository root URL without a trailing slash, and the five checklist statements from the marketplace submission guide left unchecked until the owner agrees. The draft SHALL note that the engine is a committed reproducible bundle started by the shell service, that install uses no sudo, and that the preview is the supplied Proton Drive card.

#### Scenario: Package is ready and the owner has not confirmed
- **WHEN** the manifest, README, license, preview, and shell files are in place and the owner has not confirmed the checklist
- **THEN** the repository visibility is unchanged and no marketplace issue has been opened
