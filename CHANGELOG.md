# Changelog

All notable changes to this project are documented in this file.

## [Unreleased] - 2026-02-24

### Added
- Per-file `Preview` button in the `Files` table.
- Separate preview window for media (`PreviewDialog`) with:
  - image preview,
  - in-app video/audio playback (`QMediaPlayer` + `QVideoWidget`),
  - playback controls (`Play`, `Pause`, `Stop`) and seek slider.
- Profiles support for multiple account/bucket setups:
  - create/save/delete/switch profiles,
  - persisted active profile in settings.
- Update checker:
  - `Check for Updates` action in `More`,
  - compare current app version with latest GitHub release,
  - configurable repo (`Set Update Repo`).
- Files table context action `Preview Selected`.

### Changed
- Release automation workflow now creates real semantic tags/releases (`vX.Y.Z`) on pushes to `main/master` with automatic patch bump.
- UI flow now prefers row-level preview actions over toolbar-heavy flow.

### Removed
- Legacy `Preview` tab and its embedded preview controls from the main window.

### Fixed
- Preserved preview behavior for private buckets via temporary signed URLs used for media playback.
