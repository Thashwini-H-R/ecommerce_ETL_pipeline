# Releasing

This repository follows semantic versioning for release tags (vMAJOR.MINOR.PATCH).

Release steps (manual):

1. Bump version in CHANGELOG.md: move Unreleased items into a new section for the release.
2. Create a signed annotated tag locally, e.g.: `git tag -a v0.1.0 -m "chore(release): v0.1.0"`
3. Push tags: `git push origin --tags`
4. Optionally create a GitHub Release with release notes copied from the changelog.

Automated helper (PowerShell): `scripts/release.ps1` can create and push a tag using environment creds.
