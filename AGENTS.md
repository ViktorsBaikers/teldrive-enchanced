# TelDrive Submodule Guide

Repository-wide guidance lives in the [root AGENTS.md](../AGENTS.md). Read it first. This file keeps only details specific to `teldrive-enchanced/`.

## Environment

- Within the Nix setup, `.envrc` loads the `flake.nix` dev shell, including the toolchain and Playwright's Chromium. Do not bypass it with host tools or ad hoc library-path fixes.
- `justfile` loads `.env` from this directory automatically. Never print or commit that file.
- If direnv reports an unapproved `.envrc`, inspect `.envrc` and `flake.nix`, then ask the user to approve it with `direnv allow`. Do not grant trust automatically or silently fall back to an unapproved environment.
- For commands that need a subdirectory, load this environment first: `direnv exec . bash -c 'cd ui && bunx playwright test e2e/core.spec.ts --grep storage'`.

## Verification Specifics

- Storage changes must run `e2e/core.spec.ts --grep storage` on both desktop and mobile projects.
- SQL statistics changes must cover date-window boundaries, zero-data cases, and user isolation through the disposable PostgreSQL harness.
- Edit documentation pages in `docs/content/docs/`; preview with `just docs-dev` and build with `just docs-build`.
- Describe metrics by their actual data source. Do not call active-file metadata historical upload traffic or Telegram part counts byte usage.
- Interactive charts must support keyboard and touch input as well as pointer input.
