# Runner Module

Planned home for the C host runner:
- worker thread orchestration
- retry/commit state machine
- mailbox and timer scheduling loop

Implemented foundation:
- `wire_v0.h` / `wire_v0.c`: strict v0 message + intent serialization contract
  with version checks and deterministic decode validation.
- `runner_v0.h` / `runner_v0.c`: phase-A lifecycle scaffold with DBI bootstrap,
  schema-version guard, single-step dispatch callback integration, and
  DB-backed inbox polling.
- `SapRunnerV0Worker` shell: runner tick/stop APIs and optional pthread
  start/join helpers (gated by `SAPLING_THREADED`).
