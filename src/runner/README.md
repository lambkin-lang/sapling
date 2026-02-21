# Runner Module

Planned home for the C host runner:
- worker thread orchestration
- retry/commit state machine
- mailbox and timer scheduling loop

Implemented foundation:
- `wire_v0.h` / `wire_v0.c`: strict v0 message + intent serialization contract
  with version checks and deterministic decode validation.
