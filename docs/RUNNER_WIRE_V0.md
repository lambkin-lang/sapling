# Runner Wire Format v0

This document freezes the initial message/intent serialization contract used by
the C host runner. It is intentionally small, binary, and independent of
component-model runtime machinery.

WIT remains the source of truth for logical schema (`schemas/wit/runtime-schema.wit`);
this wire format is the host/runtime framing for persisted and queued records.

## Versioning contract

- Wire version fields: `major` + `minor` (`u16` each, little-endian).
- Current supported version: `0.0`.
- Decoder behavior in this revision:
  - accepts only `major=0`, `minor=0`
  - rejects other versions with `SAP_RUNNER_WIRE_EVERSION`
- Frame length is explicit (`u32`) and must exactly match the provided buffer
  length in this revision.

This strictness keeps compatibility behavior explicit while the runner is being
brought up.

## Message frame (`LMSG`)

Header size: 60 bytes.

Fixed header fields:
- bytes `[0..4)`: magic `"LMSG"`
- bytes `[4..6)`: version major
- bytes `[6..8)`: version minor
- bytes `[8..12)`: frame length (header + body)
- byte `12`: message kind (`0=command`, `1=event`, `2=timer`)
- byte `13`: message flags
- bytes `[14..16)`: reserved (must be 0)
- bytes `[16..24)`: `to_worker` (`s64`)
- bytes `[24..32)`: `route_worker` (`s64`)
- bytes `[32..40)`: `route_timestamp` (`s64`)
- bytes `[40..48)`: `from_worker` (`s64`, meaningful when `HAS_FROM_WORKER`)
- bytes `[48..52)`: `message_id_len` (`u32`, must be > 0)
- bytes `[52..56)`: `trace_id_len` (`u32`, or `0xFFFFFFFF` if absent)
- bytes `[56..60)`: `payload_len` (`u32`)

Body layout:
- `message_id` bytes
- optional `trace_id` bytes
- `payload` bytes

Message flags:
- `0x01` durable
- `0x02` high-priority
- `0x04` dedupe-required
- `0x08` requires-ack
- `0x10` has-from-worker
- `0x20` has-trace-id

## Intent frame (`LINT`)

Header size: 28 bytes.

Fixed header fields:
- bytes `[0..4)`: magic `"LINT"`
- bytes `[4..6)`: version major
- bytes `[6..8)`: version minor
- bytes `[8..12)`: frame length (header + body)
- byte `12`: intent kind (`0=outbox-emit`, `1=timer-arm`)
- byte `13`: intent flags
- bytes `[14..16)`: reserved (must be 0)
- bytes `[16..24)`: `due_ts` (`s64`, meaningful when `HAS_DUE_TS`)
- bytes `[24..28)`: serialized message length (`u32`, must be > 0)

Body layout:
- serialized message bytes (`LMSG` frame)

Intent flags:
- `0x01` has-due-ts

Kind constraints:
- `outbox-emit` must not set `HAS_DUE_TS`
- `timer-arm` must set `HAS_DUE_TS`

## Code location

- API: `src/runner/wire_v0.h`
- implementation: `src/runner/wire_v0.c`
- tests: `tests/unit/runner_wire_test.c`
