# Exactly-Once Integrity (Deduplication)

Sapling ensures **Exactly-Once Integrity** for message processing through a persistent deduplication mechanism. This prevents redundant side effects and state updates if a message is retried or re-delivered due to worker failures or network issues.

## Mechanism (DBI 5)

The runner maintains a deduplication table in **DBI 5**. Each entry maps a unique `message_id` to processing metadata.

### Data Structure

The `SapRunnerDedupeV0` structure records the following:

- **Accepted**: Whether the message was successfully processed and committed.
- **Last Seen TS**: The timestamp of the most recent processing attempt.
- **Checksum**: A cryptographic hash of the message payload to detect identifier collisions.

## Flow

1.  **Check Phase**: Before executing the guest logic, the runner checks DBI 5 for the incoming `message_id`.
2.  **Dedupe Filter**:
    - If found and `accepted == 1`, the runner skips execution and acknowledges the source message (if applicable).
    - If found and `accepted == 0`, it indicates a previous failed attempt. The runner proceeds but updates the `last_seen_ts`.
    - If not found, it is a new message.
3.  **Atomic Commitment**: If the guest logic completes successfully, a `SAP_RUNNER_DEDUPE_V0` record with `accepted = 1` is staged in the transaction. It is committed atomically with the guest's state changes and business intents.

## Internal API

The following functions in `src/runner/dedupe_v0.h` manage these records:

- `sap_runner_dedupe_v0_get`: Retrieves deduplication metadata from DBI 5.
- `sap_runner_dedupe_v0_put`: Persists deduplication metadata immediately.
- `sap_runner_dedupe_v0_stage_put`: Stages deduplication metadata to be written atomically with a transaction commit.

## Configuration

Deduplication behavior is guided by the `SapRunnerV0Policy`. Future versions will support time-to-live (TTL) for deduplication entries to manage database size.
