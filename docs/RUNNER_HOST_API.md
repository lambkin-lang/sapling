# Sapling Runner Host API (v0)

The Sapling Runner Host API provides a set of C-level functions that a guest (either native or Wasm) uses to interact with Sapling DBIs within an **atomic block**. This API enforces the project's isolation model where workers primarily communicate through the database and message queues.

## Overview

When the runner executes an atomic block, it provides a stable read transaction and a staging area for writes. The Host API wraps these internals into a clean interface.

### Context Structure

The `SapHostV0` structure holds the necessary context:

```c
typedef struct
{
    SapRunnerTxStackV0 *stack; // The nesting context stack
    Txn *read_txn;             // Stable read snapshot
} SapHostV0;
```

## Data Operations

These operations interact with the staging area. Reads will see previously staged writes within the same atomic block (nested read-your-writes).

### `sap_host_v0_get`
Retrieves a value from a specific DBI.

```c
int sap_host_v0_get(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len,
                    const void **val_out, uint32_t *val_len_out);
```

### `sap_host_v0_put`
Stages a write (insertion or update) to a specific DBI.

```c
int sap_host_v0_put(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len,
                    const void *val, uint32_t val_len);
```

### `sap_host_v0_del`
Stages a deletion of a key from a specific DBI.

```c
int sap_host_v0_del(SapHostV0 *host, uint32_t dbi, const void *key, uint32_t key_len);
```

## Intent Operations

These operations queue side effects that will only be published if the atomic block successfully commits.

### `sap_host_v0_emit`
Publishes a message to the worker's outbox.

```c
int sap_host_v0_emit(SapHostV0 *host, const void *msg, uint32_t msg_len);
```

### `sap_host_v0_arm`
Schedules a timer to fire at a specific timestamp.

```c
int sap_host_v0_arm(SapHostV0 *host, int64_t due_ts, const void *msg, uint32_t msg_len);
```

## Lease Operations

These operations allow a guest to manage distributed leases within an atomic block.

### `sap_host_v0_lease_acquire`
Attempts to acquire a lease. The lease record is staged and will be written on commit.

```c
int sap_host_v0_lease_acquire(SapHostV0 *host, const void *key, uint32_t key_len,
                              int64_t duration_ms);
```

### `sap_host_v0_lease_release`
Stages the release of a lease.

```c
int sap_host_v0_lease_release(SapHostV0 *host, const void *key, uint32_t key_len);
```

## Usage Example (Native Guest)

```c
int my_guest_logic(void *ctx, SapHostV0 *host, const uint8_t *req, uint32_t req_len, ...)
{
    const void *val;
    uint32_t len;
    const char *lease_name = "critical-section";
    
    // 1. Acquire a lease for 5 seconds
    if (sap_host_v0_lease_acquire(host, lease_name, strlen(lease_name), 5000) != ERR_OK) {
        return ERR_BUSY;
    }

    // 2. Read state from DBI 0 (app_state)
    if (sap_host_v0_get(host, 0, "key", 3, &val, &len) == ERR_OK) {
        // ...
    }
    
    // 3. Stage a write
    sap_host_v0_put(host, 0, "counter", 7, "\x01\x00...", 8);
    
    // 4. Emit a side effect
    sap_host_v0_emit(host, "hello", 5);

    // 5. Release the lease
    sap_host_v0_lease_release(host, lease_name, strlen(lease_name));
    
    return ERR_OK;
}
```
