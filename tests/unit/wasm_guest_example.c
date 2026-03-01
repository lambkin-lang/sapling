/*
 * wasm_guest_example.c - Wasm guest using Sapling Host API
 *
 * SPDX-License-Identifier: MIT
 */
#include <stdint.h>
#include <string.h>

#if defined(__has_attribute)
#if __has_attribute(import_module) && __has_attribute(import_name)
#define SAP_WASM_IMPORT(module, name) __attribute__((import_module(module), import_name(name)))
#else
#define SAP_WASM_IMPORT(module, name)
#endif
#if __has_attribute(export_name)
#define SAP_WASM_EXPORT(name) __attribute__((export_name(name)))
#else
#define SAP_WASM_EXPORT(name)
#endif
#else
#define SAP_WASM_IMPORT(module, name)
#define SAP_WASM_EXPORT(name)
#endif

/*
 * Host API Imports (as expected by the WASI shim)
 * Note: These signatures must match the Wasm boundary contract.
 * For this phase, we use simplified exports that the shim will bridge.
 */

SAP_WASM_IMPORT("sapling:host/v0", "get")
extern int32_t sap_host_get(uint32_t dbi, const void *key, uint32_t key_len, const void **val_out,
                            uint32_t *val_len_out);

SAP_WASM_IMPORT("sapling:host/v0", "put")
extern int32_t sap_host_put(uint32_t dbi, const void *key, uint32_t key_len, const void *val,
                            uint32_t val_len);

SAP_WASM_IMPORT("sapling:host/v0", "lease_acquire")
extern int32_t sap_host_lease_acquire(const void *key, uint32_t key_len, int64_t duration_ms);

SAP_WASM_IMPORT("sapling:host/v0", "lease_release")
extern int32_t sap_host_lease_release(const void *key, uint32_t key_len);

/*
 * Entry point: called by the runner when a message arrives.
 */
SAP_WASM_EXPORT("sap_run_v0") int32_t sap_run_v0(const void *msg_payload, uint32_t msg_len)
{
    const char *lease_key = "lock-1";
    const char *counter_key = "counter";
    const void *val = NULL;
    uint32_t val_len = 0;
    uint32_t counter = 0;
    int32_t rc;

    (void)msg_payload;
    (void)msg_len;

    /* 1. Acquire lease */
    rc = sap_host_lease_acquire(lease_key, (uint32_t)strlen(lease_key), 5000);
    if (rc != 0)
    {
        return 101; /* ERR_BUSY or error */
    }

    /* 2. Read counter (DBI 0) */
    rc = sap_host_get(0, counter_key, (uint32_t)strlen(counter_key), &val, &val_len);
    if (rc == 0 && val_len == 4)
    {
        memcpy(&counter, val, 4);
    }
    else if (rc != -1)
    { /* -1 for NOTFOUND? (assuming standard mapping) */
        /* If not found, counter remains 0 */
    }

    /* 3. Increment and Put */
    counter++;
    sap_host_put(0, counter_key, (uint32_t)strlen(counter_key), &counter, 4);

    /* 4. Release lease */
    sap_host_lease_release(lease_key, (uint32_t)strlen(lease_key));

    return 0;
}
