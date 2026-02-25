/*
 * runtime_v0.h - concrete phase-A guest invocation runtime wrapper
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_WASI_RUNTIME_V0_H
#define SAPLING_WASI_RUNTIME_V0_H

#include "runner/host_v0.h"
#include "runner/wire_v0.h"
#include "sapling/sapling.h"

#include <stdint.h>

typedef int (*sap_wasi_runtime_v0_write_fn)(const uint8_t *chunk, uint32_t chunk_len, void *ctx);

typedef int (*sap_wasi_runtime_v0_entry_fn)(void *ctx, SapHostV0 *host, const uint8_t *request,
                                            uint32_t request_len, uint8_t *reply_buf,
                                            uint32_t reply_buf_cap, uint32_t *reply_len_out);

typedef int (*sap_wasi_runtime_v0_stream_fn)(void *ctx, SapHostV0 *host, const uint8_t *request,
                                             uint32_t request_len,
                                             sap_wasi_runtime_v0_write_fn write, void *write_ctx,
                                             uint32_t *reply_len_out);

typedef struct
{
    const char *name;
    sap_wasi_runtime_v0_entry_fn invoke;
    sap_wasi_runtime_v0_stream_fn invoke_stream;
} SapWasiRuntimeV0Adapter;

typedef struct
{
    const char *entry_name;
    const SapWasiRuntimeV0Adapter *adapter;
    void *adapter_ctx;
    sap_wasi_runtime_v0_entry_fn entry_fn_compat;
    void *entry_ctx_compat;
    uint64_t calls;
    int last_rc;
} SapWasiRuntimeV0;

int sap_wasi_runtime_v0_init_adapter(SapWasiRuntimeV0 *runtime, const char *entry_name,
                                     const SapWasiRuntimeV0Adapter *adapter, void *adapter_ctx);

int sap_wasi_runtime_v0_init(SapWasiRuntimeV0 *runtime, const char *entry_name,
                             sap_wasi_runtime_v0_entry_fn entry_fn, void *entry_ctx);

int sap_wasi_runtime_v0_invoke(SapWasiRuntimeV0 *runtime, SapHostV0 *host,
                               const SapRunnerMessageV0 *msg, uint8_t *reply_buf,
                               uint32_t reply_buf_cap, uint32_t *reply_len_out);

#endif /* SAPLING_WASI_RUNTIME_V0_H */
