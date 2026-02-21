/*
 * runtime_v0.h - concrete phase-A guest invocation runtime wrapper
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_WASI_RUNTIME_V0_H
#define SAPLING_WASI_RUNTIME_V0_H

#include "sapling/sapling.h"
#include "runner/wire_v0.h"

#include <stdint.h>

typedef int (*sap_wasi_runtime_v0_entry_fn)(void *ctx, const uint8_t *request, uint32_t request_len,
                                            uint8_t *reply_buf, uint32_t reply_buf_cap,
                                            uint32_t *reply_len_out);

typedef struct
{
    const char *entry_name;
    sap_wasi_runtime_v0_entry_fn entry_fn;
    void *entry_ctx;
    uint64_t calls;
    int last_rc;
} SapWasiRuntimeV0;

int sap_wasi_runtime_v0_init(SapWasiRuntimeV0 *runtime, const char *entry_name,
                             sap_wasi_runtime_v0_entry_fn entry_fn, void *entry_ctx);

int sap_wasi_runtime_v0_invoke(SapWasiRuntimeV0 *runtime, const SapRunnerMessageV0 *msg,
                               uint8_t *reply_buf, uint32_t reply_buf_cap, uint32_t *reply_len_out);

#endif /* SAPLING_WASI_RUNTIME_V0_H */
