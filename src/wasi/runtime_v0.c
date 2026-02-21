/*
 * runtime_v0.c - concrete phase-A guest invocation runtime wrapper
 *
 * SPDX-License-Identifier: MIT
 */
#include "wasi/runtime_v0.h"

#include <string.h>

int sap_wasi_runtime_v0_init(SapWasiRuntimeV0 *runtime, const char *entry_name,
                             sap_wasi_runtime_v0_entry_fn entry_fn, void *entry_ctx)
{
    if (!runtime || !entry_name || !entry_fn)
    {
        return SAP_ERROR;
    }
    memset(runtime, 0, sizeof(*runtime));
    runtime->entry_name = entry_name;
    runtime->entry_fn = entry_fn;
    runtime->entry_ctx = entry_ctx;
    runtime->calls = 0u;
    runtime->last_rc = SAP_OK;
    return SAP_OK;
}

int sap_wasi_runtime_v0_invoke(SapWasiRuntimeV0 *runtime, const SapRunnerMessageV0 *msg,
                               uint8_t *reply_buf, uint32_t reply_buf_cap, uint32_t *reply_len_out)
{
    int rc;

    if (!runtime || !runtime->entry_name || !runtime->entry_fn || !msg || !reply_buf ||
        !reply_len_out)
    {
        return SAP_ERROR;
    }

    *reply_len_out = 0u;
    rc = runtime->entry_fn(runtime->entry_ctx, msg->payload, msg->payload_len, reply_buf,
                           reply_buf_cap, reply_len_out);

    runtime->calls++;
    runtime->last_rc = rc;

    if (rc != SAP_OK)
    {
        return rc;
    }
    if (*reply_len_out > reply_buf_cap)
    {
        runtime->last_rc = SAP_ERROR;
        return SAP_ERROR;
    }
    return SAP_OK;
}
