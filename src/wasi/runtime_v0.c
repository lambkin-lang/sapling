/*
 * runtime_v0.c - concrete phase-A guest invocation runtime wrapper
 *
 * SPDX-License-Identifier: MIT
 */
#include "wasi/runtime_v0.h"

#include <string.h>

typedef struct
{
    uint8_t *buf;
    uint32_t cap;
    uint32_t len;
} StreamCollectCtx;

static int stream_collect_write(const uint8_t *chunk, uint32_t chunk_len, void *ctx)
{
    StreamCollectCtx *collect = (StreamCollectCtx *)ctx;
    if (!collect)
    {
        return SAP_ERROR;
    }
    if (chunk_len == 0u)
    {
        return SAP_OK;
    }
    if (!chunk)
    {
        return SAP_ERROR;
    }
    if (collect->len > collect->cap || chunk_len > collect->cap - collect->len)
    {
        return SAP_FULL;
    }
    memcpy(collect->buf + collect->len, chunk, chunk_len);
    collect->len += chunk_len;
    return SAP_OK;
}

static int legacy_adapter_invoke(void *ctx, const uint8_t *request, uint32_t request_len,
                                 uint8_t *reply_buf, uint32_t reply_buf_cap,
                                 uint32_t *reply_len_out)
{
    SapWasiRuntimeV0 *runtime = (SapWasiRuntimeV0 *)ctx;
    if (!runtime || !runtime->entry_fn_compat)
    {
        return SAP_ERROR;
    }
    return runtime->entry_fn_compat(runtime->entry_ctx_compat, request, request_len, reply_buf,
                                    reply_buf_cap, reply_len_out);
}

static const SapWasiRuntimeV0Adapter k_legacy_adapter = {
    "legacy-callback",
    legacy_adapter_invoke,
    NULL,
};

int sap_wasi_runtime_v0_init_adapter(SapWasiRuntimeV0 *runtime, const char *entry_name,
                                     const SapWasiRuntimeV0Adapter *adapter, void *adapter_ctx)
{
    if (!runtime || !entry_name || !adapter || (!adapter->invoke && !adapter->invoke_stream))
    {
        return SAP_ERROR;
    }
    memset(runtime, 0, sizeof(*runtime));
    runtime->entry_name = entry_name;
    runtime->adapter = adapter;
    runtime->adapter_ctx = adapter_ctx;
    runtime->calls = 0u;
    runtime->last_rc = SAP_OK;
    return SAP_OK;
}

int sap_wasi_runtime_v0_init(SapWasiRuntimeV0 *runtime, const char *entry_name,
                             sap_wasi_runtime_v0_entry_fn entry_fn, void *entry_ctx)
{
    int rc;
    if (!runtime || !entry_name || !entry_fn)
    {
        return SAP_ERROR;
    }
    rc = sap_wasi_runtime_v0_init_adapter(runtime, entry_name, &k_legacy_adapter, runtime);
    if (rc != SAP_OK)
    {
        return rc;
    }
    runtime->entry_fn_compat = entry_fn;
    runtime->entry_ctx_compat = entry_ctx;
    return SAP_OK;
}

int sap_wasi_runtime_v0_invoke(SapWasiRuntimeV0 *runtime, const SapRunnerMessageV0 *msg,
                               uint8_t *reply_buf, uint32_t reply_buf_cap, uint32_t *reply_len_out)
{
    StreamCollectCtx collect = {0};
    uint32_t produced = 0u;
    const uint8_t *request = NULL;
    uint32_t request_len = 0u;
    int rc;

    if (!runtime || !runtime->entry_name || !runtime->adapter || !msg || !reply_buf ||
        !reply_len_out || (!runtime->adapter->invoke && !runtime->adapter->invoke_stream))
    {
        return SAP_ERROR;
    }

    request = msg->payload;
    request_len = msg->payload_len;
    if (!request && request_len > 0u)
    {
        return SAP_ERROR;
    }

    *reply_len_out = 0u;
    if (runtime->adapter->invoke_stream)
    {
        collect.buf = reply_buf;
        collect.cap = reply_buf_cap;
        collect.len = 0u;
        rc = runtime->adapter->invoke_stream(runtime->adapter_ctx, request, request_len,
                                             stream_collect_write, &collect, &produced);
        if (rc == SAP_OK)
        {
            if (produced == 0u || produced == collect.len)
            {
                *reply_len_out = collect.len;
            }
            else
            {
                rc = SAP_ERROR;
            }
        }
        else if (rc == SAP_FULL)
        {
            *reply_len_out = collect.len;
        }
    }
    else
    {
        rc = runtime->adapter->invoke(runtime->adapter_ctx, request, request_len, reply_buf,
                                      reply_buf_cap, reply_len_out);
    }

    runtime->calls++;

    if (rc != SAP_OK)
    {
        runtime->last_rc = rc;
        return rc;
    }
    if (*reply_len_out > reply_buf_cap)
    {
        runtime->last_rc = SAP_ERROR;
        return SAP_ERROR;
    }
    runtime->last_rc = SAP_OK;
    return SAP_OK;
}
