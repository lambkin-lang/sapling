/*
 * wasi_runtime_test.c - tests for concrete wasi runtime wrapper
 *
 * SPDX-License-Identifier: MIT
 */
#include "wasi/runtime_v0.h"

#include <stdint.h>
#include <string.h>

#define CHECK(cond)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(cond))                                                                               \
        {                                                                                          \
            return __LINE__;                                                                       \
        }                                                                                          \
    } while (0)

typedef struct
{
    int rc;
    uint8_t reply[16];
    uint32_t reply_len;
    uint32_t calls;
    uint8_t last_req[16];
    uint32_t last_req_len;
} RuntimeCtx;

typedef struct
{
    int rc;
    uint32_t calls;
    uint32_t last_req_len;
} RuntimeStreamCtx;

static int runtime_entry(void *ctx, const uint8_t *request, uint32_t request_len,
                         uint8_t *reply_buf, uint32_t reply_buf_cap, uint32_t *reply_len_out)
{
    RuntimeCtx *rt = (RuntimeCtx *)ctx;
    if (!rt || !reply_buf || !reply_len_out)
    {
        return SAP_ERROR;
    }

    rt->calls++;
    rt->last_req_len = request_len;
    if (request_len > sizeof(rt->last_req))
    {
        return SAP_ERROR;
    }
    if (request_len > 0u && request != NULL)
    {
        memcpy(rt->last_req, request, request_len);
    }

    if (rt->rc != SAP_OK)
    {
        return rt->rc;
    }
    if (rt->reply_len > reply_buf_cap)
    {
        return SAP_ERROR;
    }
    memcpy(reply_buf, rt->reply, rt->reply_len);
    *reply_len_out = rt->reply_len;
    return SAP_OK;
}

static int runtime_stream_entry(void *ctx, const uint8_t *request, uint32_t request_len,
                                sap_wasi_runtime_v0_write_fn write, void *write_ctx,
                                uint32_t *reply_len_out)
{
    static const uint8_t k_a[] = {'o', 'k'};
    static const uint8_t k_b[] = {'!', '!'};
    RuntimeStreamCtx *rt = (RuntimeStreamCtx *)ctx;
    int rc;

    if (!rt || !write || !reply_len_out)
    {
        return SAP_ERROR;
    }
    rt->calls++;
    rt->last_req_len = request_len;
    if (request_len > 0u && !request)
    {
        return SAP_ERROR;
    }
    if (rt->rc != SAP_OK)
    {
        return rt->rc;
    }

    rc = write(k_a, (uint32_t)sizeof(k_a), write_ctx);
    if (rc != SAP_OK)
    {
        return rc;
    }
    rc = write(k_b, (uint32_t)sizeof(k_b), write_ctx);
    if (rc != SAP_OK)
    {
        return rc;
    }
    *reply_len_out = (uint32_t)sizeof(k_a) + (uint32_t)sizeof(k_b);
    return SAP_OK;
}

static int test_runtime_invoke_success(void)
{
    RuntimeCtx ctx = {0};
    SapWasiRuntimeV0 runtime = {0};
    SapRunnerMessageV0 msg = {0};
    uint8_t reply[32];
    uint32_t reply_len = 0u;

    ctx.rc = SAP_OK;
    ctx.reply[0] = 'o';
    ctx.reply[1] = 'k';
    ctx.reply_len = 2u;

    msg.payload = (const uint8_t *)"in";
    msg.payload_len = 2u;

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", runtime_entry, &ctx) == SAP_OK);
    CHECK(sap_wasi_runtime_v0_invoke(&runtime, &msg, reply, sizeof(reply), &reply_len) == SAP_OK);
    CHECK(reply_len == 2u);
    CHECK(memcmp(reply, "ok", 2u) == 0);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == SAP_OK);
    CHECK(ctx.calls == 1u);
    CHECK(ctx.last_req_len == 2u);
    CHECK(memcmp(ctx.last_req, "in", 2u) == 0);
    return 0;
}

static int test_runtime_invoke_error(void)
{
    RuntimeCtx ctx = {0};
    SapWasiRuntimeV0 runtime = {0};
    SapRunnerMessageV0 msg = {0};
    uint8_t reply[32];
    uint32_t reply_len = 0u;

    ctx.rc = SAP_CONFLICT;
    ctx.reply_len = 0u;
    msg.payload = NULL;
    msg.payload_len = 0u;

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", runtime_entry, &ctx) == SAP_OK);
    CHECK(sap_wasi_runtime_v0_invoke(&runtime, &msg, reply, sizeof(reply), &reply_len) ==
          SAP_CONFLICT);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == SAP_CONFLICT);
    CHECK(ctx.calls == 1u);
    CHECK(reply_len == 0u);
    return 0;
}

static int test_runtime_adapter_stream_success(void)
{
    static const SapWasiRuntimeV0Adapter adapter = {
        "stream-adapter",
        NULL,
        runtime_stream_entry,
    };
    RuntimeStreamCtx ctx = {0};
    SapWasiRuntimeV0 runtime = {0};
    SapRunnerMessageV0 msg = {0};
    uint8_t reply[8];
    uint32_t reply_len = 0u;

    ctx.rc = SAP_OK;
    msg.payload = (const uint8_t *)"in";
    msg.payload_len = 2u;

    CHECK(sap_wasi_runtime_v0_init_adapter(&runtime, "guest.main", &adapter, &ctx) == SAP_OK);
    CHECK(sap_wasi_runtime_v0_invoke(&runtime, &msg, reply, sizeof(reply), &reply_len) == SAP_OK);
    CHECK(reply_len == 4u);
    CHECK(memcmp(reply, "ok!!", 4u) == 0);
    CHECK(ctx.calls == 1u);
    CHECK(ctx.last_req_len == 2u);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == SAP_OK);
    return 0;
}

static int test_runtime_adapter_stream_reply_overflow(void)
{
    static const SapWasiRuntimeV0Adapter adapter = {
        "stream-adapter",
        NULL,
        runtime_stream_entry,
    };
    RuntimeStreamCtx ctx = {0};
    SapWasiRuntimeV0 runtime = {0};
    SapRunnerMessageV0 msg = {0};
    uint8_t reply[3];
    uint32_t reply_len = 0u;

    ctx.rc = SAP_OK;
    msg.payload = (const uint8_t *)"in";
    msg.payload_len = 2u;

    CHECK(sap_wasi_runtime_v0_init_adapter(&runtime, "guest.main", &adapter, &ctx) == SAP_OK);
    CHECK(sap_wasi_runtime_v0_invoke(&runtime, &msg, reply, sizeof(reply), &reply_len) == SAP_FULL);
    CHECK(ctx.calls == 1u);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == SAP_FULL);
    return 0;
}

int main(void)
{
    if (test_runtime_invoke_success() != 0)
    {
        return 1;
    }
    if (test_runtime_invoke_error() != 0)
    {
        return 2;
    }
    if (test_runtime_adapter_stream_success() != 0)
    {
        return 3;
    }
    if (test_runtime_adapter_stream_reply_overflow() != 0)
    {
        return 4;
    }
    return 0;
}
