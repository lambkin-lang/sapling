/*
 * shim_v0.c - phase-A WASI invocation shim for runner worker shell
 *
 * SPDX-License-Identifier: MIT
 */
#include "wasi/shim_v0.h"
#include "runner/dedupe_v0.h"
#include "generated/wit_schema_dbis.h"

#include <stdlib.h>
#include <string.h>

static void wr64be(uint8_t *p, uint64_t v)
{
    int i;
    for (i = 7; i >= 0; i--)
    {
        p[i] = (uint8_t)(v & 0xffu);
        v >>= 8;
    }
}

void sap_wasi_shim_v0_outbox_key_encode(uint64_t seq, uint8_t out[SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE])
{
    if (!out)
    {
        return;
    }
    wr64be(out, seq);
}

typedef struct
{
    SapWasiShimV0 *shim;
    SapRunnerV0 *runner;
    const SapRunnerMessageV0 *msg;
    int64_t now_ms;
} WasiShimAtomicCtx;

static int shim_push_reply_intent(SapRunnerTxStackV0 *stack, SapWasiShimV0 *shim,
                                  const SapRunnerV0 *runner, const SapRunnerMessageV0 *msg,
                                  uint32_t reply_len)
{
    SapRunnerMessageV0 out = {0};
    SapRunnerIntentV0 intent = {0};
    uint8_t *frame = NULL;
    uint32_t frame_len;
    int rc;

    if (!stack || !shim || !runner || !msg)
    {
        return ERR_INVALID;
    }

    out.kind = SAP_RUNNER_MESSAGE_KIND_EVENT;
    out.flags = SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER;
    if ((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u)
    {
        out.flags |= SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID;
        out.trace_id = msg->trace_id;
        out.trace_id_len = msg->trace_id_len;
    }
    out.to_worker = msg->route_worker;
    out.route_worker = msg->route_worker;
    out.route_timestamp = msg->route_timestamp;
    out.from_worker = (int64_t)runner->worker_id;
    out.message_id = msg->message_id;
    out.message_id_len = msg->message_id_len;
    out.payload = shim->reply_buf;
    out.payload_len = reply_len;

    frame_len = sap_runner_message_v0_size(&out);
    if (frame_len == 0u)
    {
        return ERR_CORRUPT;
    }
    frame = (uint8_t *)malloc((size_t)frame_len);
    if (!frame)
    {
        return ERR_OOM;
    }
    rc = sap_runner_message_v0_encode(&out, frame, frame_len, &frame_len);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        free(frame);
        return ERR_CORRUPT;
    }

    intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    intent.flags = 0u;
    intent.due_ts = 0;
    intent.message = frame;
    intent.message_len = frame_len;
    rc = sap_runner_txstack_v0_push_intent(stack, &intent);
    free(frame);
    return rc;
}

static int shim_atomic_execute(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    WasiShimAtomicCtx *atomic = (WasiShimAtomicCtx *)ctx;
    SapWasiShimV0 *shim;
    SapHostV0 host_ctx = {0};
    uint32_t reply_len = 0u;
    int rc;

    if (!stack || !atomic || !atomic->shim || !atomic->runner || !atomic->msg)
    {
        return ERR_INVALID;
    }
    shim = atomic->shim;

    sap_host_v0_init(&host_ctx, stack, read_txn, atomic->runner->worker_id, atomic->now_ms);

    if ((atomic->msg->flags & SAP_RUNNER_MESSAGE_FLAG_DEDUPE_REQUIRED) != 0u)
    {
        SapRunnerDedupeV0 dedupe = {0};
        rc = sap_runner_dedupe_v0_get(read_txn, atomic->msg->message_id,
                                      atomic->msg->message_id_len, &dedupe);
        if (rc == ERR_OK && dedupe.accepted)
        {
            return ERR_OK;
        }
    }

    rc = sap_wasi_runtime_v0_invoke(shim->runtime, &host_ctx, atomic->msg, shim->reply_buf,
                                    shim->reply_buf_cap, &reply_len);
    if (rc != ERR_OK)
    {
        return rc;
    }

    if ((atomic->msg->flags & SAP_RUNNER_MESSAGE_FLAG_DEDUPE_REQUIRED) != 0u)
    {
        SapRunnerDedupeV0 dedupe = {0};
        dedupe.accepted = 1;
        dedupe.last_seen_ts = atomic->now_ms;
        // In a real implementation, we would compute a checksum of the reply here.
        // For now, we leave it zeroed.
        rc = sap_runner_dedupe_v0_stage_put(stack, atomic->msg->message_id,
                                            atomic->msg->message_id_len, &dedupe);
        if (rc != ERR_OK)
        {
            return rc;
        }
    }
    if (!shim->emit_outbox_events || reply_len == 0u)
    {
        return ERR_OK;
    }
    if (reply_len > shim->reply_buf_cap)
    {
        return ERR_CORRUPT;
    }
    return shim_push_reply_intent(stack, shim, atomic->runner, atomic->msg, reply_len);
}

void sap_wasi_shim_v0_options_default(SapWasiShimV0Options *options)
{
    if (!options)
    {
        return;
    }
    memset(options, 0, sizeof(*options));
    options->initial_outbox_seq = 0u;
    options->emit_outbox_events = 0;
    options->reply_buf = NULL;
    options->reply_buf_cap = SAP_WASI_SHIM_V0_DEFAULT_REPLY_CAP;
}

int sap_wasi_shim_v0_init_with_options(SapWasiShimV0 *shim, DB *db, SapWasiRuntimeV0 *runtime,
                                       const SapWasiShimV0Options *options)
{
    SapWasiShimV0Options local = {0};
    uint64_t initial_outbox_seq;
    int emit_outbox_events;
    uint8_t *reply_buf = NULL;
    uint32_t reply_buf_cap = 0u;
    int rc;

    if (!shim || !db || !runtime)
    {
        return ERR_INVALID;
    }
    sap_wasi_shim_v0_options_default(&local);
    if (options)
    {
        local = *options;
        if (local.reply_buf && local.reply_buf_cap == 0u)
        {
            return ERR_INVALID;
        }
        if (local.reply_buf_cap == 0u)
        {
            local.reply_buf_cap = SAP_WASI_SHIM_V0_DEFAULT_REPLY_CAP;
        }
    }
    initial_outbox_seq = local.initial_outbox_seq;
    emit_outbox_events = local.emit_outbox_events ? 1 : 0;

    memset(shim, 0, sizeof(*shim));
    shim->db = db;
    shim->runtime = runtime;

    reply_buf_cap = local.reply_buf_cap;
    if (reply_buf_cap == 0u)
    {
        return ERR_INVALID;
    }
    if (local.reply_buf)
    {
        reply_buf = local.reply_buf;
    }
    else if (reply_buf_cap <= SAP_WASI_SHIM_V0_DEFAULT_REPLY_CAP)
    {
        reply_buf = shim->reply_buf_inline;
    }
    else
    {
        return ERR_INVALID;
    }

    shim->reply_buf = reply_buf;
    shim->reply_buf_cap = reply_buf_cap;
    rc = sap_runner_intent_sink_v0_init(&shim->intent_sink, db, initial_outbox_seq, 0u);
    if (rc != ERR_OK)
    {
        return rc;
    }
    sap_runner_attempt_v0_policy_default(&shim->attempt_policy);
    shim->last_attempt_stats.last_rc = ERR_OK;
    shim->next_outbox_seq = initial_outbox_seq;
    shim->emit_outbox_events = emit_outbox_events;
    return ERR_OK;
}

int sap_wasi_shim_v0_init(SapWasiShimV0 *shim, DB *db, SapWasiRuntimeV0 *runtime,
                          uint64_t initial_outbox_seq, int emit_outbox_events)
{
    SapWasiShimV0Options options = {0};
    sap_wasi_shim_v0_options_default(&options);
    options.initial_outbox_seq = initial_outbox_seq;
    options.emit_outbox_events = emit_outbox_events ? 1 : 0;
    return sap_wasi_shim_v0_init_with_options(shim, db, runtime, &options);
}

void sap_wasi_shim_v0_set_attempt_policy(SapWasiShimV0 *shim,
                                         const SapRunnerAttemptV0Policy *policy)
{
    if (!shim)
    {
        return;
    }
    if (!policy)
    {
        sap_runner_attempt_v0_policy_default(&shim->attempt_policy);
        return;
    }
    shim->attempt_policy = *policy;
}

int sap_wasi_shim_v0_runner_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    SapWasiShimV0 *shim = (SapWasiShimV0 *)ctx;
    WasiShimAtomicCtx atomic_ctx = {0};
    SapRunnerAttemptV0Stats stats = {0};
    int rc;

    if (!runner || !msg || !shim || !shim->db || !shim->runtime)
    {
        return ERR_INVALID;
    }

    atomic_ctx.shim = shim;
    atomic_ctx.runner = runner;
    atomic_ctx.msg = msg;
    atomic_ctx.now_ms = 0; // TODO: Pass real time if needed for TTL
    rc =
        sap_runner_attempt_v0_run(shim->db, &shim->attempt_policy, shim_atomic_execute, &atomic_ctx,
                                  sap_runner_intent_sink_v0_publish, &shim->intent_sink, &stats);
    shim->last_attempt_stats = stats;
    shim->next_outbox_seq = shim->intent_sink.outbox.next_seq;
    if (rc != ERR_OK)
    {
        return rc;
    }
    return ERR_OK;
}

int sap_wasi_shim_v0_worker_init(SapRunnerV0Worker *worker, const SapRunnerV0Config *cfg,
                                 SapWasiShimV0 *shim, uint32_t max_batch)
{
    if (!worker || !cfg || !shim)
    {
        return ERR_INVALID;
    }
    return sap_runner_v0_worker_init(worker, cfg, sap_wasi_shim_v0_runner_handler, shim, max_batch);
}
