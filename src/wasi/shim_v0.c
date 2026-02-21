/*
 * shim_v0.c - phase-A WASI invocation shim for runner worker shell
 *
 * SPDX-License-Identifier: MIT
 */
#include "wasi/shim_v0.h"

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

static int outbox_put_frame(DB *db, uint64_t seq, const uint8_t *frame, uint32_t frame_len)
{
    Txn *txn;
    uint8_t key[SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE];
    int rc;

    if (!db || !frame || frame_len == 0u)
    {
        return SAP_ERROR;
    }

    sap_wasi_shim_v0_outbox_key_encode(seq, key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }
    rc = txn_put_dbi(txn, SAP_WIT_DBI_OUTBOX, key, sizeof(key), frame, frame_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

int sap_wasi_shim_v0_init(SapWasiShimV0 *shim, DB *db, SapWasiRuntimeV0 *runtime,
                          uint64_t initial_outbox_seq, int emit_outbox_events)
{
    if (!shim || !db || !runtime)
    {
        return SAP_ERROR;
    }

    memset(shim, 0, sizeof(*shim));
    shim->db = db;
    shim->runtime = runtime;
    shim->next_outbox_seq = initial_outbox_seq;
    shim->emit_outbox_events = emit_outbox_events ? 1 : 0;
    return SAP_OK;
}

int sap_wasi_shim_v0_runner_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    SapWasiShimV0 *shim = (SapWasiShimV0 *)ctx;
    uint32_t reply_len = 0u;
    int rc;

    if (!runner || !msg || !shim || !shim->db || !shim->runtime)
    {
        return SAP_ERROR;
    }

    rc = sap_wasi_runtime_v0_invoke(shim->runtime, msg, shim->reply_buf, sizeof(shim->reply_buf),
                                    &reply_len);
    if (rc != SAP_OK)
    {
        return rc;
    }

    if (!shim->emit_outbox_events || reply_len == 0u)
    {
        return SAP_OK;
    }
    if (reply_len > sizeof(shim->reply_buf))
    {
        return SAP_ERROR;
    }

    {
        SapRunnerMessageV0 out = {0};
        uint32_t frame_len;
        uint8_t *frame;

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
            return SAP_ERROR;
        }
        frame = (uint8_t *)malloc((size_t)frame_len);
        if (!frame)
        {
            return SAP_ERROR;
        }
        rc = sap_runner_message_v0_encode(&out, frame, frame_len, &frame_len);
        if (rc != SAP_RUNNER_WIRE_OK)
        {
            free(frame);
            return SAP_ERROR;
        }
        rc = outbox_put_frame(shim->db, shim->next_outbox_seq, frame, frame_len);
        free(frame);
        if (rc != SAP_OK)
        {
            return rc;
        }
    }

    shim->next_outbox_seq++;
    return SAP_OK;
}

int sap_wasi_shim_v0_worker_init(SapRunnerV0Worker *worker, const SapRunnerV0Config *cfg,
                                 SapWasiShimV0 *shim, uint32_t max_batch)
{
    if (!worker || !cfg || !shim)
    {
        return SAP_ERROR;
    }
    return sap_runner_v0_worker_init(worker, cfg, sap_wasi_shim_v0_runner_handler, shim, max_batch);
}
