/*
 * outbox_v0.c - phase-C outbox append/drain and intent publisher scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/outbox_v0.h"

#include "generated/wit_schema_dbis.h"
#include "runner/wit_wire_bridge_v0.h"
#include "sapling/sapling.h"

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

static int copy_bytes(const uint8_t *src, uint32_t len, uint8_t **dst_out)
{
    uint8_t *dst;

    if (!src || !dst_out || len == 0u)
    {
        return ERR_INVALID;
    }
    dst = (uint8_t *)malloc((size_t)len);
    if (!dst)
    {
        return ERR_OOM;
    }
    memcpy(dst, src, len);
    *dst_out = dst;
    return ERR_OK;
}

static int map_bridge_decode_rc(int rc)
{
    if (rc == ERR_OOM)
    {
        return ERR_OOM;
    }
    return ERR_CORRUPT;
}

static int read_next_outbox_frame(DB *db, uint8_t **key_out, uint32_t *key_len_out,
                                  uint8_t **frame_out, uint32_t *frame_len_out)
{
    Txn *txn;
    Cursor *cur;
    int rc;
    const void *key = NULL;
    uint32_t key_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;

    if (!db || !key_out || !key_len_out || !frame_out || !frame_len_out)
    {
        return ERR_INVALID;
    }
    *key_out = NULL;
    *frame_out = NULL;
    *key_len_out = 0u;
    *frame_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_OOM;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_OUTBOX);
    if (!cur)
    {
        txn_abort(txn);
        return ERR_OOM;
    }

    rc = cursor_first(cur);
    if (rc != ERR_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    rc = cursor_get(cur, &key, &key_len, &val, &val_len);
    if (rc != ERR_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    if (key_len != SAP_RUNNER_OUTBOX_KEY_V0_SIZE || val_len == 0u)
    {
        cursor_close(cur);
        txn_abort(txn);
        return ERR_CORRUPT;
    }

    rc = copy_bytes((const uint8_t *)key, key_len, key_out);
    if (rc != ERR_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    if (!sap_runner_wit_wire_v0_value_is_dbi2_outbox(val, val_len))
    {
        free(*key_out);
        *key_out = NULL;
        cursor_close(cur);
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    rc = sap_runner_wit_wire_v0_decode_dbi2_outbox_value_to_wire((const uint8_t *)val, val_len,
                                                                  frame_out, frame_len_out, NULL);
    if (rc != ERR_OK)
    {
        free(*key_out);
        *key_out = NULL;
        cursor_close(cur);
        txn_abort(txn);
        return map_bridge_decode_rc(rc);
    }
    *key_len_out = key_len;

    cursor_close(cur);
    txn_abort(txn);
    return ERR_OK;
}

static int delete_outbox_if_match(DB *db, const uint8_t *key, uint32_t key_len,
                                  const uint8_t *frame, uint32_t frame_len)
{
    Txn *txn;
    const void *current_val = NULL;
    uint32_t current_len = 0u;
    const uint8_t *current_wire = NULL;
    uint8_t *current_wire_owned = NULL;
    uint32_t current_wire_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !frame || frame_len == 0u)
    {
        return ERR_INVALID;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_OUTBOX, key, key_len, &current_val, &current_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    if (!sap_runner_wit_wire_v0_value_is_dbi2_outbox(current_val, current_len))
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    rc = sap_runner_wit_wire_v0_decode_dbi2_outbox_value_to_wire(
        (const uint8_t *)current_val, current_len, &current_wire_owned, &current_wire_len, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return map_bridge_decode_rc(rc);
    }
    current_wire = current_wire_owned;

    if (current_wire_len != frame_len || memcmp(current_wire, frame, frame_len) != 0)
    {
        free(current_wire_owned);
        txn_abort(txn);
        return ERR_CONFLICT;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_OUTBOX, key, key_len);
    free(current_wire_owned);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

void sap_runner_outbox_v0_key_encode(uint64_t seq, uint8_t out[SAP_RUNNER_OUTBOX_KEY_V0_SIZE])
{
    if (!out)
    {
        return;
    }
    wr64be(out, seq);
}

int sap_runner_outbox_v0_append_frame(DB *db, uint64_t seq, const uint8_t *frame,
                                      uint32_t frame_len)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_OUTBOX_KEY_V0_SIZE];
    const void *stored_value = NULL;
    uint32_t stored_value_len = 0u;
    int rc;

    if (!db || !frame || frame_len == 0u)
    {
        return ERR_INVALID;
    }
    rc = sap_thatch_subsystem_init((SapEnv *)db);
    if (rc != ERR_OK)
    {
        return rc;
    }

    sap_runner_outbox_v0_key_encode(seq, key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    /* committed_at is not yet sourced from a clock in this path, so keep it at 0. */
    rc = sap_runner_wit_wire_v0_encode_dbi2_outbox_value_from_wire(
        (SapTxnCtx *)txn, frame, frame_len, 0, &stored_value, &stored_value_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_OUTBOX, key, sizeof(key), stored_value,
                           stored_value_len, SAP_NOOVERWRITE, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

int sap_runner_outbox_v0_drain(DB *db, uint32_t max_frames,
                               sap_runner_outbox_v0_frame_handler handler, void *ctx,
                               uint32_t *processed_out)
{
    uint32_t processed = 0u;
    uint32_t i;

    if (processed_out)
    {
        *processed_out = 0u;
    }
    if (!db || !handler)
    {
        return ERR_INVALID;
    }
    if (max_frames == 0u)
    {
        return ERR_OK;
    }

    for (i = 0u; i < max_frames; i++)
    {
        uint8_t *key = NULL;
        uint32_t key_len = 0u;
        uint8_t *frame = NULL;
        uint32_t frame_len = 0u;
        int rc;

        rc = read_next_outbox_frame(db, &key, &key_len, &frame, &frame_len);
        if (rc == ERR_NOT_FOUND)
        {
            break;
        }
        if (rc != ERR_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        rc = handler(frame, frame_len, ctx);
        if (rc != ERR_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        rc = delete_outbox_if_match(db, key, key_len, frame, frame_len);
        free(key);
        free(frame);
        if (rc != ERR_OK)
        {
            return rc;
        }
        processed++;
    }

    if (processed_out)
    {
        *processed_out = processed;
    }
    return ERR_OK;
}

int sap_runner_outbox_v0_publisher_init(SapRunnerOutboxV0Publisher *publisher, DB *db,
                                        uint64_t initial_seq)
{
    if (!publisher || !db)
    {
        return ERR_INVALID;
    }
    publisher->db = db;
    publisher->next_seq = initial_seq;
    return ERR_OK;
}

int sap_runner_outbox_v0_publish_intent(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                        void *ctx)
{
    SapRunnerOutboxV0Publisher *publisher = (SapRunnerOutboxV0Publisher *)ctx;
    SapRunnerIntentV0 intent = {0};
    int rc;

    if (!publisher || !publisher->db || !intent_frame || intent_frame_len == 0u)
    {
        return ERR_INVALID;
    }
    rc = sap_runner_intent_v0_decode(intent_frame, intent_frame_len, &intent);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        return ERR_CORRUPT;
    }
    if (intent.kind != SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT)
    {
        return ERR_INVALID;
    }

    rc = sap_runner_outbox_v0_append_frame(publisher->db, publisher->next_seq, intent.message,
                                           intent.message_len);
    if (rc != ERR_OK)
    {
        return rc;
    }
    publisher->next_seq++;
    return ERR_OK;
}
