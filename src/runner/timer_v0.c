/*
 * timer_v0.c - phase-C timer ingestion and due-drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/timer_v0.h"

#include "generated/wit_schema_dbis.h"

#include <stdlib.h>
#include <string.h>

static uint64_t rd64be(const uint8_t *p)
{
    uint64_t v = 0u;
    uint32_t i;
    for (i = 0u; i < 8u; i++)
    {
        v = (v << 8) | (uint64_t)p[i];
    }
    return v;
}

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
        return SAP_ERROR;
    }
    dst = (uint8_t *)malloc((size_t)len);
    if (!dst)
    {
        return SAP_ERROR;
    }
    memcpy(dst, src, len);
    *dst_out = dst;
    return SAP_OK;
}

static int read_next_due_timer(DB *db, int64_t now_ts, uint8_t **key_out, uint32_t *key_len_out,
                               uint8_t **payload_out, uint32_t *payload_len_out)
{
    Txn *txn;
    Cursor *cur;
    int rc;
    const void *key = NULL;
    uint32_t key_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int64_t due_ts = 0;
    uint64_t seq = 0u;

    if (!db || !key_out || !key_len_out || !payload_out || !payload_len_out)
    {
        return SAP_ERROR;
    }
    *key_out = NULL;
    *payload_out = NULL;
    *key_len_out = 0u;
    *payload_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_TIMERS);
    if (!cur)
    {
        txn_abort(txn);
        return SAP_ERROR;
    }

    rc = cursor_first(cur);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    rc = cursor_get(cur, &key, &key_len, &val, &val_len);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    if (key_len != SAP_RUNNER_TIMER_KEY_V0_SIZE || val_len == 0u)
    {
        cursor_close(cur);
        txn_abort(txn);
        return SAP_ERROR;
    }

    rc = sap_runner_timer_v0_key_decode((const uint8_t *)key, key_len, &due_ts, &seq);
    (void)seq;
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    if (due_ts > now_ts)
    {
        cursor_close(cur);
        txn_abort(txn);
        return SAP_NOTFOUND;
    }

    rc = copy_bytes((const uint8_t *)key, key_len, key_out);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    rc = copy_bytes((const uint8_t *)val, val_len, payload_out);
    if (rc != SAP_OK)
    {
        free(*key_out);
        *key_out = NULL;
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    *key_len_out = key_len;
    *payload_len_out = val_len;

    cursor_close(cur);
    txn_abort(txn);
    return SAP_OK;
}

static int delete_timer_if_match(DB *db, const uint8_t *key, uint32_t key_len,
                                 const uint8_t *payload, uint32_t payload_len)
{
    Txn *txn;
    const void *current_val = NULL;
    uint32_t current_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !payload || payload_len == 0u)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_TIMERS, key, key_len, &current_val, &current_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (current_len != payload_len || memcmp(current_val, payload, payload_len) != 0)
    {
        txn_abort(txn);
        return SAP_CONFLICT;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_TIMERS, key, key_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

void sap_runner_timer_v0_key_encode(int64_t due_ts, uint64_t seq,
                                    uint8_t out[SAP_RUNNER_TIMER_KEY_V0_SIZE])
{
    if (!out)
    {
        return;
    }
    wr64be(out, (uint64_t)due_ts);
    wr64be(out + 8, seq);
}

int sap_runner_timer_v0_key_decode(const uint8_t *key, uint32_t key_len, int64_t *due_ts_out,
                                   uint64_t *seq_out)
{
    if (!key || key_len != SAP_RUNNER_TIMER_KEY_V0_SIZE || !due_ts_out || !seq_out)
    {
        return SAP_ERROR;
    }
    *due_ts_out = (int64_t)rd64be(key);
    *seq_out = rd64be(key + 8);
    return SAP_OK;
}

int sap_runner_timer_v0_append(DB *db, int64_t due_ts, uint64_t seq, const uint8_t *payload,
                               uint32_t payload_len)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_TIMER_KEY_V0_SIZE];
    int rc;

    if (!db || !payload || payload_len == 0u)
    {
        return SAP_ERROR;
    }

    sap_runner_timer_v0_key_encode(due_ts, seq, key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }
    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_TIMERS, key, sizeof(key), payload, payload_len,
                           SAP_NOOVERWRITE, NULL);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

int sap_runner_timer_v0_drain_due(DB *db, int64_t now_ts, uint32_t max_items,
                                  sap_runner_timer_v0_due_handler handler, void *ctx,
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
        return SAP_ERROR;
    }
    if (max_items == 0u)
    {
        return SAP_OK;
    }

    for (i = 0u; i < max_items; i++)
    {
        uint8_t *key = NULL;
        uint32_t key_len = 0u;
        uint8_t *payload = NULL;
        uint32_t payload_len = 0u;
        int64_t due_ts = 0;
        uint64_t seq = 0u;
        int rc;

        rc = read_next_due_timer(db, now_ts, &key, &key_len, &payload, &payload_len);
        if (rc == SAP_NOTFOUND)
        {
            break;
        }
        if (rc != SAP_OK)
        {
            free(key);
            free(payload);
            return rc;
        }

        rc = sap_runner_timer_v0_key_decode(key, key_len, &due_ts, &seq);
        (void)seq;
        if (rc != SAP_OK)
        {
            free(key);
            free(payload);
            return rc;
        }

        rc = handler(due_ts, payload, payload_len, ctx);
        if (rc != SAP_OK)
        {
            free(key);
            free(payload);
            return rc;
        }

        rc = delete_timer_if_match(db, key, key_len, payload, payload_len);
        free(key);
        free(payload);
        if (rc != SAP_OK)
        {
            return rc;
        }
        processed++;
    }

    if (processed_out)
    {
        *processed_out = processed;
    }
    return SAP_OK;
}

int sap_runner_timer_v0_publisher_init(SapRunnerTimerV0Publisher *publisher, DB *db,
                                       uint64_t initial_seq)
{
    if (!publisher || !db)
    {
        return SAP_ERROR;
    }
    publisher->db = db;
    publisher->next_seq = initial_seq;
    return SAP_OK;
}

int sap_runner_timer_v0_publish_intent(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                       void *ctx)
{
    SapRunnerTimerV0Publisher *publisher = (SapRunnerTimerV0Publisher *)ctx;
    SapRunnerIntentV0 intent = {0};
    int rc;

    if (!publisher || !publisher->db || !intent_frame || intent_frame_len == 0u)
    {
        return SAP_ERROR;
    }
    rc = sap_runner_intent_v0_decode(intent_frame, intent_frame_len, &intent);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }
    if (intent.kind != SAP_RUNNER_INTENT_KIND_TIMER_ARM)
    {
        return SAP_ERROR;
    }
    if ((intent.flags & SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS) == 0u)
    {
        return SAP_ERROR;
    }

    rc = sap_runner_timer_v0_append(publisher->db, intent.due_ts, publisher->next_seq,
                                    intent.message, intent.message_len);
    if (rc != SAP_OK)
    {
        return rc;
    }
    publisher->next_seq++;
    return SAP_OK;
}
