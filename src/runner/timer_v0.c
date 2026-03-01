/*
 * timer_v0.c - phase-C timer ingestion and due-drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/timer_v0.h"

#include "runner/wire_v0.h"
#include "sapling/bept.h"
#include "generated/wit_schema_dbis.h"

#include <stdlib.h>
#include <string.h>

#define TIMER_BEPT_KEY_WORDS 4u
#define TIMER_REPAIR_LIMIT 1024u

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

/* Helper to convert timer (ts, seq) to BEPT-compatible ordered 128-bit key (4 x 32-bit words). */
void sap_runner_timer_v0_bept_key_encode(int64_t due_ts, uint64_t seq, uint32_t out[4])
{
    uint64_t ts_encoded;

    /* Flip sign bit of signed int64 so unsigned lexicographic ordering matches signed time order. */
    ts_encoded = (uint64_t)due_ts ^ 0x8000000000000000ULL;

    out[0] = (uint32_t)(ts_encoded >> 32);
    out[1] = (uint32_t)(ts_encoded & 0xFFFFFFFFu);
    out[2] = (uint32_t)(seq >> 32);
    out[3] = (uint32_t)(seq & 0xFFFFFFFFu);
}

void sap_runner_timer_v0_bept_key_decode(const uint32_t key[4], int64_t *due_ts_out, uint64_t *seq_out)
{
    uint64_t ts_encoded = ((uint64_t)key[0] << 32) | key[1];

    if (due_ts_out)
    {
        *due_ts_out = (int64_t)(ts_encoded ^ 0x8000000000000000ULL);
    }
    if (seq_out)
    {
        *seq_out = ((uint64_t)key[2] << 32) | key[3];
    }
}

/* Encode to legacy byte format (for external consumers). */
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
        return ERR_INVALID;
    }
    *due_ts_out = (int64_t)rd64be(key);
    *seq_out = rd64be(key + 8);
    return ERR_OK;
}

static int timer_key_bytes_to_bept_key(const uint8_t *key_bytes, uint32_t key_len,
                                       uint32_t bept_key_out[TIMER_BEPT_KEY_WORDS])
{
    int64_t due_ts = 0;
    uint64_t seq = 0u;
    int rc;

    rc = sap_runner_timer_v0_key_decode(key_bytes, key_len, &due_ts, &seq);
    if (rc != ERR_OK)
    {
        return rc;
    }
    sap_runner_timer_v0_bept_key_encode(due_ts, seq, bept_key_out);
    return ERR_OK;
}

static int upsert_bept_row(DB *db, const uint32_t bept_key[TIMER_BEPT_KEY_WORDS],
                           const uint8_t *payload, uint32_t payload_len)
{
    Txn *txn;
    int rc;

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = sap_bept_put(txn, bept_key, TIMER_BEPT_KEY_WORDS, payload, payload_len, 0u, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    return txn_commit(txn);
}

static int prune_bept_row(DB *db, const uint32_t bept_key[TIMER_BEPT_KEY_WORDS])
{
    Txn *txn;
    int rc;

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = sap_bept_del(txn, bept_key, TIMER_BEPT_KEY_WORDS);
    if (rc != ERR_OK && rc != ERR_NOT_FOUND)
    {
        txn_abort(txn);
        return rc;
    }
    return txn_commit(txn);
}

static int seed_bept_from_first_timer_row(DB *db)
{
    Txn *txn;
    Cursor *cur;
    const void *key = NULL;
    uint32_t key_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;
    uint32_t bept_key[TIMER_BEPT_KEY_WORDS];
    uint8_t *payload_copy = NULL;
    int rc;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_OOM;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_TIMERS);
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
    if (val_len == 0u)
    {
        cursor_close(cur);
        txn_abort(txn);
        return ERR_CORRUPT;
    }

    rc = timer_key_bytes_to_bept_key((const uint8_t *)key, key_len, bept_key);
    if (rc == ERR_OK)
    {
        rc = copy_bytes((const uint8_t *)val, val_len, &payload_copy);
    }

    cursor_close(cur);
    txn_abort(txn);

    if (rc != ERR_OK)
    {
        free(payload_copy);
        return rc;
    }

    rc = upsert_bept_row(db, bept_key, payload_copy, val_len);
    free(payload_copy);
    return rc;
}

static int read_valid_min_timer(DB *db, int64_t *due_ts_out, uint64_t *seq_out,
                                uint8_t **payload_out, uint32_t *payload_len_out)
{
    uint32_t repairs;

    if (!db || !due_ts_out || !seq_out || !payload_out || !payload_len_out)
    {
        return ERR_INVALID;
    }
    *payload_out = NULL;
    *payload_len_out = 0u;

    for (repairs = 0u; repairs < TIMER_REPAIR_LIMIT; repairs++)
    {
        Txn *txn;
        uint32_t bept_key[TIMER_BEPT_KEY_WORDS];
        const void *bept_val = NULL;
        uint32_t bept_len = 0u;
        int64_t due_ts = 0;
        uint64_t seq = 0u;
        uint8_t timer_key[SAP_RUNNER_TIMER_KEY_V0_SIZE];
        const void *db_val = NULL;
        uint32_t db_len = 0u;
        Cursor *dbi_cur = NULL;
        const void *dbi_first_key = NULL;
        uint32_t dbi_first_key_len = 0u;
        const void *dbi_first_val = NULL;
        uint32_t dbi_first_val_len = 0u;
        uint8_t *db_copy = NULL;
        int payload_mismatch = 0;
        int rc;

        txn = txn_begin(db, NULL, TXN_RDONLY);
        if (!txn)
        {
            return ERR_OOM;
        }

        rc = sap_bept_min(txn, bept_key, TIMER_BEPT_KEY_WORDS, &bept_val, &bept_len);
        if (rc == ERR_NOT_FOUND)
        {
            txn_abort(txn);
            rc = seed_bept_from_first_timer_row(db);
            if (rc == ERR_NOT_FOUND)
            {
                return ERR_NOT_FOUND;
            }
            if (rc != ERR_OK)
            {
                return rc;
            }
            continue;
        }
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            return rc;
        }

        sap_runner_timer_v0_bept_key_decode(bept_key, &due_ts, &seq);
        sap_runner_timer_v0_key_encode(due_ts, seq, timer_key);

        rc = txn_get_dbi(txn, SAP_WIT_DBI_TIMERS, timer_key, SAP_RUNNER_TIMER_KEY_V0_SIZE,
                         &db_val, &db_len);
        if (rc == ERR_NOT_FOUND)
        {
            txn_abort(txn);
            rc = prune_bept_row(db, bept_key);
            if (rc != ERR_OK)
            {
                return rc;
            }
            continue;
        }
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            return rc;
        }
        if (db_len == 0u)
        {
            txn_abort(txn);
            return ERR_CORRUPT;
        }

        dbi_cur = cursor_open_dbi(txn, SAP_WIT_DBI_TIMERS);
        if (!dbi_cur)
        {
            txn_abort(txn);
            return ERR_OOM;
        }
        rc = cursor_first(dbi_cur);
        if (rc != ERR_OK)
        {
            cursor_close(dbi_cur);
            txn_abort(txn);
            return rc;
        }
        rc = cursor_get(dbi_cur, &dbi_first_key, &dbi_first_key_len, &dbi_first_val, &dbi_first_val_len);
        if (rc != ERR_OK)
        {
            cursor_close(dbi_cur);
            txn_abort(txn);
            return rc;
        }
        if (dbi_first_key_len != SAP_RUNNER_TIMER_KEY_V0_SIZE || dbi_first_val_len == 0u)
        {
            cursor_close(dbi_cur);
            txn_abort(txn);
            return ERR_CORRUPT;
        }
        if (memcmp(dbi_first_key, timer_key, SAP_RUNNER_TIMER_KEY_V0_SIZE) != 0)
        {
            uint32_t first_bept_key[TIMER_BEPT_KEY_WORDS];
            uint8_t *first_copy = NULL;

            rc = timer_key_bytes_to_bept_key((const uint8_t *)dbi_first_key, dbi_first_key_len,
                                             first_bept_key);
            if (rc == ERR_OK)
            {
                rc = copy_bytes((const uint8_t *)dbi_first_val, dbi_first_val_len, &first_copy);
            }
            cursor_close(dbi_cur);
            txn_abort(txn);
            if (rc != ERR_OK)
            {
                free(first_copy);
                return rc;
            }
            rc = upsert_bept_row(db, first_bept_key, first_copy, dbi_first_val_len);
            free(first_copy);
            if (rc != ERR_OK)
            {
                return rc;
            }
            continue;
        }
        cursor_close(dbi_cur);

        payload_mismatch = (bept_len != db_len) || (memcmp(bept_val, db_val, db_len) != 0);

        rc = copy_bytes((const uint8_t *)db_val, db_len, &db_copy);
        txn_abort(txn);
        if (rc != ERR_OK)
        {
            return rc;
        }

        if (payload_mismatch)
        {
            rc = upsert_bept_row(db, bept_key, db_copy, db_len);
            free(db_copy);
            if (rc != ERR_OK)
            {
                return rc;
            }
            continue;
        }

        *due_ts_out = due_ts;
        *seq_out = seq;
        *payload_out = db_copy;
        *payload_len_out = db_len;
        return ERR_OK;
    }

    return ERR_CORRUPT;
}

int sap_runner_timer_v0_sync_index(DB *db)
{
    Txn *txn;
    Cursor *cur;
    int rc;

    if (!db)
    {
        return ERR_INVALID;
    }

    rc = sap_bept_subsystem_init((SapEnv *)db);
    if (rc != ERR_OK)
    {
        return rc;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = sap_bept_clear(txn);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_TIMERS);
    if (!cur)
    {
        txn_abort(txn);
        return ERR_OOM;
    }

    rc = cursor_first(cur);
    if (rc == ERR_NOT_FOUND)
    {
        cursor_close(cur);
        return txn_commit(txn);
    }
    if (rc != ERR_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    for (;;)
    {
        const void *key = NULL;
        uint32_t key_len = 0u;
        const void *val = NULL;
        uint32_t val_len = 0u;
        uint32_t bept_key[TIMER_BEPT_KEY_WORDS];

        rc = cursor_get(cur, &key, &key_len, &val, &val_len);
        if (rc != ERR_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
        if (val_len == 0u)
        {
            cursor_close(cur);
            txn_abort(txn);
            return ERR_CORRUPT;
        }

        rc = timer_key_bytes_to_bept_key((const uint8_t *)key, key_len, bept_key);
        if (rc != ERR_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return ERR_CORRUPT;
        }

        rc = sap_bept_put(txn, bept_key, TIMER_BEPT_KEY_WORDS, val, val_len, 0u, NULL);
        if (rc != ERR_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }

        rc = cursor_next(cur);
        if (rc == ERR_NOT_FOUND)
        {
            cursor_close(cur);
            return txn_commit(txn);
        }
        if (rc != ERR_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
    }
}

int sap_runner_timer_v0_next_due(DB *db, int64_t *due_ts_out)
{
    uint64_t seq = 0u;
    uint8_t *payload = NULL;
    uint32_t payload_len = 0u;
    int rc;

    if (!db || !due_ts_out)
    {
        return ERR_INVALID;
    }
    *due_ts_out = 0;

    rc = read_valid_min_timer(db, due_ts_out, &seq, &payload, &payload_len);
    (void)seq;
    (void)payload_len;
    free(payload);
    return rc;
}

static int read_next_due_timer(DB *db, int64_t now_ts, uint8_t **key_out, uint32_t *key_len_out,
                               uint8_t **payload_out, uint32_t *payload_len_out)
{
    int64_t due_ts = 0;
    uint64_t seq = 0u;
    int rc;

    if (!db || !key_out || !key_len_out || !payload_out || !payload_len_out)
    {
        return ERR_INVALID;
    }
    *key_out = NULL;
    *key_len_out = 0u;
    *payload_out = NULL;
    *payload_len_out = 0u;

    rc = read_valid_min_timer(db, &due_ts, &seq, payload_out, payload_len_out);
    if (rc != ERR_OK)
    {
        return rc;
    }

    if (due_ts > now_ts)
    {
        free(*payload_out);
        *payload_out = NULL;
        *payload_len_out = 0u;
        return ERR_NOT_FOUND;
    }

    *key_out = (uint8_t *)malloc(SAP_RUNNER_TIMER_KEY_V0_SIZE);
    if (!*key_out)
    {
        free(*payload_out);
        *payload_out = NULL;
        *payload_len_out = 0u;
        return ERR_OOM;
    }
    sap_runner_timer_v0_key_encode(due_ts, seq, *key_out);
    *key_len_out = SAP_RUNNER_TIMER_KEY_V0_SIZE;
    return ERR_OK;
}

static int delete_timer_if_match(DB *db, const uint8_t *key_bytes, uint32_t key_len,
                                 const uint8_t *payload, uint32_t payload_len)
{
    Txn *txn;
    const void *current_val = NULL;
    uint32_t current_len = 0u;
    uint32_t bept_key[TIMER_BEPT_KEY_WORDS];
    int rc;

    if (!db || !key_bytes || key_len != SAP_RUNNER_TIMER_KEY_V0_SIZE || !payload || payload_len == 0u)
    {
        return ERR_INVALID;
    }

    rc = timer_key_bytes_to_bept_key(key_bytes, key_len, bept_key);
    if (rc != ERR_OK)
    {
        return rc;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_TIMERS, key_bytes, key_len, &current_val, &current_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    if (current_len != payload_len || memcmp(current_val, payload, payload_len) != 0)
    {
        txn_abort(txn);
        return ERR_CONFLICT;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_TIMERS, key_bytes, key_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = sap_bept_del(txn, bept_key, TIMER_BEPT_KEY_WORDS);
    if (rc != ERR_OK && rc != ERR_NOT_FOUND)
    {
        txn_abort(txn);
        return rc;
    }

    return txn_commit(txn);
}

int sap_runner_timer_v0_append(DB *db, int64_t due_ts, uint64_t seq, const uint8_t *payload,
                               uint32_t payload_len)
{
    Txn *txn;
    uint8_t timer_key[SAP_RUNNER_TIMER_KEY_V0_SIZE];
    uint32_t bept_key[TIMER_BEPT_KEY_WORDS];
    int rc;

    if (!db || !payload || payload_len == 0u)
    {
        return ERR_INVALID;
    }

    sap_runner_timer_v0_key_encode(due_ts, seq, timer_key);
    sap_runner_timer_v0_bept_key_encode(due_ts, seq, bept_key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_TIMERS, timer_key, SAP_RUNNER_TIMER_KEY_V0_SIZE,
                           payload, payload_len, SAP_NOOVERWRITE, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = sap_bept_put(txn, bept_key, TIMER_BEPT_KEY_WORDS, payload, payload_len, 0u, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    return txn_commit(txn);
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
        return ERR_INVALID;
    }
    if (max_items == 0u)
    {
        return ERR_OK;
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
        if (rc == ERR_NOT_FOUND)
        {
            break;
        }
        if (rc != ERR_OK)
        {
            free(key);
            free(payload);
            return rc;
        }

        rc = sap_runner_timer_v0_key_decode(key, key_len, &due_ts, &seq);
        if (rc != ERR_OK)
        {
            free(key);
            free(payload);
            return rc;
        }

        rc = handler(due_ts, seq, payload, payload_len, ctx);
        if (rc != ERR_OK)
        {
            free(key);
            free(payload);
            return rc;
        }

        rc = delete_timer_if_match(db, key, key_len, payload, payload_len);
        free(key);
        free(payload);
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

int sap_runner_timer_v0_publisher_init(SapRunnerTimerV0Publisher *publisher, DB *db,
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

int sap_runner_timer_v0_publish_intent(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                       void *ctx)
{
    SapRunnerTimerV0Publisher *publisher = (SapRunnerTimerV0Publisher *)ctx;
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
    if (intent.kind != SAP_RUNNER_INTENT_KIND_TIMER_ARM)
    {
        return ERR_INVALID;
    }
    if ((intent.flags & SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS) == 0u)
    {
        return ERR_INVALID;
    }

    rc = sap_runner_timer_v0_append(publisher->db, intent.due_ts, publisher->next_seq,
                                    intent.message, intent.message_len);
    if (rc != ERR_OK)
    {
        return rc;
    }
    publisher->next_seq++;
    return ERR_OK;
}
