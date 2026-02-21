/*
 * dead_letter_v0.c - phase-C dead-letter move/record helpers
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/dead_letter_v0.h"

#include "generated/wit_schema_dbis.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t k_dead_letter_magic[4] = {'D', 'L', 'Q', '0'};

static uint32_t rd32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
    p[2] = (uint8_t)((v >> 16) & 0xffu);
    p[3] = (uint8_t)((v >> 24) & 0xffu);
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

static int read_next_dead_letter(DB *db, uint8_t **key_out, uint32_t *key_len_out,
                                 uint8_t **val_out, uint32_t *val_len_out)
{
    Txn *txn;
    Cursor *cur;
    int rc;
    const void *key = NULL;
    uint32_t key_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;

    if (!db || !key_out || !key_len_out || !val_out || !val_len_out)
    {
        return SAP_ERROR;
    }
    *key_out = NULL;
    *val_out = NULL;
    *key_len_out = 0u;
    *val_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_DEAD_LETTER);
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
    if (key_len != SAP_RUNNER_INBOX_KEY_V0_SIZE || val_len < SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE)
    {
        cursor_close(cur);
        txn_abort(txn);
        return SAP_ERROR;
    }

    rc = copy_bytes((const uint8_t *)key, key_len, key_out);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    rc = copy_bytes((const uint8_t *)val, val_len, val_out);
    if (rc != SAP_OK)
    {
        free(*key_out);
        *key_out = NULL;
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    *key_len_out = key_len;
    *val_len_out = val_len;
    cursor_close(cur);
    txn_abort(txn);
    return SAP_OK;
}

static int delete_dead_letter_if_match(DB *db, const uint8_t *key, uint32_t key_len,
                                       const uint8_t *val, uint32_t val_len)
{
    Txn *txn;
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !val || val_len == 0u)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, key_len, &cur, &cur_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (cur_len != val_len || memcmp(cur, val, val_len) != 0)
    {
        txn_abort(txn);
        return SAP_CONFLICT;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, key_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

int sap_runner_dead_letter_v0_encode(int32_t failure_rc, uint32_t attempts, const uint8_t *frame,
                                     uint32_t frame_len, uint8_t *dst, uint32_t dst_len,
                                     uint32_t *written_out)
{
    uint32_t total_len;

    if (written_out)
    {
        *written_out = 0u;
    }
    if (!frame || frame_len == 0u || !dst)
    {
        return SAP_ERROR;
    }
    if (frame_len > (UINT32_MAX - SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE))
    {
        return SAP_FULL;
    }

    total_len = SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE + frame_len;
    if (dst_len < total_len)
    {
        return SAP_FULL;
    }

    memcpy(dst, k_dead_letter_magic, sizeof(k_dead_letter_magic));
    wr32(dst + 4, (uint32_t)failure_rc);
    wr32(dst + 8, attempts);
    wr32(dst + 12, frame_len);
    memcpy(dst + SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE, frame, frame_len);
    if (written_out)
    {
        *written_out = total_len;
    }
    return SAP_OK;
}

int sap_runner_dead_letter_v0_decode(const uint8_t *raw, uint32_t raw_len,
                                     SapRunnerDeadLetterV0Record *record_out)
{
    uint32_t frame_len;

    if (!raw || !record_out || raw_len < SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE)
    {
        return SAP_ERROR;
    }
    if (memcmp(raw, k_dead_letter_magic, sizeof(k_dead_letter_magic)) != 0)
    {
        return SAP_ERROR;
    }

    frame_len = rd32(raw + 12);
    if (frame_len == 0u)
    {
        return SAP_ERROR;
    }
    if (raw_len != SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE + frame_len)
    {
        return SAP_ERROR;
    }

    record_out->failure_rc = (int32_t)rd32(raw + 4);
    record_out->attempts = rd32(raw + 8);
    record_out->frame = raw + SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE;
    record_out->frame_len = frame_len;
    return SAP_OK;
}

int sap_runner_dead_letter_v0_move(DB *db, uint64_t worker_id, uint64_t seq,
                                   const SapRunnerLeaseV0 *expected_lease, int32_t failure_rc,
                                   uint32_t attempts)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t expected_lease_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
    const void *lease_val = NULL;
    uint32_t lease_len = 0u;
    const void *frame = NULL;
    uint32_t frame_len = 0u;
    uint8_t *dlq_val = NULL;
    uint32_t dlq_len = 0u;
    int rc;

    if (!db || !expected_lease)
    {
        return SAP_ERROR;
    }

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    sap_runner_lease_v0_encode(expected_lease, expected_lease_raw);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), &lease_val, &lease_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (lease_len != sizeof(expected_lease_raw) ||
        memcmp(lease_val, expected_lease_raw, sizeof(expected_lease_raw)) != 0)
    {
        txn_abort(txn);
        return SAP_CONFLICT;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &frame, &frame_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (frame_len == 0u || frame_len > (UINT32_MAX - SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE))
    {
        txn_abort(txn);
        return SAP_ERROR;
    }

    dlq_len = SAP_RUNNER_DEAD_LETTER_V0_HEADER_SIZE + frame_len;
    dlq_val = (uint8_t *)malloc((size_t)dlq_len);
    if (!dlq_val)
    {
        txn_abort(txn);
        return SAP_ERROR;
    }
    rc = sap_runner_dead_letter_v0_encode(failure_rc, attempts, (const uint8_t *)frame, frame_len,
                                          dlq_val, dlq_len, &dlq_len);
    if (rc != SAP_OK)
    {
        free(dlq_val);
        txn_abort(txn);
        return rc;
    }

    rc = txn_put_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, sizeof(key), dlq_val, dlq_len);
    free(dlq_val);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key));
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key));
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

int sap_runner_dead_letter_v0_drain(DB *db, uint32_t max_records,
                                    sap_runner_dead_letter_v0_record_handler handler, void *ctx,
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
    if (max_records == 0u)
    {
        return SAP_OK;
    }

    for (i = 0u; i < max_records; i++)
    {
        uint8_t *key = NULL;
        uint32_t key_len = 0u;
        uint8_t *val = NULL;
        uint32_t val_len = 0u;
        uint64_t worker_id = 0u;
        uint64_t seq = 0u;
        SapRunnerDeadLetterV0Record record = {0};
        int rc;

        rc = read_next_dead_letter(db, &key, &key_len, &val, &val_len);
        if (rc == SAP_NOTFOUND)
        {
            break;
        }
        if (rc != SAP_OK)
        {
            free(key);
            free(val);
            return rc;
        }

        rc = sap_runner_v0_inbox_key_decode(key, key_len, &worker_id, &seq);
        if (rc != SAP_OK)
        {
            free(key);
            free(val);
            return rc;
        }
        rc = sap_runner_dead_letter_v0_decode(val, val_len, &record);
        if (rc != SAP_OK)
        {
            free(key);
            free(val);
            return rc;
        }

        rc = handler(worker_id, seq, &record, ctx);
        if (rc != SAP_OK)
        {
            free(key);
            free(val);
            return rc;
        }

        rc = delete_dead_letter_if_match(db, key, key_len, val, val_len);
        free(key);
        free(val);
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

int sap_runner_dead_letter_v0_replay(DB *db, uint64_t worker_id, uint64_t seq, uint64_t replay_seq)
{
    Txn *txn;
    uint8_t dead_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t replay_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *dead_raw = NULL;
    uint32_t dead_raw_len = 0u;
    SapRunnerDeadLetterV0Record record = {0};
    int rc;

    if (!db)
    {
        return SAP_ERROR;
    }

    sap_runner_v0_inbox_key_encode(worker_id, seq, dead_key);
    sap_runner_v0_inbox_key_encode(worker_id, replay_seq, replay_key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, dead_key, sizeof(dead_key), &dead_raw,
                     &dead_raw_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = sap_runner_dead_letter_v0_decode((const uint8_t *)dead_raw, dead_raw_len, &record);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_INBOX, replay_key, sizeof(replay_key), record.frame,
                           record.frame_len, SAP_NOOVERWRITE, NULL);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, dead_key, sizeof(dead_key));
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}
