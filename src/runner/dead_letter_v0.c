/*
 * dead_letter_v0.c - phase-C dead-letter move/record helpers
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/dead_letter_v0.h"

#include "generated/wit_schema_dbis.h"
#include "runner/wit_wire_bridge_v0.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

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
        return ERR_INVALID;
    }
    *key_out = NULL;
    *val_out = NULL;
    *key_len_out = 0u;
    *val_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_OOM;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_DEAD_LETTER);
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
    if (key_len != SAP_RUNNER_INBOX_KEY_V0_SIZE || val_len == 0u)
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
    rc = copy_bytes((const uint8_t *)val, val_len, val_out);
    if (rc != ERR_OK)
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
    return ERR_OK;
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
        return ERR_INVALID;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, key_len, &cur, &cur_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (cur_len != val_len || memcmp(cur, val, val_len) != 0)
    {
        txn_abort(txn);
        return ERR_CONFLICT;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, key_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

static int decode_dead_letter_blob(const uint8_t *raw, uint32_t raw_len,
                                   SapRunnerDeadLetterV0Record *record_out,
                                   uint8_t **owned_frame_out)
{
    int rc;
    int64_t failure_code = 0;
    int64_t attempts = 0;
    int64_t failed_at = 0;
    uint8_t *frame = NULL;
    uint32_t frame_len = 0u;

    if (!raw || raw_len == 0u || !record_out || !owned_frame_out)
    {
        return ERR_INVALID;
    }
    *owned_frame_out = NULL;

    if (!sap_runner_wit_wire_v0_value_is_dbi6_dead_letter(raw, raw_len))
    {
        return ERR_CORRUPT;
    }
    rc = sap_runner_wit_wire_v0_decode_dbi6_dead_letter_value_to_wire(
        raw, raw_len, &frame, &frame_len, &failure_code, &attempts, &failed_at);
    (void)failed_at;
    if (rc != ERR_OK)
    {
        free(frame);
        if (rc == ERR_OOM)
        {
            return ERR_OOM;
        }
        return ERR_CORRUPT;
    }
    if (failure_code < INT32_MIN || failure_code > INT32_MAX || attempts < 0 ||
        attempts > UINT32_MAX)
    {
        free(frame);
        return ERR_CORRUPT;
    }
    record_out->failure_rc = (int32_t)failure_code;
    record_out->attempts = (uint32_t)attempts;
    record_out->frame = frame;
    record_out->frame_len = frame_len;
    *owned_frame_out = frame;
    return ERR_OK;
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
    uint8_t *frame_wire = NULL;
    uint32_t frame_wire_len = 0u;
    const void *dlq_val = NULL;
    uint32_t dlq_len = 0u;
    int rc;

    if (!db || !expected_lease)
    {
        return ERR_INVALID;
    }
    rc = sap_thatch_subsystem_init((SapEnv *)db);
    if (rc != ERR_OK)
    {
        return rc;
    }

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    sap_runner_lease_v0_encode(expected_lease, expected_lease_raw);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), &lease_val, &lease_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (lease_len != sizeof(expected_lease_raw) ||
        memcmp(lease_val, expected_lease_raw, sizeof(expected_lease_raw)) != 0)
    {
        txn_abort(txn);
        return ERR_CONFLICT;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &frame, &frame_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (frame_len == 0u)
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    if (sap_runner_wit_wire_v0_value_is_dbi1_inbox(frame, frame_len))
    {
        rc = sap_runner_wit_wire_v0_decode_dbi1_inbox_value_to_wire((const uint8_t *)frame,
                                                                     frame_len, &frame_wire,
                                                                     &frame_wire_len);
        if (rc != ERR_OK)
        {
            free(frame_wire);
            txn_abort(txn);
            if (rc == ERR_OOM)
            {
                return ERR_OOM;
            }
            return ERR_CORRUPT;
        }
        frame = frame_wire;
        frame_len = frame_wire_len;
    }
    else
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    rc = sap_runner_wit_wire_v0_encode_dbi6_dead_letter_value_from_wire(
        (SapTxnCtx *)txn, (const uint8_t *)frame, frame_len, (int64_t)failure_rc, (int64_t)attempts,
        0, &dlq_val, &dlq_len);
    if (rc != ERR_OK)
    {
        free(frame_wire);
        txn_abort(txn);
        return rc;
    }

    rc = txn_put_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, sizeof(key), dlq_val, dlq_len);
    free(frame_wire);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key));
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key));
    if (rc != ERR_OK)
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
        return ERR_INVALID;
    }
    if (max_records == 0u)
    {
        return ERR_OK;
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
        uint8_t *owned_frame = NULL;
        int rc;

        rc = read_next_dead_letter(db, &key, &key_len, &val, &val_len);
        if (rc == ERR_NOT_FOUND)
        {
            break;
        }
        if (rc != ERR_OK)
        {
            free(key);
            free(val);
            return rc;
        }

        rc = sap_runner_v0_inbox_key_decode(key, key_len, &worker_id, &seq);
        if (rc != ERR_OK)
        {
            free(key);
            free(val);
            return rc;
        }
        rc = decode_dead_letter_blob(val, val_len, &record, &owned_frame);
        if (rc != ERR_OK)
        {
            free(key);
            free(val);
            free(owned_frame);
            return rc;
        }

        rc = handler(worker_id, seq, &record, ctx);
        free(owned_frame);
        if (rc != ERR_OK)
        {
            free(key);
            free(val);
            return rc;
        }

        rc = delete_dead_letter_if_match(db, key, key_len, val, val_len);
        free(key);
        free(val);
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

int sap_runner_dead_letter_v0_replay(DB *db, uint64_t worker_id, uint64_t seq, uint64_t replay_seq)
{
    Txn *txn;
    uint8_t dead_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t replay_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *dead_raw = NULL;
    uint32_t dead_raw_len = 0u;
    const void *inbox_val = NULL;
    uint32_t inbox_val_len = 0u;
    SapRunnerDeadLetterV0Record record = {0};
    uint8_t *owned_frame = NULL;
    int rc;

    if (!db)
    {
        return ERR_INVALID;
    }
    rc = sap_thatch_subsystem_init((SapEnv *)db);
    if (rc != ERR_OK)
    {
        return rc;
    }

    sap_runner_v0_inbox_key_encode(worker_id, seq, dead_key);
    sap_runner_v0_inbox_key_encode(worker_id, replay_seq, replay_key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, dead_key, sizeof(dead_key), &dead_raw,
                     &dead_raw_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = decode_dead_letter_blob((const uint8_t *)dead_raw, dead_raw_len, &record, &owned_frame);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        free(owned_frame);
        return rc;
    }

    rc = sap_runner_wit_wire_v0_encode_dbi1_inbox_value_from_wire(
        (SapTxnCtx *)txn, record.frame, record.frame_len, &inbox_val, &inbox_val_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        free(owned_frame);
        return rc;
    }

    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_INBOX, replay_key, sizeof(replay_key), inbox_val,
                           inbox_val_len, SAP_NOOVERWRITE, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        free(owned_frame);
        return rc;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, dead_key, sizeof(dead_key));
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        free(owned_frame);
        return rc;
    }
    rc = txn_commit(txn);
    free(owned_frame);
    return rc;
}
