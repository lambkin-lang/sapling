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
