/*
 * mailbox_v0.c - phase-C mailbox lease claim/ack/requeue scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/mailbox_v0.h"

#include "generated/wit_schema_dbis.h"

#include <string.h>

static const uint8_t k_lease_magic[4] = {'L', 'S', 'E', '0'};

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

static uint64_t rd64(const uint8_t *p)
{
    uint64_t v = 0u;
    uint32_t i;
    for (i = 0u; i < 8u; i++)
    {
        v |= ((uint64_t)p[i]) << (8u * i);
    }
    return v;
}

static void wr64(uint8_t *p, uint64_t v)
{
    uint32_t i;
    for (i = 0u; i < 8u; i++)
    {
        p[i] = (uint8_t)((v >> (8u * i)) & 0xffu);
    }
}

static void lease_key_encode(uint64_t worker_id, uint64_t seq,
                             uint8_t out[SAP_RUNNER_INBOX_KEY_V0_SIZE])
{
    sap_runner_v0_inbox_key_encode(worker_id, seq, out);
}

void sap_runner_lease_v0_encode(const SapRunnerLeaseV0 *lease,
                                uint8_t out[SAP_RUNNER_LEASE_V0_VALUE_SIZE])
{
    if (!lease || !out)
    {
        return;
    }

    memcpy(out, k_lease_magic, sizeof(k_lease_magic));
    wr64(out + 4, lease->owner_worker);
    wr64(out + 12, (uint64_t)lease->deadline_ts);
    wr32(out + 20, lease->attempts);
}

int sap_runner_lease_v0_decode(const uint8_t *raw, uint32_t raw_len, SapRunnerLeaseV0 *lease_out)
{
    if (!raw || !lease_out || raw_len != SAP_RUNNER_LEASE_V0_VALUE_SIZE)
    {
        return SAP_ERROR;
    }
    if (memcmp(raw, k_lease_magic, sizeof(k_lease_magic)) != 0)
    {
        return SAP_ERROR;
    }

    lease_out->owner_worker = rd64(raw + 4);
    lease_out->deadline_ts = (int64_t)rd64(raw + 12);
    lease_out->attempts = rd32(raw + 20);
    return SAP_OK;
}

int sap_runner_mailbox_v0_claim(DB *db, uint64_t inbox_worker_id, uint64_t seq,
                                uint64_t claimant_worker_id, int64_t now_ts,
                                int64_t lease_deadline_ts, SapRunnerLeaseV0 *lease_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *frame = NULL;
    uint32_t frame_len = 0u;
    const void *lease_val = NULL;
    uint32_t lease_len = 0u;
    SapRunnerLeaseV0 next = {0};
    int rc;

    if (!db || !lease_out)
    {
        return SAP_ERROR;
    }

    lease_key_encode(inbox_worker_id, seq, key);
    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &frame, &frame_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    (void)frame;
    (void)frame_len;

    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), &lease_val, &lease_len);
    if (rc == SAP_NOTFOUND)
    {
        uint8_t raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
        void *reserved_out = NULL;

        next.owner_worker = claimant_worker_id;
        next.deadline_ts = lease_deadline_ts;
        next.attempts = 1u;
        sap_runner_lease_v0_encode(&next, raw);

        rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), raw, sizeof(raw),
                               SAP_NOOVERWRITE, &reserved_out);
        (void)reserved_out;
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            if (rc == SAP_EXISTS)
            {
                return SAP_BUSY;
            }
            return rc;
        }
    }
    else if (rc == SAP_OK)
    {
        SapRunnerLeaseV0 cur = {0};
        uint8_t expected_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
        uint8_t replacement_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];

        rc = sap_runner_lease_v0_decode((const uint8_t *)lease_val, lease_len, &cur);
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }
        if (now_ts <= cur.deadline_ts)
        {
            txn_abort(txn);
            return SAP_BUSY;
        }
        if (cur.attempts == UINT32_MAX)
        {
            txn_abort(txn);
            return SAP_ERROR;
        }

        next.owner_worker = claimant_worker_id;
        next.deadline_ts = lease_deadline_ts;
        next.attempts = cur.attempts + 1u;
        sap_runner_lease_v0_encode(&cur, expected_raw);
        sap_runner_lease_v0_encode(&next, replacement_raw);

        rc = txn_put_if(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), replacement_raw,
                        sizeof(replacement_raw), expected_raw, sizeof(expected_raw));
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            if (rc == SAP_CONFLICT || rc == SAP_NOTFOUND)
            {
                return SAP_BUSY;
            }
            return rc;
        }
    }
    else
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_commit(txn);
    if (rc != SAP_OK)
    {
        return rc;
    }

    *lease_out = next;
    return SAP_OK;
}

int sap_runner_mailbox_v0_ack(DB *db, uint64_t worker_id, uint64_t seq,
                              const SapRunnerLeaseV0 *expected_lease)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t expected_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
    const void *lease_val = NULL;
    uint32_t lease_len = 0u;
    const void *frame = NULL;
    uint32_t frame_len = 0u;
    int rc;

    if (!db || !expected_lease)
    {
        return SAP_ERROR;
    }

    lease_key_encode(worker_id, seq, key);
    sap_runner_lease_v0_encode(expected_lease, expected_raw);

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
    if (lease_len != sizeof(expected_raw) ||
        memcmp(lease_val, expected_raw, sizeof(expected_raw)) != 0)
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
    (void)frame;
    (void)frame_len;

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

int sap_runner_mailbox_v0_requeue(DB *db, uint64_t worker_id, uint64_t seq,
                                  const SapRunnerLeaseV0 *expected_lease, uint64_t new_seq)
{
    Txn *txn;
    uint8_t old_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t new_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t expected_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
    const void *lease_val = NULL;
    uint32_t lease_len = 0u;
    const void *frame = NULL;
    uint32_t frame_len = 0u;
    int rc;

    if (!db || !expected_lease || seq == new_seq)
    {
        return SAP_ERROR;
    }

    lease_key_encode(worker_id, seq, old_key);
    lease_key_encode(worker_id, new_seq, new_key);
    sap_runner_lease_v0_encode(expected_lease, expected_raw);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, old_key, sizeof(old_key), &lease_val, &lease_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (lease_len != sizeof(expected_raw) ||
        memcmp(lease_val, expected_raw, sizeof(expected_raw)) != 0)
    {
        txn_abort(txn);
        return SAP_CONFLICT;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, old_key, sizeof(old_key), &frame, &frame_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_INBOX, new_key, sizeof(new_key), frame, frame_len,
                           SAP_NOOVERWRITE, NULL);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_INBOX, old_key, sizeof(old_key));
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_LEASES, old_key, sizeof(old_key));
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}
