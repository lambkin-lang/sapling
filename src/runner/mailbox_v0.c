/*
 * mailbox_v0.c - phase-C mailbox lease claim/ack/requeue scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/mailbox_v0.h"
#include "generated/wit_schema_dbis.h"

#include <string.h>

static void lease_key_encode(uint64_t worker_id, uint64_t seq,
                             uint8_t out[SAP_RUNNER_INBOX_KEY_V0_SIZE])
{
    sap_runner_v0_inbox_key_encode(worker_id, seq, out);
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
        return ERR_INVALID;
    }

    lease_key_encode(inbox_worker_id, seq, key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &frame, &frame_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), &lease_val, &lease_len);
    if (rc == ERR_NOT_FOUND)
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
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            if (rc == ERR_EXISTS)
            {
                return ERR_BUSY;
            }
            return rc;
        }
    }
    else if (rc == ERR_OK)
    {
        SapRunnerLeaseV0 cur = {0};
        uint8_t expected_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
        uint8_t replacement_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];

        rc = sap_runner_lease_v0_decode((const uint8_t *)lease_val, lease_len, &cur);
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            return rc;
        }
        if (now_ts <= cur.deadline_ts)
        {
            txn_abort(txn);
            return ERR_BUSY;
        }

        next.owner_worker = claimant_worker_id;
        next.deadline_ts = lease_deadline_ts;
        next.attempts = cur.attempts + 1u;
        sap_runner_lease_v0_encode(&cur, expected_raw);
        sap_runner_lease_v0_encode(&next, replacement_raw);

        rc = txn_put_if(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), replacement_raw,
                        sizeof(replacement_raw), expected_raw, sizeof(expected_raw));
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            if (rc == ERR_CONFLICT || rc == ERR_NOT_FOUND)
            {
                return ERR_BUSY;
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
    if (rc != ERR_OK)
    {
        return rc;
    }

    *lease_out = next;
    return ERR_OK;
}

int sap_runner_mailbox_v0_ack(DB *db, uint64_t worker_id, uint64_t seq,
                              const SapRunnerLeaseV0 *expected_lease)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t expected_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
    const void *lease_val = NULL;
    uint32_t lease_len = 0u;
    int rc;

    if (!db || !expected_lease)
    {
        return ERR_INVALID;
    }

    lease_key_encode(worker_id, seq, key);

    sap_runner_lease_v0_encode(expected_lease, expected_raw);

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
    if (lease_len != sizeof(expected_raw) ||
        memcmp(lease_val, expected_raw, sizeof(expected_raw)) != 0)
    {
        txn_abort(txn);
        return ERR_CONFLICT;
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
        return ERR_INVALID;
    }

    lease_key_encode(worker_id, seq, old_key);
    lease_key_encode(worker_id, new_seq, new_key);
    sap_runner_lease_v0_encode(expected_lease, expected_raw);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, old_key, sizeof(old_key), &lease_val, &lease_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (lease_len != sizeof(expected_raw) ||
        memcmp(lease_val, expected_raw, sizeof(expected_raw)) != 0)
    {
        txn_abort(txn);
        return ERR_CONFLICT;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, old_key, sizeof(old_key), &frame, &frame_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_put_flags_dbi(txn, SAP_WIT_DBI_INBOX, new_key, sizeof(new_key), frame, frame_len,
                           SAP_NOOVERWRITE, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_INBOX, old_key, sizeof(old_key));
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_LEASES, old_key, sizeof(old_key));
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}
