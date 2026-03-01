/*
 * lease_v0.c - General lease management (DBI 3)
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/lease_v0.h"
#include "generated/wit_schema_dbis.h"

#include <limits.h>
#include <string.h>

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

void sap_runner_lease_v0_encode(const SapRunnerLeaseV0 *lease,
                                uint8_t out[SAP_RUNNER_LEASE_V0_VALUE_SIZE])
{
    if (!lease || !out)
    {
        return;
    }

    memset(out, 0, SAP_RUNNER_LEASE_V0_VALUE_SIZE);

    /*
     * dbi3-leases-value {
     *   state: lease-state::leased(lease-info{owner, deadline-ts, attempts})
     * }
     */
    out[0] = SAP_WIT_TAG_RECORD;
    wr32(out + 1, 38u); /* [TAG_VARIANT][skip][case][lease-info-record] */

    out[5] = SAP_WIT_TAG_VARIANT;
    wr32(out + 6, 33u); /* [case_tag][lease-info-record] */
    out[10] = SAP_WIT_LEASE_STATE_LEASED;

    out[11] = SAP_WIT_TAG_RECORD;
    wr32(out + 12, 27u); /* three s64 fields */

    out[16] = SAP_WIT_TAG_S64;
    wr64(out + 17, lease->owner_worker);

    out[25] = SAP_WIT_TAG_S64;
    wr64(out + 26, (uint64_t)lease->deadline_ts);

    out[34] = SAP_WIT_TAG_S64;
    wr64(out + 35, (uint64_t)lease->attempts);
}

int sap_runner_lease_v0_decode(const uint8_t *raw, uint32_t raw_len, SapRunnerLeaseV0 *lease_out)
{
    int64_t owner = 0;
    int64_t attempts = 0;

    if (!raw || !lease_out || raw_len != SAP_RUNNER_LEASE_V0_VALUE_SIZE)
    {
        return ERR_INVALID;
    }
    if (raw[0] != SAP_WIT_TAG_RECORD || rd32(raw + 1) != 38u)
    {
        return ERR_CORRUPT;
    }
    if (raw[5] != SAP_WIT_TAG_VARIANT || rd32(raw + 6) != 33u ||
        raw[10] != SAP_WIT_LEASE_STATE_LEASED)
    {
        return ERR_CORRUPT;
    }
    if (raw[11] != SAP_WIT_TAG_RECORD || rd32(raw + 12) != 27u)
    {
        return ERR_CORRUPT;
    }
    if (raw[16] != SAP_WIT_TAG_S64 || raw[25] != SAP_WIT_TAG_S64 || raw[34] != SAP_WIT_TAG_S64)
    {
        return ERR_CORRUPT;
    }

    owner = (int64_t)rd64(raw + 17);
    attempts = (int64_t)rd64(raw + 35);
    if (owner < 0 || attempts < 0 || attempts > (int64_t)UINT32_MAX)
    {
        return ERR_CORRUPT;
    }

    lease_out->owner_worker = (uint64_t)owner;
    lease_out->deadline_ts = (int64_t)rd64(raw + 26);
    lease_out->attempts = (uint32_t)attempts;
    return ERR_OK;
}

int sap_runner_lease_v0_stage_acquire(SapRunnerTxStackV0 *stack, Txn *read_txn, const void *key,
                                      uint32_t key_len, uint64_t owner_worker, int64_t now_ts,
                                      int64_t duration_ms, SapRunnerLeaseV0 *lease_out)
{
    const void *val = NULL;
    uint32_t val_len = 0u;
    SapRunnerLeaseV0 next = {0};
    uint8_t raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
    int rc;

    if (!stack || !read_txn || !key || !lease_out)
    {
        return ERR_INVALID;
    }

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, SAP_WIT_DBI_LEASES, key, key_len, &val,
                                        &val_len);
    if (rc == ERR_OK)
    {
        SapRunnerLeaseV0 cur = {0};
        rc = sap_runner_lease_v0_decode((const uint8_t *)val, val_len, &cur);
        if (rc != ERR_OK)
        {
            return rc;
        }

        if (now_ts <= cur.deadline_ts && cur.owner_worker != owner_worker)
        {
            return ERR_BUSY;
        }

        next.owner_worker = owner_worker;
        next.deadline_ts = now_ts + duration_ms;
        next.attempts = cur.attempts + 1u;
    }
    else if (rc == ERR_NOT_FOUND)
    {
        next.owner_worker = owner_worker;
        next.deadline_ts = now_ts + duration_ms;
        next.attempts = 1u;
    }
    else
    {
        return rc;
    }

    sap_runner_lease_v0_encode(&next, raw);
    rc = sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_LEASES, key, key_len, raw,
                                             sizeof(raw));
    if (rc == ERR_OK)
    {
        *lease_out = next;
    }
    return rc;
}

int sap_runner_lease_v0_stage_release(SapRunnerTxStackV0 *stack, Txn *read_txn, const void *key,
                                      uint32_t key_len, uint64_t owner_worker)
{
    const void *val = NULL;
    uint32_t val_len = 0u;
    SapRunnerLeaseV0 cur = {0};
    int rc;

    if (!stack || !read_txn || !key)
    {
        return ERR_INVALID;
    }

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, SAP_WIT_DBI_LEASES, key, key_len, &val,
                                        &val_len);
    if (rc != ERR_OK)
    {
        return rc;
    }

    rc = sap_runner_lease_v0_decode((const uint8_t *)val, val_len, &cur);
    if (rc != ERR_OK)
    {
        return rc;
    }

    if (cur.owner_worker != owner_worker)
    {
        return ERR_CONFLICT;
    }

    return sap_runner_txstack_v0_stage_del_dbi(stack, SAP_WIT_DBI_LEASES, key, key_len);
}
