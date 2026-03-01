/*
 * dedupe_v0.c - Exactly-once message deduplication (DBI 5)
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/dedupe_v0.h"
#include "generated/wit_schema_dbis.h"

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
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) |
           ((uint64_t)p[3] << 24) | ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40) |
           ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

static void wr64(uint8_t *p, uint64_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
    p[2] = (uint8_t)((v >> 16) & 0xffu);
    p[3] = (uint8_t)((v >> 24) & 0xffu);
    p[4] = (uint8_t)((v >> 32) & 0xffu);
    p[5] = (uint8_t)((v >> 40) & 0xffu);
    p[6] = (uint8_t)((v >> 48) & 0xffu);
    p[7] = (uint8_t)((v >> 56) & 0xffu);
}

void sap_runner_dedupe_v0_encode(const SapRunnerDedupeV0 *dedupe,
                                 uint8_t out[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE])
{
    memset(out, 0, SAP_RUNNER_DEDUPE_V0_VALUE_SIZE);
    out[0] = dedupe->accepted ? 1u : 0u;
    wr64(out + 1, (uint64_t)dedupe->last_seen_ts);
    wr32(out + 9, 17u); // checksum_offset: follows the fixed fields (1+8+4+4 = 17)
    wr32(out + 13, dedupe->checksum_len);
    if (dedupe->checksum_len > 0)
    {
        uint32_t len = dedupe->checksum_len > SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE
                           ? SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE
                           : dedupe->checksum_len;
        memcpy(out + 17, dedupe->checksum, len);
    }
}

int sap_runner_dedupe_v0_decode(const uint8_t *raw, uint32_t raw_len, SapRunnerDedupeV0 *dedupe_out)
{
    uint32_t offset;
    uint32_t len;

    if (!raw || !dedupe_out || raw_len < 17u)
    {
        return ERR_INVALID;
    }

    memset(dedupe_out, 0, sizeof(*dedupe_out));
    dedupe_out->accepted = raw[0] != 0;
    dedupe_out->last_seen_ts = (int64_t)rd64(raw + 1);
    offset = rd32(raw + 9);
    len = rd32(raw + 13);

    if (len > 0)
    {
        if (offset + len > raw_len)
        {
            return ERR_CORRUPT;
        }
        dedupe_out->checksum_len =
            len > SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE ? SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE : len;
        memcpy(dedupe_out->checksum, raw + offset, dedupe_out->checksum_len);
    }

    return ERR_OK;
}

int sap_runner_dedupe_v0_get(Txn *txn, const void *message_id, uint32_t message_id_len,
                             SapRunnerDedupeV0 *dedupe_out)
{
    const void *val;
    uint32_t val_len;
    int rc;

    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEDUPE, message_id, message_id_len, &val, &val_len);
    if (rc != ERR_OK)
    {
        return rc;
    }

    return sap_runner_dedupe_v0_decode((const uint8_t *)val, val_len, dedupe_out);
}

int sap_runner_dedupe_v0_put(Txn *txn, const void *message_id, uint32_t message_id_len,
                             const SapRunnerDedupeV0 *dedupe)
{
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];
    sap_runner_dedupe_v0_encode(dedupe, raw);
    return txn_put_dbi(txn, SAP_WIT_DBI_DEDUPE, message_id, message_id_len, raw, sizeof(raw));
}

int sap_runner_dedupe_v0_stage_put(SapRunnerTxStackV0 *stack, const void *message_id,
                                   uint32_t message_id_len, const SapRunnerDedupeV0 *dedupe)
{
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];
    sap_runner_dedupe_v0_encode(dedupe, raw);
    return sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_DEDUPE, message_id,
                                               message_id_len, raw, sizeof(raw));
}
