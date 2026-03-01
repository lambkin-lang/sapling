/*
 * dedupe_v0.c - Exactly-once message deduplication (DBI 5)
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/dedupe_v0.h"
#include "generated/wit_schema_dbis.h"

#include <string.h>

static uint32_t dedupe_checksum_len(const SapRunnerDedupeV0 *dedupe)
{
    if (!dedupe)
    {
        return 0u;
    }
    if (dedupe->checksum_len > SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE)
    {
        return SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE;
    }
    return dedupe->checksum_len;
}

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

uint32_t sap_runner_dedupe_v0_encoded_len(const SapRunnerDedupeV0 *dedupe)
{
    return 20u + dedupe_checksum_len(dedupe);
}

void sap_runner_dedupe_v0_encode(const SapRunnerDedupeV0 *dedupe,
                                 uint8_t out[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE])
{
    uint32_t csum_len;

    if (!dedupe || !out)
    {
        return;
    }

    csum_len = dedupe_checksum_len(dedupe);
    memset(out, 0, SAP_RUNNER_DEDUPE_V0_VALUE_SIZE);

    /* Root record with skip pointer covering the remaining payload bytes. */
    out[0] = SAP_WIT_TAG_RECORD;
    wr32(out + 1, 15u + csum_len);

    /* accepted: bool */
    out[5] = dedupe->accepted ? SAP_WIT_TAG_BOOL_TRUE : SAP_WIT_TAG_BOOL_FALSE;

    /* last-seen-ts: s64 */
    out[6] = SAP_WIT_TAG_S64;
    wr64(out + 7, (uint64_t)dedupe->last_seen_ts);

    /* checksum: bytes */
    out[15] = SAP_WIT_TAG_BYTES;
    wr32(out + 16, csum_len);
    if (csum_len > 0u)
    {
        memcpy(out + 20, dedupe->checksum, csum_len);
    }
}

int sap_runner_dedupe_v0_decode(const uint8_t *raw, uint32_t raw_len, SapRunnerDedupeV0 *dedupe_out)
{
    uint32_t skip_len;
    uint32_t payload_len;
    uint8_t accepted_tag;
    uint8_t s64_tag;
    uint8_t bytes_tag;

    if (!raw || !dedupe_out || raw_len < 20u)
    {
        return ERR_INVALID;
    }
    if (raw[0] != SAP_WIT_TAG_RECORD)
    {
        return ERR_CORRUPT;
    }
    skip_len = rd32(raw + 1);
    if (skip_len != raw_len - 5u || skip_len < 15u)
    {
        return ERR_CORRUPT;
    }

    memset(dedupe_out, 0, sizeof(*dedupe_out));
    accepted_tag = raw[5];
    if (accepted_tag == SAP_WIT_TAG_BOOL_TRUE)
    {
        dedupe_out->accepted = 1;
    }
    else if (accepted_tag == SAP_WIT_TAG_BOOL_FALSE)
    {
        dedupe_out->accepted = 0;
    }
    else
    {
        return ERR_CORRUPT;
    }

    s64_tag = raw[6];
    if (s64_tag != SAP_WIT_TAG_S64)
    {
        return ERR_CORRUPT;
    }
    dedupe_out->last_seen_ts = (int64_t)rd64(raw + 7);

    bytes_tag = raw[15];
    if (bytes_tag != SAP_WIT_TAG_BYTES)
    {
        return ERR_CORRUPT;
    }
    payload_len = rd32(raw + 16);
    if (payload_len != skip_len - 15u || 20u + payload_len != raw_len)
    {
        return ERR_CORRUPT;
    }
    if (payload_len > 0u)
    {
        dedupe_out->checksum_len =
            payload_len > SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE ? SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE
                                                             : payload_len;
        memcpy(dedupe_out->checksum, raw + 20, dedupe_out->checksum_len);
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
    uint32_t raw_len;
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];

    if (!txn || !message_id || message_id_len == 0u || !dedupe)
    {
        return ERR_INVALID;
    }
    raw_len = sap_runner_dedupe_v0_encoded_len(dedupe);
    sap_runner_dedupe_v0_encode(dedupe, raw);
    return txn_put_dbi(txn, SAP_WIT_DBI_DEDUPE, message_id, message_id_len, raw, raw_len);
}

int sap_runner_dedupe_v0_stage_put(SapRunnerTxStackV0 *stack, const void *message_id,
                                   uint32_t message_id_len, const SapRunnerDedupeV0 *dedupe)
{
    uint32_t raw_len;
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];

    if (!stack || !message_id || message_id_len == 0u || !dedupe)
    {
        return ERR_INVALID;
    }
    raw_len = sap_runner_dedupe_v0_encoded_len(dedupe);
    sap_runner_dedupe_v0_encode(dedupe, raw);
    return sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_DEDUPE, message_id,
                                               message_id_len, raw, raw_len);
}
