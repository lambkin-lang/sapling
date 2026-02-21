/*
 * txctx_v0.h - phase-B host transaction context scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_TXCTX_V0_H
#define SAPLING_RUNNER_TXCTX_V0_H

#include "runner/wire_v0.h"
#include "sapling/sapling.h"

#include <stdint.h>

typedef enum
{
    SAP_RUNNER_TX_WRITE_KIND_PUT = 0,
    SAP_RUNNER_TX_WRITE_KIND_DEL = 1
} SapRunnerTxWriteKindV0;

typedef struct
{
    uint32_t dbi;
    uint8_t *key;
    uint32_t key_len;
    uint8_t *val;
    uint32_t val_len;
    int exists;
} SapRunnerTxReadV0;

typedef struct
{
    uint32_t dbi;
    uint8_t *key;
    uint32_t key_len;
    uint8_t *val;
    uint32_t val_len;
    uint8_t kind;
} SapRunnerTxWriteV0;

typedef struct
{
    uint8_t *frame;
    uint32_t frame_len;
} SapRunnerTxIntentV0;

typedef struct
{
    SapRunnerTxReadV0 *reads;
    uint32_t read_count;
    uint32_t read_cap;
    SapRunnerTxWriteV0 *writes;
    uint32_t write_count;
    uint32_t write_cap;
    SapRunnerTxIntentV0 *intents;
    uint32_t intent_count;
    uint32_t intent_cap;
} SapRunnerTxCtxV0;

int sap_runner_txctx_v0_init(SapRunnerTxCtxV0 *ctx);
void sap_runner_txctx_v0_reset(SapRunnerTxCtxV0 *ctx);
void sap_runner_txctx_v0_dispose(SapRunnerTxCtxV0 *ctx);

/* Read-through helper with read-set tracking and read-your-write behavior. */
int sap_runner_txctx_v0_read_dbi(SapRunnerTxCtxV0 *ctx, Txn *txn, uint32_t dbi, const void *key,
                                 uint32_t key_len, const void **val_out, uint32_t *val_len_out);

/* Stage write-set operations (coalesced by dbi+key). */
int sap_runner_txctx_v0_stage_put_dbi(SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key,
                                      uint32_t key_len, const void *val, uint32_t val_len);
int sap_runner_txctx_v0_stage_del_dbi(SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key,
                                      uint32_t key_len);

/* Buffer encoded intent frames for post-commit publication. */
int sap_runner_txctx_v0_push_intent(SapRunnerTxCtxV0 *ctx, const SapRunnerIntentV0 *intent);

/* Validate recorded reads and apply staged writes in a short write txn. */
int sap_runner_txctx_v0_validate_reads(const SapRunnerTxCtxV0 *ctx, Txn *txn);
int sap_runner_txctx_v0_apply_writes(const SapRunnerTxCtxV0 *ctx, Txn *txn);

uint32_t sap_runner_txctx_v0_read_count(const SapRunnerTxCtxV0 *ctx);
uint32_t sap_runner_txctx_v0_write_count(const SapRunnerTxCtxV0 *ctx);
uint32_t sap_runner_txctx_v0_intent_count(const SapRunnerTxCtxV0 *ctx);
const uint8_t *sap_runner_txctx_v0_intent_frame(const SapRunnerTxCtxV0 *ctx, uint32_t index,
                                                uint32_t *frame_len_out);

#endif /* SAPLING_RUNNER_TXCTX_V0_H */
