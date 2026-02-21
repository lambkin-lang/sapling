/*
 * txstack_v0.h - phase-B nested atomic context stack scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_TXSTACK_V0_H
#define SAPLING_RUNNER_TXSTACK_V0_H

#include "runner/txctx_v0.h"

#include <stdint.h>

typedef struct
{
    SapRunnerTxCtxV0 *frames;
    uint32_t depth;
    uint32_t cap;
} SapRunnerTxStackV0;

int sap_runner_txstack_v0_init(SapRunnerTxStackV0 *stack);
void sap_runner_txstack_v0_reset(SapRunnerTxStackV0 *stack);
void sap_runner_txstack_v0_dispose(SapRunnerTxStackV0 *stack);

uint32_t sap_runner_txstack_v0_depth(const SapRunnerTxStackV0 *stack);
SapRunnerTxCtxV0 *sap_runner_txstack_v0_current(SapRunnerTxStackV0 *stack);
const SapRunnerTxCtxV0 *sap_runner_txstack_v0_root(const SapRunnerTxStackV0 *stack);

/* Push a new (possibly nested) atomic context frame. */
int sap_runner_txstack_v0_push(SapRunnerTxStackV0 *stack);

/* Commit nested frame into parent (closed nesting); requires depth >= 2. */
int sap_runner_txstack_v0_commit_top(SapRunnerTxStackV0 *stack);

/* Abort top frame and discard all staged state; requires depth >= 1. */
int sap_runner_txstack_v0_abort_top(SapRunnerTxStackV0 *stack);

/* Helpers that operate on current frame with nested read/write semantics. */
int sap_runner_txstack_v0_read_dbi(SapRunnerTxStackV0 *stack, Txn *txn, uint32_t dbi,
                                   const void *key, uint32_t key_len, const void **val_out,
                                   uint32_t *val_len_out);
int sap_runner_txstack_v0_stage_put_dbi(SapRunnerTxStackV0 *stack, uint32_t dbi, const void *key,
                                        uint32_t key_len, const void *val, uint32_t val_len);
int sap_runner_txstack_v0_stage_del_dbi(SapRunnerTxStackV0 *stack, uint32_t dbi, const void *key,
                                        uint32_t key_len);
int sap_runner_txstack_v0_push_intent(SapRunnerTxStackV0 *stack, const SapRunnerIntentV0 *intent);

/* Root-only commit-phase helpers; require exactly one open frame. */
int sap_runner_txstack_v0_validate_root_reads(const SapRunnerTxStackV0 *stack, Txn *txn);
int sap_runner_txstack_v0_apply_root_writes(const SapRunnerTxStackV0 *stack, Txn *txn);

#endif /* SAPLING_RUNNER_TXSTACK_V0_H */
