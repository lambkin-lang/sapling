/*
 * wit_wire_bridge_v0.h - bridge helpers between wire_v0 frames and WIT/Thatch blobs
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_WIT_WIRE_BRIDGE_V0_H
#define SAPLING_RUNNER_WIT_WIRE_BRIDGE_V0_H

#include "sapling/txn.h"

#include <stdint.h>

int sap_runner_wit_wire_v0_value_is_dbi1_inbox(const void *raw, uint32_t raw_len);

int sap_runner_wit_wire_v0_encode_dbi1_inbox_value_from_wire(SapTxnCtx *txn,
                                                              const uint8_t *frame,
                                                              uint32_t frame_len,
                                                              const void **value_out,
                                                              uint32_t *value_len_out);

int sap_runner_wit_wire_v0_decode_dbi1_inbox_value_to_wire(const uint8_t *value,
                                                            uint32_t value_len,
                                                            uint8_t **frame_out,
                                                            uint32_t *frame_len_out);

int sap_runner_wit_wire_v0_value_is_dbi2_outbox(const void *raw, uint32_t raw_len);

int sap_runner_wit_wire_v0_encode_dbi2_outbox_value_from_wire(SapTxnCtx *txn,
                                                               const uint8_t *frame,
                                                               uint32_t frame_len,
                                                               int64_t committed_at,
                                                               const void **value_out,
                                                               uint32_t *value_len_out);

int sap_runner_wit_wire_v0_decode_dbi2_outbox_value_to_wire(const uint8_t *value,
                                                             uint32_t value_len,
                                                             uint8_t **frame_out,
                                                             uint32_t *frame_len_out,
                                                             int64_t *committed_at_out);

int sap_runner_wit_wire_v0_value_is_dbi4_timers(const void *raw, uint32_t raw_len);

int sap_runner_wit_wire_v0_encode_dbi4_timers_value_from_wire(SapTxnCtx *txn,
                                                               const uint8_t *frame,
                                                               uint32_t frame_len,
                                                               const void **value_out,
                                                               uint32_t *value_len_out);

int sap_runner_wit_wire_v0_decode_dbi4_timers_value_to_wire(const uint8_t *value,
                                                             uint32_t value_len,
                                                             uint8_t **frame_out,
                                                             uint32_t *frame_len_out);

int sap_runner_wit_wire_v0_value_is_dbi6_dead_letter(const void *raw, uint32_t raw_len);

int sap_runner_wit_wire_v0_encode_dbi6_dead_letter_value_from_wire(SapTxnCtx *txn,
                                                                    const uint8_t *frame,
                                                                    uint32_t frame_len,
                                                                    int64_t failure_code,
                                                                    int64_t attempts,
                                                                    int64_t failed_at,
                                                                    const void **value_out,
                                                                    uint32_t *value_len_out);

int sap_runner_wit_wire_v0_decode_dbi6_dead_letter_value_to_wire(const uint8_t *value,
                                                                  uint32_t value_len,
                                                                  uint8_t **frame_out,
                                                                  uint32_t *frame_len_out,
                                                                  int64_t *failure_code_out,
                                                                  int64_t *attempts_out,
                                                                  int64_t *failed_at_out);

#endif /* SAPLING_RUNNER_WIT_WIRE_BRIDGE_V0_H */
