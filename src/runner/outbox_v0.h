/*
 * outbox_v0.h - phase-C outbox append/drain and intent publisher scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_OUTBOX_V0_H
#define SAPLING_RUNNER_OUTBOX_V0_H

#include "runner/attempt_v0.h"

#include <stdint.h>

#define SAP_RUNNER_OUTBOX_KEY_V0_SIZE 8u

typedef int (*sap_runner_outbox_v0_frame_handler)(const uint8_t *frame, uint32_t frame_len,
                                                  void *ctx);

typedef struct
{
    DB *db;
    uint64_t next_seq;
} SapRunnerOutboxV0Publisher;

void sap_runner_outbox_v0_key_encode(uint64_t seq, uint8_t out[SAP_RUNNER_OUTBOX_KEY_V0_SIZE]);

int sap_runner_outbox_v0_append_frame(DB *db, uint64_t seq, const uint8_t *frame,
                                      uint32_t frame_len);

int sap_runner_outbox_v0_drain(DB *db, uint32_t max_frames,
                               sap_runner_outbox_v0_frame_handler handler, void *ctx,
                               uint32_t *processed_out);

int sap_runner_outbox_v0_publisher_init(SapRunnerOutboxV0Publisher *publisher, DB *db,
                                        uint64_t initial_seq);

/* Adapter for sap_runner_attempt_v0_run intent_sink callback. */
int sap_runner_outbox_v0_publish_intent(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                        void *ctx);

#endif /* SAPLING_RUNNER_OUTBOX_V0_H */
