/*
 * intent_sink_v0.h - composed attempt intent sink for outbox + timers
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_INTENT_SINK_V0_H
#define SAPLING_RUNNER_INTENT_SINK_V0_H

#include "runner/outbox_v0.h"
#include "runner/timer_v0.h"

#include <stdint.h>

typedef struct
{
    SapRunnerOutboxV0Publisher outbox;
    SapRunnerTimerV0Publisher timers;
} SapRunnerIntentSinkV0;

int sap_runner_intent_sink_v0_init(SapRunnerIntentSinkV0 *sink, DB *db, uint64_t outbox_initial_seq,
                                   uint64_t timer_initial_seq);

/* Adapter for sap_runner_attempt_v0_run intent_sink callback. */
int sap_runner_intent_sink_v0_publish(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                      void *ctx);

#endif /* SAPLING_RUNNER_INTENT_SINK_V0_H */
