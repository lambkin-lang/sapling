/*
 * intent_sink_v0.c - composed attempt intent sink for outbox + timers
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/intent_sink_v0.h"

int sap_runner_intent_sink_v0_init(SapRunnerIntentSinkV0 *sink, DB *db, uint64_t outbox_initial_seq,
                                   uint64_t timer_initial_seq)
{
    int rc;

    if (!sink || !db)
    {
        return SAP_ERROR;
    }

    rc = sap_runner_outbox_v0_publisher_init(&sink->outbox, db, outbox_initial_seq);
    if (rc != SAP_OK)
    {
        return rc;
    }
    rc = sap_runner_timer_v0_publisher_init(&sink->timers, db, timer_initial_seq);
    if (rc != SAP_OK)
    {
        return rc;
    }
    return SAP_OK;
}

int sap_runner_intent_sink_v0_publish(const uint8_t *intent_frame, uint32_t intent_frame_len,
                                      void *ctx)
{
    SapRunnerIntentSinkV0 *sink = (SapRunnerIntentSinkV0 *)ctx;
    SapRunnerIntentV0 intent = {0};
    int rc;

    if (!sink || !intent_frame || intent_frame_len == 0u)
    {
        return SAP_ERROR;
    }
    rc = sap_runner_intent_v0_decode(intent_frame, intent_frame_len, &intent);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }

    if (intent.kind == SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT)
    {
        return sap_runner_outbox_v0_publish_intent(intent_frame, intent_frame_len, &sink->outbox);
    }
    if (intent.kind == SAP_RUNNER_INTENT_KIND_TIMER_ARM)
    {
        return sap_runner_timer_v0_publish_intent(intent_frame, intent_frame_len, &sink->timers);
    }
    return SAP_ERROR;
}
