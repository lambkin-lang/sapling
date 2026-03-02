/*
 * wit_wire_bridge_v0.c - bridge helpers between wire_v0 frames and WIT/Thatch blobs
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/wit_wire_bridge_v0.h"

#include "generated/wit_schema_dbis.h"
#include "runner/wire_v0.h"

#include <stdlib.h>
#include <string.h>

static uint32_t wit_message_flags_from_wire_flags(uint8_t wire_flags)
{
    uint32_t out = 0u;
    if ((wire_flags & SAP_RUNNER_MESSAGE_FLAG_DURABLE) != 0u)
        out |= SAP_WIT_MESSAGE_FLAGS_DURABLE;
    if ((wire_flags & SAP_RUNNER_MESSAGE_FLAG_HIGH_PRIORITY) != 0u)
        out |= SAP_WIT_MESSAGE_FLAGS_HIGH_PRIORITY;
    if ((wire_flags & SAP_RUNNER_MESSAGE_FLAG_DEDUPE_REQUIRED) != 0u)
        out |= SAP_WIT_MESSAGE_FLAGS_DEDUPE_REQUIRED;
    return out;
}

static uint8_t wire_flags_from_wit_message(const SapWitMessageEnvelope *envelope)
{
    uint8_t out = 0u;
    if ((envelope->message_flags & SAP_WIT_MESSAGE_FLAGS_DURABLE) != 0u)
        out |= SAP_RUNNER_MESSAGE_FLAG_DURABLE;
    if ((envelope->message_flags & SAP_WIT_MESSAGE_FLAGS_HIGH_PRIORITY) != 0u)
        out |= SAP_RUNNER_MESSAGE_FLAG_HIGH_PRIORITY;
    if ((envelope->message_flags & SAP_WIT_MESSAGE_FLAGS_DEDUPE_REQUIRED) != 0u)
        out |= SAP_RUNNER_MESSAGE_FLAG_DEDUPE_REQUIRED;
    if (envelope->requires_ack)
        out |= SAP_RUNNER_MESSAGE_FLAG_REQUIRES_ACK;
    if (envelope->has_from_worker)
        out |= SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER;
    if (envelope->has_trace_id)
        out |= SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID;
    return out;
}

static void wit_envelope_from_wire_message(const SapRunnerMessageV0 *msg, SapWitMessageEnvelope *envelope)
{
    if (!msg || !envelope)
        return;
    memset(envelope, 0, sizeof(*envelope));
    envelope->message_id_data = msg->message_id;
    envelope->message_id_len = msg->message_id_len;
    envelope->has_from_worker =
        (uint8_t)(((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER) != 0u) ? 1u : 0u);
    envelope->from_worker = msg->from_worker;
    envelope->to = msg->to_worker;
    envelope->route_0 = msg->route_worker;
    envelope->route_1 = msg->route_timestamp;
    envelope->kind = msg->kind;
    envelope->message_flags = wit_message_flags_from_wire_flags(msg->flags);
    envelope->requires_ack =
        (uint8_t)(((msg->flags & SAP_RUNNER_MESSAGE_FLAG_REQUIRES_ACK) != 0u) ? 1u : 0u);
    envelope->has_trace_id =
        (uint8_t)(((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u) ? 1u : 0u);
    envelope->trace_id_data = msg->trace_id;
    envelope->trace_id_len = msg->trace_id_len;
    envelope->payload_data = msg->payload;
    envelope->payload_len = msg->payload_len;
}

static void wire_message_from_wit_envelope(const SapWitMessageEnvelope *envelope, SapRunnerMessageV0 *msg)
{
    if (!envelope || !msg)
        return;
    memset(msg, 0, sizeof(*msg));
    msg->kind = envelope->kind;
    msg->flags = wire_flags_from_wit_message(envelope);
    msg->to_worker = envelope->to;
    msg->route_worker = envelope->route_0;
    msg->route_timestamp = envelope->route_1;
    msg->from_worker = envelope->has_from_worker ? envelope->from_worker : 0;
    msg->message_id = envelope->message_id_data;
    msg->message_id_len = envelope->message_id_len;
    msg->trace_id = envelope->has_trace_id ? envelope->trace_id_data : NULL;
    msg->trace_id_len = envelope->has_trace_id ? envelope->trace_id_len : 0u;
    msg->payload = envelope->payload_data;
    msg->payload_len = envelope->payload_len;
}

static int encode_wire_message_alloc(const SapRunnerMessageV0 *msg, uint8_t **frame_out,
                                     uint32_t *frame_len_out)
{
    uint8_t *frame = NULL;
    uint32_t frame_len = 0u;
    int rc;

    if (!msg || !frame_out || !frame_len_out)
        return ERR_INVALID;
    *frame_out = NULL;
    *frame_len_out = 0u;

    frame_len = sap_runner_message_v0_size(msg);
    if (frame_len == 0u)
        return ERR_CORRUPT;
    frame = (uint8_t *)malloc((size_t)frame_len);
    if (!frame)
        return ERR_OOM;
    rc = sap_runner_message_v0_encode(msg, frame, frame_len, &frame_len);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        free(frame);
        return ERR_CORRUPT;
    }
    *frame_out = frame;
    *frame_len_out = frame_len;
    return ERR_OK;
}

int sap_runner_wit_wire_v0_value_is_dbi1_inbox(const void *raw, uint32_t raw_len)
{
    if (!raw || raw_len == 0u)
        return 0;
    return sap_wit_validate_dbi1_inbox_value(raw, raw_len) == 0;
}

int sap_runner_wit_wire_v0_encode_dbi1_inbox_value_from_wire(SapTxnCtx *txn,
                                                              const uint8_t *frame,
                                                              uint32_t frame_len,
                                                              const void **value_out,
                                                              uint32_t *value_len_out)
{
    SapRunnerMessageV0 msg;
    SapWitDbi1InboxValue wit_value;
    ThatchRegion *region = NULL;
    ThatchCursor cursor = 0;
    const void *raw = NULL;
    int rc;

    if (!txn || !frame || frame_len == 0u || !value_out || !value_len_out)
        return ERR_INVALID;
    *value_out = NULL;
    *value_len_out = 0u;

    rc = sap_runner_message_v0_decode(frame, frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK)
        return ERR_INVALID;

    memset(&wit_value, 0, sizeof(wit_value));
    wit_envelope_from_wire_message(&msg, &wit_value.envelope);

    rc = thatch_region_new(txn, &region);
    if (rc != ERR_OK)
        return rc;
    rc = sap_wit_write_dbi1_inbox_value(region, &wit_value);
    if (rc != ERR_OK)
        return rc;
    *value_len_out = thatch_region_used(region);
    if (*value_len_out == 0u)
        return ERR_CORRUPT;
    rc = thatch_read_ptr(region, &cursor, *value_len_out, &raw);
    if (rc != ERR_OK)
        return rc;
    *value_out = raw;
    return ERR_OK;
}

int sap_runner_wit_wire_v0_decode_dbi1_inbox_value_to_wire(const uint8_t *value,
                                                            uint32_t value_len,
                                                            uint8_t **frame_out,
                                                            uint32_t *frame_len_out)
{
    ThatchRegion view;
    ThatchCursor cursor = 0;
    SapWitDbi1InboxValue wit_value;
    SapRunnerMessageV0 msg;
    int rc;

    if (!value || value_len == 0u || !frame_out || !frame_len_out)
        return ERR_INVALID;
    *frame_out = NULL;
    *frame_len_out = 0u;

    if (sap_wit_validate_dbi1_inbox_value(value, value_len) != 0)
        return ERR_INVALID;
    rc = thatch_region_init_readonly(&view, value, value_len);
    if (rc != ERR_OK)
        return rc;

    memset(&wit_value, 0, sizeof(wit_value));
    rc = sap_wit_read_dbi1_inbox_value(&view, &cursor, &wit_value);
    if (rc != ERR_OK)
        return ERR_CORRUPT;
    if (cursor != value_len)
        return ERR_CORRUPT;

    wire_message_from_wit_envelope(&wit_value.envelope, &msg);
    return encode_wire_message_alloc(&msg, frame_out, frame_len_out);
}

int sap_runner_wit_wire_v0_value_is_dbi2_outbox(const void *raw, uint32_t raw_len)
{
    if (!raw || raw_len == 0u)
        return 0;
    return sap_wit_validate_dbi2_outbox_value(raw, raw_len) == 0;
}

int sap_runner_wit_wire_v0_encode_dbi2_outbox_value_from_wire(SapTxnCtx *txn,
                                                               const uint8_t *frame,
                                                               uint32_t frame_len,
                                                               int64_t committed_at,
                                                               const void **value_out,
                                                               uint32_t *value_len_out)
{
    SapRunnerMessageV0 msg;
    SapWitDbi2OutboxValue wit_value;
    ThatchRegion *region = NULL;
    ThatchCursor cursor = 0;
    const void *raw = NULL;
    int rc;

    if (!txn || !frame || frame_len == 0u || !value_out || !value_len_out)
        return ERR_INVALID;
    *value_out = NULL;
    *value_len_out = 0u;

    rc = sap_runner_message_v0_decode(frame, frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK)
        return ERR_INVALID;

    memset(&wit_value, 0, sizeof(wit_value));
    wit_envelope_from_wire_message(&msg, &wit_value.envelope);
    wit_value.committed_at = committed_at;

    rc = thatch_region_new(txn, &region);
    if (rc != ERR_OK)
        return rc;
    rc = sap_wit_write_dbi2_outbox_value(region, &wit_value);
    if (rc != ERR_OK)
        return rc;
    *value_len_out = thatch_region_used(region);
    if (*value_len_out == 0u)
        return ERR_CORRUPT;
    rc = thatch_read_ptr(region, &cursor, *value_len_out, &raw);
    if (rc != ERR_OK)
        return rc;
    *value_out = raw;
    return ERR_OK;
}

int sap_runner_wit_wire_v0_decode_dbi2_outbox_value_to_wire(const uint8_t *value,
                                                             uint32_t value_len,
                                                             uint8_t **frame_out,
                                                             uint32_t *frame_len_out,
                                                             int64_t *committed_at_out)
{
    ThatchRegion view;
    ThatchCursor cursor = 0;
    SapWitDbi2OutboxValue wit_value;
    SapRunnerMessageV0 msg;
    int rc;

    if (!value || value_len == 0u || !frame_out || !frame_len_out)
        return ERR_INVALID;
    *frame_out = NULL;
    *frame_len_out = 0u;
    if (committed_at_out)
        *committed_at_out = 0;

    if (sap_wit_validate_dbi2_outbox_value(value, value_len) != 0)
        return ERR_INVALID;
    rc = thatch_region_init_readonly(&view, value, value_len);
    if (rc != ERR_OK)
        return rc;

    memset(&wit_value, 0, sizeof(wit_value));
    rc = sap_wit_read_dbi2_outbox_value(&view, &cursor, &wit_value);
    if (rc != ERR_OK)
        return ERR_CORRUPT;
    if (cursor != value_len)
        return ERR_CORRUPT;
    if (committed_at_out)
        *committed_at_out = wit_value.committed_at;

    wire_message_from_wit_envelope(&wit_value.envelope, &msg);
    return encode_wire_message_alloc(&msg, frame_out, frame_len_out);
}

int sap_runner_wit_wire_v0_value_is_dbi4_timers(const void *raw, uint32_t raw_len)
{
    if (!raw || raw_len == 0u)
        return 0;
    return sap_wit_validate_dbi4_timers_value(raw, raw_len) == 0;
}

int sap_runner_wit_wire_v0_encode_dbi4_timers_value_from_wire(SapTxnCtx *txn,
                                                               const uint8_t *frame,
                                                               uint32_t frame_len,
                                                               const void **value_out,
                                                               uint32_t *value_len_out)
{
    SapRunnerMessageV0 msg;
    SapWitDbi4TimersValue wit_value;
    ThatchRegion *region = NULL;
    ThatchCursor cursor = 0;
    const void *raw = NULL;
    int rc;

    if (!txn || !frame || frame_len == 0u || !value_out || !value_len_out)
        return ERR_INVALID;
    *value_out = NULL;
    *value_len_out = 0u;

    rc = sap_runner_message_v0_decode(frame, frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK)
        return ERR_INVALID;

    memset(&wit_value, 0, sizeof(wit_value));
    wit_envelope_from_wire_message(&msg, &wit_value.envelope);

    rc = thatch_region_new(txn, &region);
    if (rc != ERR_OK)
        return rc;
    rc = sap_wit_write_dbi4_timers_value(region, &wit_value);
    if (rc != ERR_OK)
        return rc;
    *value_len_out = thatch_region_used(region);
    if (*value_len_out == 0u)
        return ERR_CORRUPT;
    rc = thatch_read_ptr(region, &cursor, *value_len_out, &raw);
    if (rc != ERR_OK)
        return rc;
    *value_out = raw;
    return ERR_OK;
}

int sap_runner_wit_wire_v0_decode_dbi4_timers_value_to_wire(const uint8_t *value,
                                                             uint32_t value_len,
                                                             uint8_t **frame_out,
                                                             uint32_t *frame_len_out)
{
    ThatchRegion view;
    ThatchCursor cursor = 0;
    SapWitDbi4TimersValue wit_value;
    SapRunnerMessageV0 msg;
    int rc;

    if (!value || value_len == 0u || !frame_out || !frame_len_out)
        return ERR_INVALID;
    *frame_out = NULL;
    *frame_len_out = 0u;

    if (sap_wit_validate_dbi4_timers_value(value, value_len) != 0)
        return ERR_INVALID;
    rc = thatch_region_init_readonly(&view, value, value_len);
    if (rc != ERR_OK)
        return rc;

    memset(&wit_value, 0, sizeof(wit_value));
    rc = sap_wit_read_dbi4_timers_value(&view, &cursor, &wit_value);
    if (rc != ERR_OK)
        return ERR_CORRUPT;
    if (cursor != value_len)
        return ERR_CORRUPT;

    wire_message_from_wit_envelope(&wit_value.envelope, &msg);
    return encode_wire_message_alloc(&msg, frame_out, frame_len_out);
}

int sap_runner_wit_wire_v0_value_is_dbi6_dead_letter(const void *raw, uint32_t raw_len)
{
    if (!raw || raw_len == 0u)
        return 0;
    return sap_wit_validate_dbi6_dead_letter_value(raw, raw_len) == 0;
}

int sap_runner_wit_wire_v0_encode_dbi6_dead_letter_value_from_wire(SapTxnCtx *txn,
                                                                    const uint8_t *frame,
                                                                    uint32_t frame_len,
                                                                    int64_t failure_code,
                                                                    int64_t attempts,
                                                                    int64_t failed_at,
                                                                    const void **value_out,
                                                                    uint32_t *value_len_out)
{
    SapRunnerMessageV0 msg;
    SapWitDbi6DeadLetterValue wit_value;
    ThatchRegion *region = NULL;
    ThatchCursor cursor = 0;
    const void *raw = NULL;
    int rc;

    if (!txn || !frame || frame_len == 0u || !value_out || !value_len_out)
        return ERR_INVALID;
    *value_out = NULL;
    *value_len_out = 0u;

    rc = sap_runner_message_v0_decode(frame, frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK)
        return ERR_INVALID;

    memset(&wit_value, 0, sizeof(wit_value));
    wit_envelope_from_wire_message(&msg, &wit_value.envelope);
    wit_value.failure_code = failure_code;
    wit_value.attempts = attempts;
    wit_value.failed_at = failed_at;

    rc = thatch_region_new(txn, &region);
    if (rc != ERR_OK)
        return rc;
    rc = sap_wit_write_dbi6_dead_letter_value(region, &wit_value);
    if (rc != ERR_OK)
        return rc;
    *value_len_out = thatch_region_used(region);
    if (*value_len_out == 0u)
        return ERR_CORRUPT;
    rc = thatch_read_ptr(region, &cursor, *value_len_out, &raw);
    if (rc != ERR_OK)
        return rc;
    *value_out = raw;
    return ERR_OK;
}

int sap_runner_wit_wire_v0_decode_dbi6_dead_letter_value_to_wire(const uint8_t *value,
                                                                  uint32_t value_len,
                                                                  uint8_t **frame_out,
                                                                  uint32_t *frame_len_out,
                                                                  int64_t *failure_code_out,
                                                                  int64_t *attempts_out,
                                                                  int64_t *failed_at_out)
{
    ThatchRegion view;
    ThatchCursor cursor = 0;
    SapWitDbi6DeadLetterValue wit_value;
    SapRunnerMessageV0 msg;
    int rc;

    if (!value || value_len == 0u || !frame_out || !frame_len_out)
        return ERR_INVALID;
    *frame_out = NULL;
    *frame_len_out = 0u;
    if (failure_code_out)
        *failure_code_out = 0;
    if (attempts_out)
        *attempts_out = 0;
    if (failed_at_out)
        *failed_at_out = 0;

    if (sap_wit_validate_dbi6_dead_letter_value(value, value_len) != 0)
        return ERR_INVALID;
    rc = thatch_region_init_readonly(&view, value, value_len);
    if (rc != ERR_OK)
        return rc;

    memset(&wit_value, 0, sizeof(wit_value));
    rc = sap_wit_read_dbi6_dead_letter_value(&view, &cursor, &wit_value);
    if (rc != ERR_OK)
        return ERR_CORRUPT;
    if (cursor != value_len)
        return ERR_CORRUPT;

    if (failure_code_out)
        *failure_code_out = wit_value.failure_code;
    if (attempts_out)
        *attempts_out = wit_value.attempts;
    if (failed_at_out)
        *failed_at_out = wit_value.failed_at;

    wire_message_from_wit_envelope(&wit_value.envelope, &msg);
    return encode_wire_message_alloc(&msg, frame_out, frame_len_out);
}
