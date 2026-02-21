/*
 * wire_v0.h - runner v0 message/intent wire contract
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_WIRE_V0_H
#define SAPLING_RUNNER_WIRE_V0_H

#include <stdint.h>

#define SAP_RUNNER_WIRE_V0_MAJOR 0u
#define SAP_RUNNER_WIRE_V0_MINOR 0u

#define SAP_RUNNER_MESSAGE_V0_HEADER_SIZE 60u
#define SAP_RUNNER_INTENT_V0_HEADER_SIZE 28u

typedef enum
{
    SAP_RUNNER_WIRE_OK = 0,
    SAP_RUNNER_WIRE_EINVAL = -1,
    SAP_RUNNER_WIRE_E2BIG = -2,
    SAP_RUNNER_WIRE_EFORMAT = -3,
    SAP_RUNNER_WIRE_EVERSION = -4,
    SAP_RUNNER_WIRE_ETRUNC = -5
} SapRunnerWireRc;

typedef enum
{
    SAP_RUNNER_MESSAGE_KIND_COMMAND = 0,
    SAP_RUNNER_MESSAGE_KIND_EVENT = 1,
    SAP_RUNNER_MESSAGE_KIND_TIMER = 2
} SapRunnerMessageKind;

#define SAP_RUNNER_MESSAGE_FLAG_DURABLE 0x01u
#define SAP_RUNNER_MESSAGE_FLAG_HIGH_PRIORITY 0x02u
#define SAP_RUNNER_MESSAGE_FLAG_DEDUPE_REQUIRED 0x04u
#define SAP_RUNNER_MESSAGE_FLAG_REQUIRES_ACK 0x08u
#define SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER 0x10u
#define SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID 0x20u
#define SAP_RUNNER_MESSAGE_FLAG_ALLOWED_MASK 0x3fu

typedef struct
{
    uint8_t kind;
    uint8_t flags;
    int64_t to_worker;
    int64_t route_worker;
    int64_t route_timestamp;
    int64_t from_worker;
    const uint8_t *message_id;
    uint32_t message_id_len;
    const uint8_t *trace_id;
    uint32_t trace_id_len;
    const uint8_t *payload;
    uint32_t payload_len;
} SapRunnerMessageV0;

typedef enum
{
    SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT = 0,
    SAP_RUNNER_INTENT_KIND_TIMER_ARM = 1
} SapRunnerIntentKind;

#define SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS 0x01u
#define SAP_RUNNER_INTENT_FLAG_ALLOWED_MASK 0x01u

typedef struct
{
    uint8_t kind;
    uint8_t flags;
    int64_t due_ts;
    const uint8_t *message;
    uint32_t message_len;
} SapRunnerIntentV0;

int sap_runner_wire_version_is_supported(uint16_t major, uint16_t minor);

uint32_t sap_runner_message_v0_size(const SapRunnerMessageV0 *msg);
int sap_runner_message_v0_encode(const SapRunnerMessageV0 *msg, uint8_t *dst, uint32_t dst_len,
                                 uint32_t *written_out);
int sap_runner_message_v0_decode(const uint8_t *src, uint32_t src_len, SapRunnerMessageV0 *msg_out);

uint32_t sap_runner_intent_v0_size(const SapRunnerIntentV0 *intent);
int sap_runner_intent_v0_encode(const SapRunnerIntentV0 *intent, uint8_t *dst, uint32_t dst_len,
                                uint32_t *written_out);
int sap_runner_intent_v0_decode(const uint8_t *src, uint32_t src_len,
                                SapRunnerIntentV0 *intent_out);

#endif /* SAPLING_RUNNER_WIRE_V0_H */
