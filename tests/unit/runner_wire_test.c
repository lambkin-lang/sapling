/*
 * runner_wire_test.c - tests for runner v0 message/intent wire format
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/wire_v0.h"

#include <stdio.h>
#include <string.h>

#define CHECK(cond)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(cond))                                                                               \
        {                                                                                          \
            return __LINE__;                                                                       \
        }                                                                                          \
    } while (0)

static int test_message_roundtrip_full(void)
{
    const uint8_t msg_id[] = {'m', 's', 'g', '-', '1'};
    const uint8_t trace_id[] = {'t', 'r', 'a', 'c', 'e'};
    const uint8_t payload[] = {0xde, 0xad, 0xbe, 0xef};
    uint8_t buf[256];
    uint32_t written = 0;
    SapRunnerMessageV0 in = {0};
    SapRunnerMessageV0 out = {0};

    in.kind = SAP_RUNNER_MESSAGE_KIND_EVENT;
    in.flags =
        (uint8_t)(SAP_RUNNER_MESSAGE_FLAG_DURABLE | SAP_RUNNER_MESSAGE_FLAG_REQUIRES_ACK |
                  SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER | SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID);
    in.to_worker = 77;
    in.route_worker = 11;
    in.route_timestamp = 123456;
    in.from_worker = 42;
    in.message_id = msg_id;
    in.message_id_len = sizeof(msg_id);
    in.trace_id = trace_id;
    in.trace_id_len = sizeof(trace_id);
    in.payload = payload;
    in.payload_len = sizeof(payload);

    CHECK(sap_runner_message_v0_encode(&in, buf, sizeof(buf), &written) == SAP_RUNNER_WIRE_OK);
    CHECK(written == sap_runner_message_v0_size(&in));
    CHECK(sap_runner_message_v0_decode(buf, written, &out) == SAP_RUNNER_WIRE_OK);
    CHECK(out.kind == in.kind);
    CHECK(out.flags == in.flags);
    CHECK(out.to_worker == in.to_worker);
    CHECK(out.route_worker == in.route_worker);
    CHECK(out.route_timestamp == in.route_timestamp);
    CHECK(out.from_worker == in.from_worker);
    CHECK(out.message_id_len == in.message_id_len);
    CHECK(out.trace_id_len == in.trace_id_len);
    CHECK(out.payload_len == in.payload_len);
    CHECK(memcmp(out.message_id, in.message_id, in.message_id_len) == 0);
    CHECK(memcmp(out.trace_id, in.trace_id, in.trace_id_len) == 0);
    CHECK(memcmp(out.payload, in.payload, in.payload_len) == 0);
    return 0;
}

static int test_message_roundtrip_minimal(void)
{
    const uint8_t msg_id[] = {'m'};
    uint8_t buf[128];
    uint32_t written = 0;
    SapRunnerMessageV0 in = {0};
    SapRunnerMessageV0 out = {0};

    in.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    in.flags = 0u;
    in.to_worker = 5;
    in.route_worker = 5;
    in.route_timestamp = 99;
    in.from_worker = 0;
    in.message_id = msg_id;
    in.message_id_len = sizeof(msg_id);
    in.trace_id = NULL;
    in.trace_id_len = 0;
    in.payload = NULL;
    in.payload_len = 0;

    CHECK(sap_runner_message_v0_encode(&in, buf, sizeof(buf), &written) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_message_v0_decode(buf, written, &out) == SAP_RUNNER_WIRE_OK);
    CHECK(out.flags == 0u);
    CHECK(out.from_worker == 0);
    CHECK(out.trace_id == NULL);
    CHECK(out.trace_id_len == 0u);
    CHECK(out.payload == NULL);
    CHECK(out.payload_len == 0u);
    CHECK(memcmp(out.message_id, msg_id, sizeof(msg_id)) == 0);
    return 0;
}

static int test_message_decode_rejects_version_and_truncation(void)
{
    const uint8_t msg_id[] = {'v', 'e', 'r'};
    uint8_t buf[128];
    uint32_t written = 0;
    SapRunnerMessageV0 in = {0};
    SapRunnerMessageV0 out = {0};

    in.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    in.flags = 0u;
    in.to_worker = 1;
    in.route_worker = 1;
    in.route_timestamp = 2;
    in.from_worker = 0;
    in.message_id = msg_id;
    in.message_id_len = sizeof(msg_id);
    in.trace_id = NULL;
    in.trace_id_len = 0;
    in.payload = NULL;
    in.payload_len = 0;

    CHECK(sap_runner_message_v0_encode(&in, buf, sizeof(buf), &written) == SAP_RUNNER_WIRE_OK);
    buf[6] = 1u; /* mutate minor version */
    CHECK(sap_runner_message_v0_decode(buf, written, &out) == SAP_RUNNER_WIRE_EVERSION);

    buf[6] = 0u;
    CHECK(sap_runner_message_v0_decode(buf, written - 1u, &out) == SAP_RUNNER_WIRE_ETRUNC);
    return 0;
}

static int test_intent_roundtrip(void)
{
    const uint8_t msg_payload[] = {0xaa, 0xbb, 0xcc, 0xdd};
    uint8_t buf[128];
    uint32_t written = 0;
    SapRunnerIntentV0 in = {0};
    SapRunnerIntentV0 out = {0};

    in.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
    in.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
    in.due_ts = 1700000000;
    in.message = msg_payload;
    in.message_len = sizeof(msg_payload);

    CHECK(sap_runner_intent_v0_encode(&in, buf, sizeof(buf), &written) == SAP_RUNNER_WIRE_OK);
    CHECK(written == sap_runner_intent_v0_size(&in));
    CHECK(sap_runner_intent_v0_decode(buf, written, &out) == SAP_RUNNER_WIRE_OK);
    CHECK(out.kind == in.kind);
    CHECK(out.flags == in.flags);
    CHECK(out.due_ts == in.due_ts);
    CHECK(out.message_len == in.message_len);
    CHECK(memcmp(out.message, in.message, in.message_len) == 0);
    return 0;
}

static int test_intent_validation(void)
{
    const uint8_t msg_payload[] = {0x01};
    uint8_t buf[128];
    uint32_t written = 0;
    SapRunnerIntentV0 intent = {0};

    intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    intent.flags = 0u;
    intent.due_ts = 0;
    intent.message = msg_payload;
    intent.message_len = sizeof(msg_payload);
    CHECK(sap_runner_intent_v0_encode(&intent, buf, sizeof(buf), &written) == SAP_RUNNER_WIRE_OK);

    intent.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
    CHECK(sap_runner_intent_v0_encode(&intent, buf, sizeof(buf), &written) ==
          SAP_RUNNER_WIRE_EINVAL);
    return 0;
}

static int run_test(const char *name, int (*fn)(void))
{
    int rc = fn();
    if (rc != 0)
    {
        fprintf(stderr, "%s failed at line %d\n", name, rc);
        return 1;
    }
    return 0;
}

int main(void)
{
    if (run_test("test_message_roundtrip_full", test_message_roundtrip_full) != 0)
    {
        return 1;
    }
    if (run_test("test_message_roundtrip_minimal", test_message_roundtrip_minimal) != 0)
    {
        return 2;
    }
    if (run_test("test_message_decode_rejects_version_and_truncation",
                 test_message_decode_rejects_version_and_truncation) != 0)
    {
        return 3;
    }
    if (run_test("test_intent_roundtrip", test_intent_roundtrip) != 0)
    {
        return 4;
    }
    if (run_test("test_intent_validation", test_intent_validation) != 0)
    {
        return 5;
    }
    return 0;
}
