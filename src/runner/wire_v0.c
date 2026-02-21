/*
 * wire_v0.c - runner v0 message/intent wire contract
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/wire_v0.h"

#include <limits.h>
#include <string.h>

static const uint8_t k_message_magic[4] = {'L', 'M', 'S', 'G'};
static const uint8_t k_intent_magic[4] = {'L', 'I', 'N', 'T'};

static uint16_t rd16(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }

static uint32_t rd32(const uint8_t *p)
{
    return (uint32_t)(p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
                      ((uint32_t)p[3] << 24));
}

static uint64_t rd64(const uint8_t *p)
{
    uint64_t lo = rd32(p);
    uint64_t hi = rd32(p + 4);
    return lo | (hi << 32);
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
}

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
    p[2] = (uint8_t)((v >> 16) & 0xffu);
    p[3] = (uint8_t)((v >> 24) & 0xffu);
}

static void wr64(uint8_t *p, uint64_t v)
{
    wr32(p, (uint32_t)(v & 0xffffffffu));
    wr32(p + 4, (uint32_t)((v >> 32) & 0xffffffffu));
}

static int blob_valid(const uint8_t *ptr, uint32_t len)
{
    if (len > 0 && ptr == NULL)
    {
        return 0;
    }
    return 1;
}

static int validate_message(const SapRunnerMessageV0 *msg)
{
    if (!msg)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (msg->kind > SAP_RUNNER_MESSAGE_KIND_TIMER)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if ((msg->flags & ~SAP_RUNNER_MESSAGE_FLAG_ALLOWED_MASK) != 0u)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (msg->message_id_len == 0 || !blob_valid(msg->message_id, msg->message_id_len))
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (!blob_valid(msg->payload, msg->payload_len))
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if ((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u)
    {
        if (!blob_valid(msg->trace_id, msg->trace_id_len))
        {
            return SAP_RUNNER_WIRE_EINVAL;
        }
    }
    else
    {
        if (msg->trace_id != NULL || msg->trace_id_len != 0u)
        {
            return SAP_RUNNER_WIRE_EINVAL;
        }
    }
    if ((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER) == 0u && msg->from_worker != 0)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    return SAP_RUNNER_WIRE_OK;
}

static int validate_intent(const SapRunnerIntentV0 *intent)
{
    if (!intent)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (intent->kind > SAP_RUNNER_INTENT_KIND_TIMER_ARM)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if ((intent->flags & ~SAP_RUNNER_INTENT_FLAG_ALLOWED_MASK) != 0u)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (intent->message_len == 0 || !blob_valid(intent->message, intent->message_len))
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (intent->kind == SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT &&
        (intent->flags & SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS) != 0u)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (intent->kind == SAP_RUNNER_INTENT_KIND_TIMER_ARM &&
        (intent->flags & SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS) == 0u)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if ((intent->flags & SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS) == 0u && intent->due_ts != 0)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    return SAP_RUNNER_WIRE_OK;
}

int sap_runner_wire_version_is_supported(uint16_t major, uint16_t minor)
{
    if (major != SAP_RUNNER_WIRE_V0_MAJOR || minor != SAP_RUNNER_WIRE_V0_MINOR)
    {
        return 0;
    }
    return 1;
}

uint32_t sap_runner_message_v0_size(const SapRunnerMessageV0 *msg)
{
    uint64_t total;

    if (validate_message(msg) != SAP_RUNNER_WIRE_OK)
    {
        return 0u;
    }

    total = SAP_RUNNER_MESSAGE_V0_HEADER_SIZE;
    total += (uint64_t)msg->message_id_len;
    total += (uint64_t)msg->payload_len;
    if ((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u)
    {
        total += (uint64_t)msg->trace_id_len;
    }
    if (total > UINT32_MAX)
    {
        return 0u;
    }
    return (uint32_t)total;
}

int sap_runner_message_v0_encode(const SapRunnerMessageV0 *msg, uint8_t *dst, uint32_t dst_len,
                                 uint32_t *written_out)
{
    uint32_t frame_len;
    uint32_t cursor;
    uint32_t trace_len_field = UINT32_MAX;

    if (!written_out)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    *written_out = 0u;

    frame_len = sap_runner_message_v0_size(msg);
    if (frame_len == 0u)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    *written_out = frame_len;

    if (!dst || dst_len < frame_len)
    {
        return SAP_RUNNER_WIRE_E2BIG;
    }

    memcpy(dst, k_message_magic, sizeof(k_message_magic));
    wr16(dst + 4, SAP_RUNNER_WIRE_V0_MAJOR);
    wr16(dst + 6, SAP_RUNNER_WIRE_V0_MINOR);
    wr32(dst + 8, frame_len);
    dst[12] = msg->kind;
    dst[13] = msg->flags;
    wr16(dst + 14, 0u);
    wr64(dst + 16, (uint64_t)msg->to_worker);
    wr64(dst + 24, (uint64_t)msg->route_worker);
    wr64(dst + 32, (uint64_t)msg->route_timestamp);
    wr64(dst + 40, (uint64_t)msg->from_worker);
    wr32(dst + 48, msg->message_id_len);
    if ((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u)
    {
        trace_len_field = msg->trace_id_len;
    }
    wr32(dst + 52, trace_len_field);
    wr32(dst + 56, msg->payload_len);

    cursor = SAP_RUNNER_MESSAGE_V0_HEADER_SIZE;
    memcpy(dst + cursor, msg->message_id, msg->message_id_len);
    cursor += msg->message_id_len;

    if ((msg->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u && msg->trace_id_len > 0u)
    {
        memcpy(dst + cursor, msg->trace_id, msg->trace_id_len);
        cursor += msg->trace_id_len;
    }

    if (msg->payload_len > 0u)
    {
        memcpy(dst + cursor, msg->payload, msg->payload_len);
    }
    return SAP_RUNNER_WIRE_OK;
}

int sap_runner_message_v0_decode(const uint8_t *src, uint32_t src_len, SapRunnerMessageV0 *msg_out)
{
    uint16_t major;
    uint16_t minor;
    uint32_t frame_len;
    uint32_t msg_id_len;
    uint32_t trace_len_raw;
    uint32_t payload_len;
    uint32_t trace_len = 0u;
    uint64_t body_len;
    uint32_t cursor;

    if (!src || !msg_out)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (src_len < SAP_RUNNER_MESSAGE_V0_HEADER_SIZE)
    {
        return SAP_RUNNER_WIRE_ETRUNC;
    }
    if (memcmp(src, k_message_magic, sizeof(k_message_magic)) != 0)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    major = rd16(src + 4);
    minor = rd16(src + 6);
    if (!sap_runner_wire_version_is_supported(major, minor))
    {
        return SAP_RUNNER_WIRE_EVERSION;
    }

    frame_len = rd32(src + 8);
    if (frame_len < SAP_RUNNER_MESSAGE_V0_HEADER_SIZE)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if (frame_len > src_len)
    {
        return SAP_RUNNER_WIRE_ETRUNC;
    }
    if (frame_len != src_len)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if (rd16(src + 14) != 0u)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    msg_out->kind = src[12];
    msg_out->flags = src[13];
    if (msg_out->kind > SAP_RUNNER_MESSAGE_KIND_TIMER)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if ((msg_out->flags & ~SAP_RUNNER_MESSAGE_FLAG_ALLOWED_MASK) != 0u)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    msg_out->to_worker = (int64_t)rd64(src + 16);
    msg_out->route_worker = (int64_t)rd64(src + 24);
    msg_out->route_timestamp = (int64_t)rd64(src + 32);
    msg_out->from_worker = (int64_t)rd64(src + 40);

    msg_id_len = rd32(src + 48);
    trace_len_raw = rd32(src + 52);
    payload_len = rd32(src + 56);

    if (msg_id_len == 0u)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    if ((msg_out->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_TRACE_ID) != 0u)
    {
        if (trace_len_raw == UINT32_MAX)
        {
            return SAP_RUNNER_WIRE_EFORMAT;
        }
        trace_len = trace_len_raw;
    }
    else
    {
        if (trace_len_raw != UINT32_MAX)
        {
            return SAP_RUNNER_WIRE_EFORMAT;
        }
    }

    body_len = (uint64_t)msg_id_len + (uint64_t)trace_len + (uint64_t)payload_len;
    if (SAP_RUNNER_MESSAGE_V0_HEADER_SIZE + body_len != frame_len)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    cursor = SAP_RUNNER_MESSAGE_V0_HEADER_SIZE;
    msg_out->message_id = src + cursor;
    msg_out->message_id_len = msg_id_len;
    cursor += msg_id_len;

    if (trace_len > 0u)
    {
        msg_out->trace_id = src + cursor;
        msg_out->trace_id_len = trace_len;
        cursor += trace_len;
    }
    else
    {
        msg_out->trace_id = NULL;
        msg_out->trace_id_len = 0u;
    }

    if (payload_len > 0u)
    {
        msg_out->payload = src + cursor;
        msg_out->payload_len = payload_len;
    }
    else
    {
        msg_out->payload = NULL;
        msg_out->payload_len = 0u;
    }

    if ((msg_out->flags & SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER) == 0u)
    {
        msg_out->from_worker = 0;
    }

    return SAP_RUNNER_WIRE_OK;
}

uint32_t sap_runner_intent_v0_size(const SapRunnerIntentV0 *intent)
{
    uint64_t total;

    if (validate_intent(intent) != SAP_RUNNER_WIRE_OK)
    {
        return 0u;
    }

    total = SAP_RUNNER_INTENT_V0_HEADER_SIZE + (uint64_t)intent->message_len;
    if (total > UINT32_MAX)
    {
        return 0u;
    }
    return (uint32_t)total;
}

int sap_runner_intent_v0_encode(const SapRunnerIntentV0 *intent, uint8_t *dst, uint32_t dst_len,
                                uint32_t *written_out)
{
    uint32_t frame_len;

    if (!written_out)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    *written_out = 0u;

    frame_len = sap_runner_intent_v0_size(intent);
    if (frame_len == 0u)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    *written_out = frame_len;

    if (!dst || dst_len < frame_len)
    {
        return SAP_RUNNER_WIRE_E2BIG;
    }

    memcpy(dst, k_intent_magic, sizeof(k_intent_magic));
    wr16(dst + 4, SAP_RUNNER_WIRE_V0_MAJOR);
    wr16(dst + 6, SAP_RUNNER_WIRE_V0_MINOR);
    wr32(dst + 8, frame_len);
    dst[12] = intent->kind;
    dst[13] = intent->flags;
    wr16(dst + 14, 0u);
    wr64(dst + 16, (uint64_t)intent->due_ts);
    wr32(dst + 24, intent->message_len);
    memcpy(dst + SAP_RUNNER_INTENT_V0_HEADER_SIZE, intent->message, intent->message_len);
    return SAP_RUNNER_WIRE_OK;
}

int sap_runner_intent_v0_decode(const uint8_t *src, uint32_t src_len, SapRunnerIntentV0 *intent_out)
{
    uint16_t major;
    uint16_t minor;
    uint32_t frame_len;
    uint32_t message_len;

    if (!src || !intent_out)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (src_len < SAP_RUNNER_INTENT_V0_HEADER_SIZE)
    {
        return SAP_RUNNER_WIRE_ETRUNC;
    }
    if (memcmp(src, k_intent_magic, sizeof(k_intent_magic)) != 0)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    major = rd16(src + 4);
    minor = rd16(src + 6);
    if (!sap_runner_wire_version_is_supported(major, minor))
    {
        return SAP_RUNNER_WIRE_EVERSION;
    }

    frame_len = rd32(src + 8);
    if (frame_len < SAP_RUNNER_INTENT_V0_HEADER_SIZE)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if (frame_len > src_len)
    {
        return SAP_RUNNER_WIRE_ETRUNC;
    }
    if (frame_len != src_len)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if (rd16(src + 14) != 0u)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    intent_out->kind = src[12];
    intent_out->flags = src[13];
    if (intent_out->kind > SAP_RUNNER_INTENT_KIND_TIMER_ARM)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if ((intent_out->flags & ~SAP_RUNNER_INTENT_FLAG_ALLOWED_MASK) != 0u)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    intent_out->due_ts = (int64_t)rd64(src + 16);
    message_len = rd32(src + 24);
    if (message_len == 0u)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    if ((uint64_t)SAP_RUNNER_INTENT_V0_HEADER_SIZE + (uint64_t)message_len != frame_len)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }

    intent_out->message = src + SAP_RUNNER_INTENT_V0_HEADER_SIZE;
    intent_out->message_len = message_len;

    if (validate_intent(intent_out) != SAP_RUNNER_WIRE_OK)
    {
        return SAP_RUNNER_WIRE_EFORMAT;
    }
    return SAP_RUNNER_WIRE_OK;
}
