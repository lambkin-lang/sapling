/*
 * test_wit_thatch_codegen.c — round-trip, invariant, adversarial, and
 * result<> tests for generated WIT Thatch code.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sapling/arena.h"
#include "sapling/txn.h"
#include "sapling/thatch.h"
#include "generated/wit_schema_dbis.h"

/*
 * Include test-only result<> generated types inline.  Rename symbols that
 * conflict with the production wit_schema_dbis.c (skip).
 */
#define sap_wit_skip_value      test_result_skip_value
#include "tests/generated/test_result_types.c"
#undef sap_wit_skip_value

static int passed = 0;
static int failed = 0;

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            failed++; \
            return; \
        } \
        passed++; \
    } while (0)

/* Helper: create a test arena + env with Thatch registered */
static void make_env(SapMemArena **arena_out, SapEnv **env_out) {
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };
    CHECK(sap_arena_init(arena_out, &opts) == 0);
    *env_out = sap_env_create(*arena_out, SAPLING_PAGE_SIZE);
    CHECK(*env_out != NULL);
    CHECK(sap_thatch_subsystem_init(*env_out) == ERR_OK);
}

/* Helper: create env + txn + region for a single test */
static void setup(SapMemArena **a, SapEnv **e, SapTxnCtx **t, ThatchRegion **r) {
    make_env(a, e);
    *t = sap_txn_begin(*e, NULL, 0);
    CHECK(*t != NULL);
    CHECK(thatch_region_new(*t, r) == ERR_OK);
}

static void teardown(SapMemArena *a, SapEnv *e, SapTxnCtx *t) {
    sap_txn_abort(t);
    sap_env_destroy(e);
    sap_arena_destroy(a);
}

/* Helper: build a standard message-envelope for reuse */
static SapWitMessageEnvelope make_simple_envelope(void) {
    static const uint8_t msg_id[] = "test-msg";
    static const uint8_t payload[] = {1, 2, 3};
    return (SapWitMessageEnvelope){
        .message_id_data = msg_id, .message_id_len = 8,
        .has_from_worker = 0,
        .to = 1, .route_0 = 2, .route_1 = 3,
        .kind = SAP_WIT_MESSAGE_KIND_COMMAND,
        .message_flags = 0,
        .requires_ack = 0,
        .has_trace_id = 0,
        .payload_data = payload, .payload_len = 3,
    };
}

/* ================================================================== */
/* Round-trip tests                                                   */
/* ================================================================== */

static void test_lease_info_roundtrip(void) {
    printf("--- lease-info round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseInfo in = { .owner = 42, .deadline_ts = 1000000, .attempts = 3 };
    CHECK(sap_wit_write_lease_info(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseInfo out = {0};
    CHECK(sap_wit_read_lease_info(r, &cur, &out) == ERR_OK);
    CHECK(out.owner == 42);
    CHECK(out.deadline_ts == 1000000);
    CHECK(out.attempts == 3);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_dbi0_app_state_key_roundtrip(void) {
    printf("--- dbi0 app-state key round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t ns[] = "default";
    const uint8_t key[] = "counter";
    SapWitDbi0AppStateKey in = {
        .namespace_data = ns, .namespace_len = 7,
        .key_data = key, .key_len = 7,
    };
    CHECK(sap_wit_write_dbi0_app_state_key(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitDbi0AppStateKey out = {0};
    CHECK(sap_wit_read_dbi0_app_state_key(r, &cur, &out) == ERR_OK);
    CHECK(out.namespace_len == 7);
    CHECK(memcmp(out.namespace_data, "default", 7) == 0);
    CHECK(out.key_len == 7);
    CHECK(memcmp(out.key_data, "counter", 7) == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_dbi0_app_state_value_roundtrip(void) {
    printf("--- dbi0 app-state value round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t body[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SapWitDbi0AppStateValue in = {
        .body_data = body, .body_len = 4,
        .revision = 17,
        .updated_at = 1709000000,
        .confidence = 0.95,
    };
    CHECK(sap_wit_write_dbi0_app_state_value(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitDbi0AppStateValue out = {0};
    CHECK(sap_wit_read_dbi0_app_state_value(r, &cur, &out) == ERR_OK);
    CHECK(out.body_len == 4);
    CHECK(memcmp(out.body_data, body, 4) == 0);
    CHECK(out.revision == 17);
    CHECK(out.updated_at == 1709000000);
    CHECK(out.confidence == 0.95);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_message_envelope_roundtrip(void) {
    printf("--- message-envelope round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t msg_id[] = "msg-001";
    const uint8_t trace[] = "trace-abc";
    const uint8_t payload[] = {1, 2, 3, 4, 5};

    SapWitMessageEnvelope in = {
        .message_id_data = msg_id, .message_id_len = 7,
        .has_from_worker = 1, .from_worker = 99,
        .to = 200,
        .route_0 = 300, .route_1 = 400,
        .kind = SAP_WIT_MESSAGE_KIND_EVENT,
        .message_flags = SAP_WIT_MESSAGE_FLAGS_DURABLE | SAP_WIT_MESSAGE_FLAGS_HIGH_PRIORITY,
        .requires_ack = 1,
        .has_trace_id = 1, .trace_id_data = trace, .trace_id_len = 9,
        .payload_data = payload, .payload_len = 5,
    };
    CHECK(sap_wit_write_message_envelope(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitMessageEnvelope out = {0};
    CHECK(sap_wit_read_message_envelope(r, &cur, &out) == ERR_OK);
    CHECK(out.message_id_len == 7);
    CHECK(memcmp(out.message_id_data, "msg-001", 7) == 0);
    CHECK(out.has_from_worker == 1);
    CHECK(out.from_worker == 99);
    CHECK(out.to == 200);
    CHECK(out.route_0 == 300);
    CHECK(out.route_1 == 400);
    CHECK(out.kind == SAP_WIT_MESSAGE_KIND_EVENT);
    CHECK(out.message_flags == (SAP_WIT_MESSAGE_FLAGS_DURABLE | SAP_WIT_MESSAGE_FLAGS_HIGH_PRIORITY));
    CHECK(out.requires_ack == 1);
    CHECK(out.has_trace_id == 1);
    CHECK(out.trace_id_len == 9);
    CHECK(memcmp(out.trace_id_data, "trace-abc", 9) == 0);
    CHECK(out.payload_len == 5);
    CHECK(memcmp(out.payload_data, payload, 5) == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_message_envelope_none_options(void) {
    printf("--- message-envelope NONE options ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t msg_id[] = "msg-002";
    const uint8_t payload[] = {0xFF};

    SapWitMessageEnvelope in = {
        .message_id_data = msg_id, .message_id_len = 7,
        .has_from_worker = 0,
        .to = 1,
        .route_0 = 2, .route_1 = 3,
        .kind = SAP_WIT_MESSAGE_KIND_COMMAND,
        .message_flags = 0,
        .requires_ack = 0,
        .has_trace_id = 0,
        .payload_data = payload, .payload_len = 1,
    };
    CHECK(sap_wit_write_message_envelope(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitMessageEnvelope out = {0};
    CHECK(sap_wit_read_message_envelope(r, &cur, &out) == ERR_OK);
    CHECK(out.has_from_worker == 0);
    CHECK(out.has_trace_id == 0);
    CHECK(out.requires_ack == 0);
    CHECK(out.kind == SAP_WIT_MESSAGE_KIND_COMMAND);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_lease_state_variant_queued(void) {
    printf("--- lease-state variant: queued ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseState in = { .case_tag = SAP_WIT_LEASE_STATE_QUEUED };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseState out = {0};
    CHECK(sap_wit_read_lease_state(r, &cur, &out) == ERR_OK);
    CHECK(out.case_tag == SAP_WIT_LEASE_STATE_QUEUED);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_lease_state_variant_leased(void) {
    printf("--- lease-state variant: leased ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseState in = {
        .case_tag = SAP_WIT_LEASE_STATE_LEASED,
        .val.leased = { .owner = 7, .deadline_ts = 9999, .attempts = 2 },
    };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseState out = {0};
    CHECK(sap_wit_read_lease_state(r, &cur, &out) == ERR_OK);
    CHECK(out.case_tag == SAP_WIT_LEASE_STATE_LEASED);
    CHECK(out.val.leased.owner == 7);
    CHECK(out.val.leased.deadline_ts == 9999);
    CHECK(out.val.leased.attempts == 2);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_lease_state_variant_done(void) {
    printf("--- lease-state variant: done ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseState in = { .case_tag = SAP_WIT_LEASE_STATE_DONE };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseState out = {0};
    CHECK(sap_wit_read_lease_state(r, &cur, &out) == ERR_OK);
    CHECK(out.case_tag == SAP_WIT_LEASE_STATE_DONE);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_lease_state_variant_failed(void) {
    printf("--- lease-state variant: failed ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t reason[] = "timeout exceeded";
    SapWitLeaseState in = {
        .case_tag = SAP_WIT_LEASE_STATE_FAILED,
        .val.failed = { .data = reason, .len = 16 },
    };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseState out = {0};
    CHECK(sap_wit_read_lease_state(r, &cur, &out) == ERR_OK);
    CHECK(out.case_tag == SAP_WIT_LEASE_STATE_FAILED);
    CHECK(out.val.failed.len == 16);
    CHECK(memcmp(out.val.failed.data, "timeout exceeded", 16) == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_tx_error_variant(void) {
    printf("--- tx-error variant round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitTxError in_busy = { .case_tag = SAP_WIT_TX_ERROR_BUSY };
    CHECK(sap_wit_write_tx_error(r, &in_busy) == ERR_OK);

    const uint8_t detail[] = "version mismatch";
    SapWitTxError in_sm = {
        .case_tag = SAP_WIT_TX_ERROR_SCHEMA_MISMATCH,
        .val.schema_mismatch = { .data = detail, .len = 16 },
    };
    CHECK(sap_wit_write_tx_error(r, &in_sm) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitTxError out_busy = {0};
    CHECK(sap_wit_read_tx_error(r, &cur, &out_busy) == ERR_OK);
    CHECK(out_busy.case_tag == SAP_WIT_TX_ERROR_BUSY);

    SapWitTxError out_sm = {0};
    CHECK(sap_wit_read_tx_error(r, &cur, &out_sm) == ERR_OK);
    CHECK(out_sm.case_tag == SAP_WIT_TX_ERROR_SCHEMA_MISMATCH);
    CHECK(out_sm.val.schema_mismatch.len == 16);
    CHECK(memcmp(out_sm.val.schema_mismatch.data, "version mismatch", 16) == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_skip_correctness(void) {
    printf("--- skip correctness ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t msg_id[] = "skip-test";
    const uint8_t payload[] = {0xAA, 0xBB};
    SapWitMessageEnvelope env_in = {
        .message_id_data = msg_id, .message_id_len = 9,
        .has_from_worker = 0,
        .to = 1, .route_0 = 2, .route_1 = 3,
        .kind = SAP_WIT_MESSAGE_KIND_TIMER,
        .message_flags = SAP_WIT_MESSAGE_FLAGS_DEDUPE_REQUIRED,
        .requires_ack = 0,
        .has_trace_id = 0,
        .payload_data = payload, .payload_len = 2,
    };
    CHECK(sap_wit_write_message_envelope(r, &env_in) == ERR_OK);
    uint32_t total = thatch_region_used(r);

    ThatchCursor cur = 0;
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(cur == total);

    teardown(a, e, t);
}

static void test_skip_variant(void) {
    printf("--- skip variant ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t reason[] = "oops";
    SapWitLeaseState in = {
        .case_tag = SAP_WIT_LEASE_STATE_FAILED,
        .val.failed = { .data = reason, .len = 4 },
    };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);
    uint32_t total = thatch_region_used(r);

    ThatchCursor cur = 0;
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(cur == total);

    teardown(a, e, t);
}

static void test_dbi1_inbox_roundtrip(void) {
    printf("--- dbi1 inbox value round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t msg_id[] = "inbox-1";
    const uint8_t payload[] = {10, 20, 30};
    SapWitDbi1InboxValue in = {
        .envelope = {
            .message_id_data = msg_id, .message_id_len = 7,
            .has_from_worker = 0,
            .to = 55, .route_0 = 66, .route_1 = 77,
            .kind = SAP_WIT_MESSAGE_KIND_COMMAND,
            .message_flags = 0,
            .requires_ack = 1,
            .has_trace_id = 0,
            .payload_data = payload, .payload_len = 3,
        },
    };
    CHECK(sap_wit_write_dbi1_inbox_value(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitDbi1InboxValue out = {0};
    CHECK(sap_wit_read_dbi1_inbox_value(r, &cur, &out) == ERR_OK);
    CHECK(out.envelope.to == 55);
    CHECK(out.envelope.requires_ack == 1);
    CHECK(out.envelope.payload_len == 3);
    CHECK(memcmp(out.envelope.payload_data, payload, 3) == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_dbi3_leases_roundtrip(void) {
    printf("--- dbi3 leases value round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi3LeasesValue in = {
        .state = {
            .case_tag = SAP_WIT_LEASE_STATE_LEASED,
            .val.leased = { .owner = 42, .deadline_ts = 5000, .attempts = 1 },
        },
    };
    CHECK(sap_wit_write_dbi3_leases_value(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitDbi3LeasesValue out = {0};
    CHECK(sap_wit_read_dbi3_leases_value(r, &cur, &out) == ERR_OK);
    CHECK(out.state.case_tag == SAP_WIT_LEASE_STATE_LEASED);
    CHECK(out.state.val.leased.owner == 42);
    CHECK(out.state.val.leased.deadline_ts == 5000);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_dbi5_dedupe_roundtrip(void) {
    printf("--- dbi5 dedupe value round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t cksum[] = {0x01, 0x02, 0x03, 0x04};
    SapWitDbi5DedupeValue in = {
        .accepted = 1,
        .last_seen_ts = 1234567890,
        .checksum_data = cksum, .checksum_len = 4,
    };
    CHECK(sap_wit_write_dbi5_dedupe_value(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitDbi5DedupeValue out = {0};
    CHECK(sap_wit_read_dbi5_dedupe_value(r, &cur, &out) == ERR_OK);
    CHECK(out.accepted == 1);
    CHECK(out.last_seen_ts == 1234567890);
    CHECK(out.checksum_len == 4);
    CHECK(memcmp(out.checksum_data, cksum, 4) == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_multiple_values_in_region(void) {
    printf("--- multiple values in one region ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi1InboxKey key1 = { .worker = 10, .seq = 1 };
    SapWitDbi1InboxKey key2 = { .worker = 20, .seq = 2 };
    CHECK(sap_wit_write_dbi1_inbox_key(r, &key1) == ERR_OK);
    CHECK(sap_wit_write_dbi1_inbox_key(r, &key2) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitDbi1InboxKey out1 = {0}, out2 = {0};
    CHECK(sap_wit_read_dbi1_inbox_key(r, &cur, &out1) == ERR_OK);
    CHECK(sap_wit_read_dbi1_inbox_key(r, &cur, &out2) == ERR_OK);
    CHECK(out1.worker == 10);
    CHECK(out1.seq == 1);
    CHECK(out2.worker == 20);
    CHECK(out2.seq == 2);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

static void test_tag_mismatch_rejection(void) {
    printf("--- tag mismatch rejection ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseState in = { .case_tag = SAP_WIT_LEASE_STATE_QUEUED };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseInfo bad_out = {0};
    CHECK(sap_wit_read_lease_info(r, &cur, &bad_out) == ERR_TYPE);

    teardown(a, e, t);
}

static void test_message_ack_roundtrip(void) {
    printf("--- message-ack round-trip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t id[] = "ack-42";
    SapWitMessageAck in = {
        .message_id_data = id, .message_id_len = 6,
        .committed_at = 999999,
        .accepted = 0,
    };
    CHECK(sap_wit_write_message_ack(r, &in) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitMessageAck out = {0};
    CHECK(sap_wit_read_message_ack(r, &cur, &out) == ERR_OK);
    CHECK(out.message_id_len == 6);
    CHECK(memcmp(out.message_id_data, "ack-42", 6) == 0);
    CHECK(out.committed_at == 999999);
    CHECK(out.accepted == 0);
    CHECK(cur == thatch_region_used(r));

    teardown(a, e, t);
}

/* ================================================================== */
/* DBI invariant tests: encode / read / skip / validate consistency   */
/* ================================================================== */

static void test_invariants_dbi0_app_state_value(void) {
    printf("--- invariants: dbi0 app-state value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t body[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SapWitDbi0AppStateValue in = {
        .body_data = body, .body_len = 4,
        .revision = 17, .updated_at = 1709000000, .confidence = 0.95,
    };
    CHECK(sap_wit_write_dbi0_app_state_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);
    CHECK(N > 0);

    ThatchCursor read_cur = 0;
    SapWitDbi0AppStateValue out = {0};
    CHECK(sap_wit_read_dbi0_app_state_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi0_app_state_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi0_app_state_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi0_app_state_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_invariants_dbi1_inbox_value(void) {
    printf("--- invariants: dbi1 inbox value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi1InboxValue in = { .envelope = make_simple_envelope() };
    CHECK(sap_wit_write_dbi1_inbox_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitDbi1InboxValue out = {0};
    CHECK(sap_wit_read_dbi1_inbox_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi1_inbox_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi1_inbox_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi1_inbox_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_invariants_dbi2_outbox_value(void) {
    printf("--- invariants: dbi2 outbox value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi2OutboxValue in = {
        .envelope = make_simple_envelope(),
        .committed_at = 1234567890,
    };
    CHECK(sap_wit_write_dbi2_outbox_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitDbi2OutboxValue out = {0};
    CHECK(sap_wit_read_dbi2_outbox_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi2_outbox_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi2_outbox_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi2_outbox_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_invariants_dbi3_leases_value(void) {
    printf("--- invariants: dbi3 leases value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi3LeasesValue in = {
        .state = {
            .case_tag = SAP_WIT_LEASE_STATE_LEASED,
            .val.leased = { .owner = 42, .deadline_ts = 5000, .attempts = 1 },
        },
    };
    CHECK(sap_wit_write_dbi3_leases_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitDbi3LeasesValue out = {0};
    CHECK(sap_wit_read_dbi3_leases_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi3_leases_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi3_leases_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi3_leases_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_invariants_dbi4_timers_value(void) {
    printf("--- invariants: dbi4 timers value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi4TimersValue in = { .envelope = make_simple_envelope() };
    CHECK(sap_wit_write_dbi4_timers_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitDbi4TimersValue out = {0};
    CHECK(sap_wit_read_dbi4_timers_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi4_timers_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi4_timers_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi4_timers_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_invariants_dbi5_dedupe_value(void) {
    printf("--- invariants: dbi5 dedupe value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t cksum[] = {0xAA, 0xBB};
    SapWitDbi5DedupeValue in = {
        .accepted = 1, .last_seen_ts = 999, .checksum_data = cksum, .checksum_len = 2,
    };
    CHECK(sap_wit_write_dbi5_dedupe_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitDbi5DedupeValue out = {0};
    CHECK(sap_wit_read_dbi5_dedupe_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi5_dedupe_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi5_dedupe_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi5_dedupe_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_invariants_dbi6_dead_letter_value(void) {
    printf("--- invariants: dbi6 dead-letter value ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi6DeadLetterValue in = {
        .envelope = make_simple_envelope(),
        .failure_code = 500, .attempts = 3, .failed_at = 1709000000,
    };
    CHECK(sap_wit_write_dbi6_dead_letter_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitDbi6DeadLetterValue out = {0};
    CHECK(sap_wit_read_dbi6_dead_letter_value(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    CHECK(sap_wit_validate_dbi6_dead_letter_value(raw, N) == 0);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0xFF;
    CHECK(sap_wit_validate_dbi6_dead_letter_value(buf, N + 1) == -1);
    CHECK(sap_wit_validate_dbi6_dead_letter_value(raw, N - 1) == -1);

    free(buf);
    teardown(a, e, t);
}

/* ================================================================== */
/* Wire version pinned test                                           */
/* ================================================================== */

static void test_wire_version_pinned(void) {
    printf("--- wire version pinned ---\n");
    CHECK(SAP_WIT_WIRE_VERSION == 1u);
}

/* ================================================================== */
/* Validator null/length semantics                                    */
/* ================================================================== */

static void test_validator_null_length_semantics(void) {
    printf("--- validator null/length semantics ---\n");

    CHECK(sap_wit_validate_dbi0_app_state_value(NULL, 0) == 0);
    CHECK(sap_wit_validate_dbi0_app_state_value(NULL, 5) == -1);

    uint8_t dummy = 0;
    CHECK(sap_wit_validate_dbi0_app_state_value(&dummy, 0) == -1);
}

/* ================================================================== */
/* Variant wire contract tests                                        */
/* ================================================================== */

static void test_unit_variant_wire_layout(void) {
    printf("--- unit variant wire layout ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseState in = { .case_tag = SAP_WIT_LEASE_STATE_QUEUED };
    CHECK(sap_wit_write_lease_state(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    /* TAG_VARIANT(1) + skip_len(4) + case_tag(1) = 6 */
    CHECK(N == 6);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);
    const uint8_t *bytes = (const uint8_t *)raw;

    CHECK(bytes[0] == SAP_WIT_TAG_VARIANT);
    uint32_t skip_len;
    memcpy(&skip_len, bytes + 1, 4);
    CHECK(skip_len == 1);
    CHECK(bytes[5] == SAP_WIT_LEASE_STATE_QUEUED);

    teardown(a, e, t);
}

static void test_tuple_wire_layout_and_skip(void) {
    printf("--- tuple wire layout and skip ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitMessageEnvelope env = make_simple_envelope();
    CHECK(sap_wit_write_message_envelope(r, &env) == ERR_OK);

    ThatchCursor cur = 0;
    uint8_t tag = 0;
    uint32_t skip_len = 0;

    /* message-envelope record header */
    CHECK(thatch_read_tag(r, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_RECORD);
    CHECK(thatch_read_skip_len(r, &cur, &skip_len) == ERR_OK);

    /* message-id (utf8), from-worker (option none), to (worker-id) */
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(thatch_read_tag(r, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_OPTION_NONE);
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);

    /* route tuple starts here */
    ThatchCursor tuple_start = cur;

    /* Universal skip must consume the entire tuple segment. */
    ThatchCursor tuple_skip_cur = tuple_start;
    CHECK(sap_wit_skip_value(r, &tuple_skip_cur) == ERR_OK);
    CHECK(tuple_skip_cur == tuple_start + 23); /* tag(1) + skip(4) + 2*(s64 tag+8) */

    /* Exact tuple wire contract at this position. */
    CHECK(thatch_read_tag(r, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_TUPLE);
    CHECK(thatch_read_skip_len(r, &cur, &skip_len) == ERR_OK);
    CHECK(skip_len == 18); /* two s64 values, each [tag + 8-byte payload] */
    CHECK(thatch_advance_cursor(r, &cur, skip_len) == ERR_OK);
    CHECK(cur == tuple_skip_cur);

    teardown(a, e, t);
}

static void test_old_variant_layout_rejected(void) {
    printf("--- old variant layout rejected ---\n");

    /* Old format: [TAG_VARIANT][case_tag] — no skip pointer */
    uint8_t old_format[] = { SAP_WIT_TAG_VARIANT, SAP_WIT_LEASE_STATE_QUEUED };
    ThatchRegion view;
    CHECK(thatch_region_init_readonly(&view, old_format, sizeof(old_format)) == ERR_OK);

    ThatchCursor cur = 0;
    SapWitLeaseState out = {0};
    int rc = sap_wit_read_lease_state(&view, &cur, &out);
    CHECK(rc != ERR_OK);
}

static void test_skip_sequence_mixed_variants(void) {
    printf("--- skip sequence mixed variants ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitLeaseState v1 = { .case_tag = SAP_WIT_LEASE_STATE_QUEUED };
    SapWitLeaseState v2 = {
        .case_tag = SAP_WIT_LEASE_STATE_LEASED,
        .val.leased = { .owner = 1, .deadline_ts = 2, .attempts = 3 },
    };
    SapWitLeaseState v3 = { .case_tag = SAP_WIT_LEASE_STATE_DONE };
    const uint8_t reason[] = "err";
    SapWitLeaseState v4 = {
        .case_tag = SAP_WIT_LEASE_STATE_FAILED,
        .val.failed = { .data = reason, .len = 3 },
    };

    CHECK(sap_wit_write_lease_state(r, &v1) == ERR_OK);
    CHECK(sap_wit_write_lease_state(r, &v2) == ERR_OK);
    CHECK(sap_wit_write_lease_state(r, &v3) == ERR_OK);
    CHECK(sap_wit_write_lease_state(r, &v4) == ERR_OK);
    uint32_t total = thatch_region_used(r);

    ThatchCursor cur = 0;
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(cur == total);

    teardown(a, e, t);
}

static void test_old_variant_in_dbi_rejected(void) {
    printf("--- old variant in DBI rejected ---\n");

    uint8_t bad_dbi3[] = {
        SAP_WIT_TAG_RECORD,
        0x02, 0x00, 0x00, 0x00, /* skip_len = 2 */
        SAP_WIT_TAG_VARIANT,
        SAP_WIT_LEASE_STATE_QUEUED,
    };
    CHECK(sap_wit_validate_dbi3_leases_value(bad_dbi3, sizeof(bad_dbi3)) == -1);
}

/* ================================================================== */
/* Structural byte-flip sweep (dbi0 app-state value)                  */
/* ================================================================== */

static void test_structural_byte_flip_dbi0(void) {
    printf("--- structural byte-flip dbi0 ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t body[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SapWitDbi0AppStateValue in = {
        .body_data = body, .body_len = 4,
        .revision = 17, .updated_at = 1709000000, .confidence = 0.95,
    };
    CHECK(sap_wit_write_dbi0_app_state_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);

    /*
     * Structural byte map:
     *   [0]     TAG_RECORD    structural
     *   [1-4]   skip_len      structural
     *   [5]     TAG_BYTES     structural
     *   [6-13]  body_len+data payload
     *   [14]    TAG_S64       structural
     *   [15-22] revision      payload
     *   [23]    TAG_S64       structural
     *   [24-31] updated_at    payload
     *   [32]    TAG_F64       structural
     *   [33-40] confidence    payload
     */
    static const uint8_t is_structural[] = {
        1,                          /* 0:  TAG_RECORD */
        1, 1, 1, 1,                 /* 1-4:  skip_len */
        1,                          /* 5:  TAG_BYTES */
        0, 0, 0, 0,                 /* 6-9:  body_len */
        0, 0, 0, 0,                 /* 10-13: body data */
        1,                          /* 14: TAG_S64 */
        0, 0, 0, 0, 0, 0, 0, 0,    /* 15-22: revision */
        1,                          /* 23: TAG_S64 */
        0, 0, 0, 0, 0, 0, 0, 0,    /* 24-31: updated_at */
        1,                          /* 32: TAG_F64 */
        0, 0, 0, 0, 0, 0, 0, 0,    /* 33-40: confidence */
    };
    CHECK(N == sizeof(is_structural));

    uint8_t *buf = malloc(N);
    CHECK(buf != NULL);

    for (uint32_t i = 0; i < N; i++) {
        memcpy(buf, raw, N);
        buf[i] ^= 0xFF;
        int rc = sap_wit_validate_dbi0_app_state_value(buf, N);
        if (is_structural[i]) {
            CHECK(rc == -1);
        } else {
            CHECK(rc == 0 || rc == -1);
        }
    }

    free(buf);
    teardown(a, e, t);
}

/* ================================================================== */
/* Malformed skip_len tests                                           */
/* ================================================================== */

static void test_skip_len_overflow_record(void) {
    printf("--- skip_len overflow record ---\n");

    uint8_t data[] = {
        SAP_WIT_TAG_RECORD,
        0xFF, 0xFF, 0xFF, 0xFF,
    };
    ThatchRegion view;
    CHECK(thatch_region_init_readonly(&view, data, sizeof(data)) == ERR_OK);

    ThatchCursor cur = 0;
    int rc = sap_wit_skip_value(&view, &cur);
    CHECK(rc == ERR_RANGE);
}

static void test_skip_len_overflow_variant(void) {
    printf("--- skip_len overflow variant ---\n");

    uint8_t data[] = {
        SAP_WIT_TAG_VARIANT,
        0xFF, 0xFF, 0xFF, 0xFF,
    };
    ThatchRegion view;
    CHECK(thatch_region_init_readonly(&view, data, sizeof(data)) == ERR_OK);

    ThatchCursor cur = 0;
    int rc = sap_wit_skip_value(&view, &cur);
    CHECK(rc == ERR_RANGE);
}

static void test_skip_len_overflow_in_validator(void) {
    printf("--- skip_len overflow in validator ---\n");

    uint8_t data[] = {
        SAP_WIT_TAG_RECORD,
        0xFF, 0xFF, 0xFF, 0xFF,
    };
    CHECK(sap_wit_validate_dbi0_app_state_value(data, sizeof(data)) == -1);
}

static void test_skip_len_too_small_by_1(void) {
    printf("--- skip_len too small by 1 ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t body[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SapWitDbi0AppStateValue in = {
        .body_data = body, .body_len = 4,
        .revision = 17, .updated_at = 1709000000, .confidence = 0.95,
    };
    CHECK(sap_wit_write_dbi0_app_state_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);

    uint8_t *buf = malloc(N);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);

    uint32_t skip_len;
    memcpy(&skip_len, buf + 1, 4);
    skip_len--;
    memcpy(buf + 1, &skip_len, 4);

    CHECK(sap_wit_validate_dbi0_app_state_value(buf, N) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_skip_len_too_large_by_1(void) {
    printf("--- skip_len too large by 1 ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t body[] = {0xDE, 0xAD, 0xBE, 0xEF};
    SapWitDbi0AppStateValue in = {
        .body_data = body, .body_len = 4,
        .revision = 17, .updated_at = 1709000000, .confidence = 0.95,
    };
    CHECK(sap_wit_write_dbi0_app_state_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);

    uint8_t *buf = malloc(N + 1);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);
    buf[N] = 0x00;

    uint32_t skip_len;
    memcpy(&skip_len, buf + 1, 4);
    skip_len++;
    memcpy(buf + 1, &skip_len, 4);

    CHECK(sap_wit_validate_dbi0_app_state_value(buf, N + 1) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_nested_skip_len_corrupted(void) {
    printf("--- nested skip_len corrupted ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi3LeasesValue in = {
        .state = {
            .case_tag = SAP_WIT_LEASE_STATE_LEASED,
            .val.leased = { .owner = 42, .deadline_ts = 5000, .attempts = 1 },
        },
    };
    CHECK(sap_wit_write_dbi3_leases_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor raw_cur = 0;
    const void *raw;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);

    /*
     * Wire layout:
     *   [0]     TAG_RECORD (outer)
     *   [1-4]   skip_len_outer
     *   [5]     TAG_VARIANT (lease-state)
     *   [6-9]   skip_len_variant
     *   [10]    case_tag (leased = 1)
     *   [11]    TAG_RECORD (lease-info)
     *   [12-15] skip_len_inner  <-- corrupt this
     *   [16+]   lease-info fields
     */
    const uint8_t *bytes = (const uint8_t *)raw;
    CHECK(bytes[0] == SAP_WIT_TAG_RECORD);
    CHECK(bytes[5] == SAP_WIT_TAG_VARIANT);
    CHECK(bytes[10] == SAP_WIT_LEASE_STATE_LEASED);
    CHECK(bytes[11] == SAP_WIT_TAG_RECORD);

    uint8_t *buf = malloc(N);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);

    uint32_t inner_skip;
    memcpy(&inner_skip, buf + 12, 4);
    inner_skip--;
    memcpy(buf + 12, &inner_skip, 4);

    CHECK(sap_wit_validate_dbi3_leases_value(buf, N) == -1);

    free(buf);
    teardown(a, e, t);
}

static void test_tuple_skip_len_corrupted_in_validator(void) {
    printf("--- tuple skip_len corrupted in validator ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitDbi1InboxValue in = {
        .envelope = make_simple_envelope(),
    };
    CHECK(sap_wit_write_dbi1_inbox_value(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor raw_cur = 0;
    const void *raw = NULL;
    CHECK(thatch_read_ptr(r, &raw_cur, N, &raw) == ERR_OK);

    uint8_t *buf = malloc(N);
    CHECK(buf != NULL);
    memcpy(buf, raw, N);

    /*
     * Walk to the nested route tuple inside message-envelope:
     *   dbi1-inbox-value.record -> envelope.record ->
     *   message-id, from-worker, to, route(tuple)
     */
    ThatchRegion view;
    CHECK(thatch_region_init_readonly(&view, buf, N) == ERR_OK);

    ThatchCursor cur = 0;
    uint8_t tag = 0;
    uint32_t skip = 0;

    CHECK(thatch_read_tag(&view, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_RECORD);
    CHECK(thatch_read_skip_len(&view, &cur, &skip) == ERR_OK);

    CHECK(thatch_read_tag(&view, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_RECORD);
    CHECK(thatch_read_skip_len(&view, &cur, &skip) == ERR_OK);

    CHECK(sap_wit_skip_value(&view, &cur) == ERR_OK); /* message-id */
    CHECK(thatch_read_tag(&view, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_OPTION_NONE);            /* from-worker */
    CHECK(sap_wit_skip_value(&view, &cur) == ERR_OK); /* to */

    CHECK(thatch_read_tag(&view, &cur, &tag) == ERR_OK);
    CHECK(tag == SAP_WIT_TAG_TUPLE);

    ThatchCursor tuple_skip_off = cur;
    CHECK(thatch_read_skip_len(&view, &cur, &skip) == ERR_OK);
    CHECK(skip > 0);

    /* Corrupt only the tuple skip_len; outer framing remains valid. */
    skip -= 1;
    memcpy(buf + tuple_skip_off, &skip, 4);

    CHECK(sap_wit_validate_dbi1_inbox_value(buf, N) == -1);

    free(buf);
    teardown(a, e, t);
}

/* ================================================================== */
/* Result<> round-trip tests (via test fixture)                       */
/* ================================================================== */

static void test_result_round_trip_ok(void) {
    printf("--- result<> round-trip OK ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t msg_id[] = "ack-ok";
    SapWitTestResultCarrier in = {
        .is_result_ok = 1,
        .result_val.ok.v = {
            .message_id_data = msg_id, .message_id_len = 6,
            .committed_at = 12345,
            .accepted = 1,
        },
    };
    CHECK(sap_wit_write_test_result_carrier(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitTestResultCarrier out = {0};
    CHECK(sap_wit_read_test_result_carrier(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);
    CHECK(out.is_result_ok == 1);
    CHECK(out.result_val.ok.v.message_id_len == 6);
    CHECK(memcmp(out.result_val.ok.v.message_id_data, "ack-ok", 6) == 0);
    CHECK(out.result_val.ok.v.committed_at == 12345);
    CHECK(out.result_val.ok.v.accepted == 1);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    teardown(a, e, t);
}

static void test_result_round_trip_err(void) {
    printf("--- result<> round-trip ERR ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    SapWitTestResultCarrier in = {
        .is_result_ok = 0,
        .result_val.err.v = {
            .case_tag = SAP_WIT_TEST_ERROR_BUSY,
        },
    };
    CHECK(sap_wit_write_test_result_carrier(r, &in) == ERR_OK);
    uint32_t N = thatch_region_used(r);

    ThatchCursor read_cur = 0;
    SapWitTestResultCarrier out = {0};
    CHECK(sap_wit_read_test_result_carrier(r, &read_cur, &out) == ERR_OK);
    CHECK(read_cur == N);
    CHECK(out.is_result_ok == 0);
    CHECK(out.result_val.err.v.case_tag == SAP_WIT_TEST_ERROR_BUSY);

    ThatchCursor skip_cur = 0;
    CHECK(sap_wit_skip_value(r, &skip_cur) == ERR_OK);
    CHECK(skip_cur == N);

    teardown(a, e, t);
}

static void test_result_skip(void) {
    printf("--- result<> skip both branches ---\n");
    SapMemArena *a; SapEnv *e; SapTxnCtx *t; ThatchRegion *r;
    setup(&a, &e, &t, &r);

    const uint8_t msg_id[] = "r-skip";
    SapWitTestResultCarrier ok_carrier = {
        .is_result_ok = 1,
        .result_val.ok.v = {
            .message_id_data = msg_id, .message_id_len = 6,
            .committed_at = 1, .accepted = 1,
        },
    };
    CHECK(sap_wit_write_test_result_carrier(r, &ok_carrier) == ERR_OK);

    const uint8_t detail[] = "oops";
    SapWitTestResultCarrier err_carrier = {
        .is_result_ok = 0,
        .result_val.err.v = {
            .case_tag = SAP_WIT_TEST_ERROR_INTERNAL,
            .val.internal = { .data = detail, .len = 4 },
        },
    };
    CHECK(sap_wit_write_test_result_carrier(r, &err_carrier) == ERR_OK);
    uint32_t total = thatch_region_used(r);

    ThatchCursor cur = 0;
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(sap_wit_skip_value(r, &cur) == ERR_OK);
    CHECK(cur == total);

    teardown(a, e, t);
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */

int main(void) {
    printf("=== WIT Thatch Codegen Tests ===\n\n");

    /* Round-trip tests */
    test_lease_info_roundtrip();
    test_dbi0_app_state_key_roundtrip();
    test_dbi0_app_state_value_roundtrip();
    test_message_envelope_roundtrip();
    test_message_envelope_none_options();
    test_lease_state_variant_queued();
    test_lease_state_variant_leased();
    test_lease_state_variant_done();
    test_lease_state_variant_failed();
    test_tx_error_variant();
    test_skip_correctness();
    test_skip_variant();
    test_dbi1_inbox_roundtrip();
    test_dbi3_leases_roundtrip();
    test_dbi5_dedupe_roundtrip();
    test_multiple_values_in_region();
    test_tag_mismatch_rejection();
    test_message_ack_roundtrip();

    /* DBI invariant tests */
    test_invariants_dbi0_app_state_value();
    test_invariants_dbi1_inbox_value();
    test_invariants_dbi2_outbox_value();
    test_invariants_dbi3_leases_value();
    test_invariants_dbi4_timers_value();
    test_invariants_dbi5_dedupe_value();
    test_invariants_dbi6_dead_letter_value();

    /* Wire contract tests */
    test_wire_version_pinned();
    test_validator_null_length_semantics();
    test_unit_variant_wire_layout();
    test_tuple_wire_layout_and_skip();
    test_old_variant_layout_rejected();
    test_skip_sequence_mixed_variants();
    test_old_variant_in_dbi_rejected();

    /* Structural byte-flip */
    test_structural_byte_flip_dbi0();

    /* Malformed skip_len */
    test_skip_len_overflow_record();
    test_skip_len_overflow_variant();
    test_skip_len_overflow_in_validator();
    test_skip_len_too_small_by_1();
    test_skip_len_too_large_by_1();
    test_nested_skip_len_corrupted();
    test_tuple_skip_len_corrupted_in_validator();

    /* Result<> tests */
    test_result_round_trip_ok();
    test_result_round_trip_err();
    test_result_skip();

    printf("\nResults: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}
