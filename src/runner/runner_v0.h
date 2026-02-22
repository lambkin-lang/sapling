/*
 * runner_v0.h - phase-A runner lifecycle and schema guards
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_V0_H
#define SAPLING_RUNNER_V0_H

#include "runner/wire_v0.h"
#include "sapling/sapling.h"

#include <stdint.h>

#ifdef SAPLING_THREADED
#include <pthread.h>
#endif

#define SAP_RUNNER_INBOX_KEY_V0_SIZE 16u

typedef enum
{
    SAP_RUNNER_V0_STATE_STOPPED = 0,
    SAP_RUNNER_V0_STATE_RUNNING = 1
} SapRunnerV0State;

typedef struct
{
    DB *db;
    uint32_t worker_id;
    uint16_t schema_major;
    uint16_t schema_minor;
    int bootstrap_schema_if_missing;
} SapRunnerV0Config;

typedef struct
{
    int64_t lease_ttl_ms;
    uint32_t requeue_max_attempts;
    uint32_t retry_budget_max;
} SapRunnerV0Policy;

typedef struct
{
    uint64_t step_attempts;
    uint64_t step_successes;
    uint64_t retryable_failures;
    uint64_t conflict_failures;
    uint64_t busy_failures;
    uint64_t non_retryable_failures;
    uint64_t requeues;
    uint64_t dead_letter_moves;
    uint64_t step_latency_samples;
    uint64_t step_latency_total_ms;
    uint32_t step_latency_max_ms;
} SapRunnerV0Metrics;

/*
 * Metrics sink callback contract:
 * - callback receives a by-value snapshot of counters after runner updates
 * - callback runs synchronously on the calling runner thread
 * - sink implementations should avoid blocking/re-entering runner APIs
 */
typedef void (*sap_runner_v0_metrics_sink)(const SapRunnerV0Metrics *metrics, void *ctx);

typedef enum
{
    SAP_RUNNER_V0_LOG_EVENT_STEP_RETRYABLE_FAILURE = 0,
    SAP_RUNNER_V0_LOG_EVENT_STEP_NON_RETRYABLE_FAILURE = 1,
    SAP_RUNNER_V0_LOG_EVENT_DISPOSITION_REQUEUE = 2,
    SAP_RUNNER_V0_LOG_EVENT_DISPOSITION_DEAD_LETTER = 3,
    SAP_RUNNER_V0_LOG_EVENT_WORKER_ERROR = 4
} SapRunnerV0LogEventKind;

typedef struct
{
    uint8_t kind;
    uint64_t worker_id;
    uint64_t seq;
    int32_t rc;
    uint32_t detail;
} SapRunnerV0LogEvent;

typedef void (*sap_runner_v0_log_sink)(const SapRunnerV0LogEvent *event, void *ctx);

typedef enum
{
    SAP_RUNNER_V0_REPLAY_EVENT_INBOX_ATTEMPT = 0,
    SAP_RUNNER_V0_REPLAY_EVENT_INBOX_RESULT = 1,
    SAP_RUNNER_V0_REPLAY_EVENT_TIMER_ATTEMPT = 2,
    SAP_RUNNER_V0_REPLAY_EVENT_TIMER_RESULT = 3,
    SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_REQUEUE = 4,
    SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_DEAD_LETTER = 5
} SapRunnerV0ReplayEventKind;

typedef struct
{
    uint8_t kind;
    uint64_t worker_id;
    uint64_t seq;
    int32_t rc;
    const uint8_t *frame;
    uint32_t frame_len;
} SapRunnerV0ReplayEvent;

/*
 * Replay hook callback contract:
 * - event->frame points to transient runner-owned bytes
 * - bytes are valid only for the duration of the callback
 * - hooks must copy bytes they need after callback return
 */
typedef void (*sap_runner_v0_replay_hook)(const SapRunnerV0ReplayEvent *event, void *ctx);

typedef struct
{
    DB *db;
    uint32_t worker_id;
    uint16_t schema_major;
    uint16_t schema_minor;
    uint64_t steps_completed;
    SapRunnerV0State state;
    SapRunnerV0Policy policy;
    SapRunnerV0Metrics metrics;
    sap_runner_v0_metrics_sink metrics_sink;
    void *metrics_sink_ctx;
    sap_runner_v0_log_sink log_sink;
    void *log_sink_ctx;
    sap_runner_v0_replay_hook replay_hook;
    void *replay_hook_ctx;
} SapRunnerV0;

#ifdef SAPLING_THREADED
typedef struct
{
    pthread_mutex_t mutex;
} SapRunnerV0DbGate;
#else
typedef struct
{
    uint8_t unused;
} SapRunnerV0DbGate;
#endif

typedef int (*sap_runner_v0_message_handler)(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg,
                                             void *ctx);

typedef struct
{
    SapRunnerV0 runner;
    sap_runner_v0_message_handler handler;
    void *handler_ctx;
    uint32_t max_batch;
    uint32_t max_idle_sleep_ms;
    int64_t (*now_ms_fn)(void *ctx);
    void *now_ms_ctx;
    void (*sleep_ms_fn)(uint32_t sleep_ms, void *ctx);
    void *sleep_ms_ctx;
    SapRunnerV0DbGate *db_gate;
    uint64_t ticks;
    int stop_requested;
    int last_error;
#ifdef SAPLING_THREADED
    pthread_t thread;
    int thread_started;
#endif
} SapRunnerV0Worker;

/* Open required DBIs from generated WIT schema metadata. */
int sap_runner_v0_bootstrap_dbis(DB *db);

/* Validate or create the runner schema-version marker in DBI 0. */
int sap_runner_v0_ensure_schema_version(DB *db, uint16_t expected_major, uint16_t expected_minor,
                                        int bootstrap_if_missing);

/* Initialize lifecycle state and apply DBI/schema guards. */
int sap_runner_v0_init(SapRunnerV0 *runner, const SapRunnerV0Config *cfg);
void sap_runner_v0_shutdown(SapRunnerV0 *runner);
void sap_runner_v0_policy_default(SapRunnerV0Policy *policy);
void sap_runner_v0_set_policy(SapRunnerV0 *runner, const SapRunnerV0Policy *policy);
void sap_runner_v0_metrics_reset(SapRunnerV0 *runner);
void sap_runner_v0_metrics_snapshot(const SapRunnerV0 *runner, SapRunnerV0Metrics *metrics_out);
void sap_runner_v0_set_metrics_sink(SapRunnerV0 *runner, sap_runner_v0_metrics_sink sink,
                                    void *sink_ctx);
void sap_runner_v0_set_log_sink(SapRunnerV0 *runner, sap_runner_v0_log_sink sink, void *sink_ctx);
void sap_runner_v0_set_replay_hook(SapRunnerV0 *runner, sap_runner_v0_replay_hook hook,
                                   void *hook_ctx);

/* Inbox key helpers (DBI 1): [worker_id:u64be][seq:u64be] */
void sap_runner_v0_inbox_key_encode(uint64_t worker_id, uint64_t seq,
                                    uint8_t out[SAP_RUNNER_INBOX_KEY_V0_SIZE]);
int sap_runner_v0_inbox_key_decode(const uint8_t *key, uint32_t key_len, uint64_t *worker_id_out,
                                   uint64_t *seq_out);

/* Queue one encoded message frame into inbox (DBI 1). */
int sap_runner_v0_inbox_put(DB *db, uint64_t worker_id, uint64_t seq, const uint8_t *frame,
                            uint32_t frame_len);

/* Decode one frame and dispatch it through the provided callback. */
int sap_runner_v0_run_step(SapRunnerV0 *runner, const uint8_t *frame, uint32_t frame_len,
                           sap_runner_v0_message_handler handler, void *ctx);

/* Poll and dispatch up to max_messages from DB-backed inbox (DBI 1). */
int sap_runner_v0_poll_inbox(SapRunnerV0 *runner, uint32_t max_messages,
                             sap_runner_v0_message_handler handler, void *ctx,
                             uint32_t *processed_out);

/* Worker shell around lifecycle + poll loop. */
int sap_runner_v0_worker_init(SapRunnerV0Worker *worker, const SapRunnerV0Config *cfg,
                              sap_runner_v0_message_handler handler, void *handler_ctx,
                              uint32_t max_batch);
int sap_runner_v0_db_gate_init(SapRunnerV0DbGate *gate);
void sap_runner_v0_db_gate_shutdown(SapRunnerV0DbGate *gate);
void sap_runner_v0_worker_set_db_gate(SapRunnerV0Worker *worker, SapRunnerV0DbGate *gate);
int sap_runner_v0_worker_tick(SapRunnerV0Worker *worker, uint32_t *processed_out);
void sap_runner_v0_worker_set_idle_policy(SapRunnerV0Worker *worker, uint32_t max_idle_sleep_ms);
void sap_runner_v0_worker_set_policy(SapRunnerV0Worker *worker, const SapRunnerV0Policy *policy);
void sap_runner_v0_worker_set_time_hooks(SapRunnerV0Worker *worker, int64_t (*now_ms_fn)(void *ctx),
                                         void *now_ms_ctx,
                                         void (*sleep_ms_fn)(uint32_t sleep_ms, void *ctx),
                                         void *sleep_ms_ctx);
int sap_runner_v0_worker_compute_idle_sleep_ms(SapRunnerV0Worker *worker, uint32_t *sleep_ms_out);
void sap_runner_v0_worker_request_stop(SapRunnerV0Worker *worker);
void sap_runner_v0_worker_shutdown(SapRunnerV0Worker *worker);
int sap_runner_v0_worker_start(SapRunnerV0Worker *worker);
int sap_runner_v0_worker_join(SapRunnerV0Worker *worker);

#endif /* SAPLING_RUNNER_V0_H */
