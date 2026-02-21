/*
 * runner_v0.c - phase-A runner lifecycle and schema guards
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/runner_v0.h"

#include "generated/wit_schema_dbis.h"
#include "runner/dead_letter_v0.h"
#include "runner/mailbox_v0.h"
#include "runner/scheduler_v0.h"
#include "runner/timer_v0.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define RUNNER_SCHEMA_KEY_STR "runner.schema.version"
#define RUNNER_SCHEMA_KEY_LEN ((uint32_t)(sizeof(RUNNER_SCHEMA_KEY_STR) - 1u))
#define RUNNER_SCHEMA_VAL_LEN 8u
#define RUNNER_LEASE_TTL_MS 1000
#define RUNNER_REQUEUE_MAX_ATTEMPTS 4u
#define RUNNER_RETRY_BUDGET_MAX 4u
#define RUNNER_RETRY_KEY_PREFIX "retry:"
#define RUNNER_RETRY_KEY_PREFIX_LEN ((uint32_t)(sizeof(RUNNER_RETRY_KEY_PREFIX) - 1u))

static const uint8_t k_runner_schema_key[] = RUNNER_SCHEMA_KEY_STR;
static const uint8_t k_runner_schema_magic[4] = {'R', 'S', 'V', '0'};

typedef struct
{
    SapRunnerV0Worker *worker;
    uint32_t count;
} TimerDispatchCtx;

static void metrics_note_latency(SapRunnerV0 *runner, int64_t start_ms, int64_t end_ms);
static void metrics_note_failure(SapRunnerV0 *runner, int rc);
static void emit_replay_event(const SapRunnerV0 *runner, uint8_t kind, uint64_t seq, int32_t rc,
                              const uint8_t *frame, uint32_t frame_len);

static int64_t default_now_ms(void *ctx)
{
    struct timespec ts;
    int64_t ms;

    (void)ctx;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
        return 0;
    }
    ms = (int64_t)ts.tv_sec * 1000 + (int64_t)(ts.tv_nsec / 1000000L);
    return ms;
}

static int64_t worker_now_ms(const SapRunnerV0Worker *worker)
{
    if (!worker)
    {
        return 0;
    }
    if (worker->now_ms_fn)
    {
        return worker->now_ms_fn(worker->now_ms_ctx);
    }
    return default_now_ms(NULL);
}

#ifdef SAPLING_THREADED
static void default_sleep_ms(uint32_t sleep_ms, void *ctx)
{
    struct timespec ts;

    (void)ctx;
    ts.tv_sec = (time_t)(sleep_ms / 1000u);
    ts.tv_nsec = (long)((sleep_ms % 1000u) * 1000000L);
    nanosleep(&ts, NULL);
}

static void worker_sleep_ms(const SapRunnerV0Worker *worker, uint32_t sleep_ms)
{
    if (!worker || sleep_ms == 0u)
    {
        return;
    }
    if (worker->sleep_ms_fn)
    {
        worker->sleep_ms_fn(sleep_ms, worker->sleep_ms_ctx);
        return;
    }
    default_sleep_ms(sleep_ms, NULL);
}
#endif

static int timer_dispatch_handler(int64_t due_ts, const uint8_t *payload, uint32_t payload_len,
                                  void *ctx)
{
    TimerDispatchCtx *tctx = (TimerDispatchCtx *)ctx;
    int64_t step_start;
    int64_t step_end;
    int step_rc;

    (void)due_ts;
    if (!tctx || !tctx->worker)
    {
        return SAP_ERROR;
    }
    step_start = default_now_ms(NULL);
    tctx->worker->runner.metrics.step_attempts++;
    emit_replay_event(&tctx->worker->runner, SAP_RUNNER_V0_REPLAY_EVENT_TIMER_ATTEMPT, 0u, SAP_OK,
                      payload, payload_len);
    step_rc = sap_runner_v0_run_step(&tctx->worker->runner, payload, payload_len,
                                     tctx->worker->handler, tctx->worker->handler_ctx);
    step_end = default_now_ms(NULL);
    metrics_note_latency(&tctx->worker->runner, step_start, step_end);
    emit_replay_event(&tctx->worker->runner, SAP_RUNNER_V0_REPLAY_EVENT_TIMER_RESULT, 0u,
                      (int32_t)step_rc, payload, payload_len);
    if (step_rc != SAP_OK)
    {
        metrics_note_failure(&tctx->worker->runner, step_rc);
        return step_rc;
    }
    tctx->worker->runner.metrics.step_successes++;
    tctx->count++;
    return SAP_OK;
}

static uint16_t rd16(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
}

static uint32_t rd32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
    p[2] = (uint8_t)((v >> 16) & 0xffu);
    p[3] = (uint8_t)((v >> 24) & 0xffu);
}

static uint64_t rd64be(const uint8_t *p)
{
    uint64_t v = 0u;
    uint32_t i;
    for (i = 0u; i < 8u; i++)
    {
        v = (v << 8) | (uint64_t)p[i];
    }
    return v;
}

static void wr64be(uint8_t *p, uint64_t v)
{
    int i;
    for (i = 7; i >= 0; i--)
    {
        p[i] = (uint8_t)(v & 0xffu);
        v >>= 8;
    }
}

static void encode_schema_value(uint8_t out[RUNNER_SCHEMA_VAL_LEN], uint16_t major, uint16_t minor)
{
    memcpy(out, k_runner_schema_magic, sizeof(k_runner_schema_magic));
    wr16(out + 4, major);
    wr16(out + 6, minor);
}

static int validate_schema_value(const void *val, uint32_t val_len, uint16_t expected_major,
                                 uint16_t expected_minor)
{
    const uint8_t *raw = (const uint8_t *)val;
    uint16_t major;
    uint16_t minor;

    if (!raw || val_len != RUNNER_SCHEMA_VAL_LEN)
    {
        return SAP_CONFLICT;
    }
    if (memcmp(raw, k_runner_schema_magic, sizeof(k_runner_schema_magic)) != 0)
    {
        return SAP_CONFLICT;
    }

    major = rd16(raw + 4);
    minor = rd16(raw + 6);
    if (major != expected_major || minor != expected_minor)
    {
        return SAP_CONFLICT;
    }
    return SAP_OK;
}

static int copy_bytes(const uint8_t *src, uint32_t len, uint8_t **dst_out)
{
    uint8_t *dst;

    if (!src || !dst_out || len == 0u)
    {
        return SAP_ERROR;
    }
    dst = (uint8_t *)malloc((size_t)len);
    if (!dst)
    {
        return SAP_ERROR;
    }
    memcpy(dst, src, len);
    *dst_out = dst;
    return SAP_OK;
}

static int read_next_inbox_frame(DB *db, uint32_t worker_id, uint8_t **key_out,
                                 uint32_t *key_len_out, uint8_t **frame_out,
                                 uint32_t *frame_len_out)
{
    Txn *txn;
    Cursor *cur;
    uint8_t prefix[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    int rc;
    const void *key = NULL;
    uint32_t key_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;

    if (!db || !key_out || !key_len_out || !frame_out || !frame_len_out)
    {
        return SAP_ERROR;
    }
    *key_out = NULL;
    *frame_out = NULL;
    *key_len_out = 0u;
    *frame_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    cur = cursor_open_dbi(txn, SAP_WIT_DBI_INBOX);
    if (!cur)
    {
        txn_abort(txn);
        return SAP_ERROR;
    }

    sap_runner_v0_inbox_key_encode((uint64_t)worker_id, 0u, prefix);
    rc = cursor_seek_prefix(cur, prefix, 8u);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    rc = cursor_get(cur, &key, &key_len, &val, &val_len);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    if (key_len != SAP_RUNNER_INBOX_KEY_V0_SIZE || val_len == 0u)
    {
        cursor_close(cur);
        txn_abort(txn);
        return SAP_ERROR;
    }

    rc = copy_bytes((const uint8_t *)key, key_len, key_out);
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    rc = copy_bytes((const uint8_t *)val, val_len, frame_out);
    if (rc != SAP_OK)
    {
        free(*key_out);
        *key_out = NULL;
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }
    *key_len_out = key_len;
    *frame_len_out = val_len;

    cursor_close(cur);
    txn_abort(txn);
    return SAP_OK;
}

static int is_retryable_step_rc(int rc) { return rc == SAP_BUSY || rc == SAP_CONFLICT; }

static void metrics_note_latency(SapRunnerV0 *runner, int64_t start_ms, int64_t end_ms)
{
    uint64_t delta_ms = 0u;

    if (!runner)
    {
        return;
    }
    if (end_ms > start_ms)
    {
        delta_ms = (uint64_t)(end_ms - start_ms);
    }
    runner->metrics.step_latency_samples++;
    runner->metrics.step_latency_total_ms += delta_ms;
    if (delta_ms > (uint64_t)UINT32_MAX)
    {
        runner->metrics.step_latency_max_ms = UINT32_MAX;
    }
    else if ((uint32_t)delta_ms > runner->metrics.step_latency_max_ms)
    {
        runner->metrics.step_latency_max_ms = (uint32_t)delta_ms;
    }
}

static void metrics_note_failure(SapRunnerV0 *runner, int rc)
{
    if (!runner)
    {
        return;
    }
    if (is_retryable_step_rc(rc))
    {
        runner->metrics.retryable_failures++;
        if (rc == SAP_CONFLICT)
        {
            runner->metrics.conflict_failures++;
        }
        else if (rc == SAP_BUSY)
        {
            runner->metrics.busy_failures++;
        }
        return;
    }
    runner->metrics.non_retryable_failures++;
}

static void emit_replay_event(const SapRunnerV0 *runner, uint8_t kind, uint64_t seq, int32_t rc,
                              const uint8_t *frame, uint32_t frame_len)
{
    SapRunnerV0ReplayEvent event = {0};

    if (!runner || !runner->replay_hook)
    {
        return;
    }
    event.kind = kind;
    event.worker_id = runner->worker_id;
    event.seq = seq;
    event.rc = rc;
    event.frame = frame;
    event.frame_len = frame_len;
    runner->replay_hook(&event, runner->replay_hook_ctx);
}

static int extract_message_id_from_frame(const uint8_t *frame, uint32_t frame_len,
                                         const uint8_t **message_id_out,
                                         uint32_t *message_id_len_out)
{
    SapRunnerMessageV0 msg = {0};
    int rc;

    if (message_id_out)
    {
        *message_id_out = NULL;
    }
    if (message_id_len_out)
    {
        *message_id_len_out = 0u;
    }
    if (!frame || frame_len == 0u || !message_id_out || !message_id_len_out)
    {
        return SAP_ERROR;
    }

    rc = sap_runner_message_v0_decode(frame, frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }
    if (!msg.message_id || msg.message_id_len == 0u)
    {
        return SAP_ERROR;
    }

    *message_id_out = msg.message_id;
    *message_id_len_out = msg.message_id_len;
    return SAP_OK;
}

static int make_retry_key(const uint8_t *message_id, uint32_t message_id_len, uint8_t **key_out,
                          uint32_t *key_len_out)
{
    uint8_t *key = NULL;
    uint32_t key_len;

    if (!message_id || message_id_len == 0u || !key_out || !key_len_out)
    {
        return SAP_ERROR;
    }
    if (message_id_len > (UINT32_MAX - RUNNER_RETRY_KEY_PREFIX_LEN))
    {
        return SAP_FULL;
    }
    key_len = RUNNER_RETRY_KEY_PREFIX_LEN + message_id_len;
    key = (uint8_t *)malloc((size_t)key_len);
    if (!key)
    {
        return SAP_ERROR;
    }
    memcpy(key, RUNNER_RETRY_KEY_PREFIX, RUNNER_RETRY_KEY_PREFIX_LEN);
    memcpy(key + RUNNER_RETRY_KEY_PREFIX_LEN, message_id, message_id_len);
    *key_out = key;
    *key_len_out = key_len;
    return SAP_OK;
}

static int retry_count_increment(DB *db, const uint8_t *message_id, uint32_t message_id_len,
                                 uint32_t *count_out)
{
    Txn *txn;
    uint8_t *key = NULL;
    uint32_t key_len = 0u;
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    uint8_t raw_count[4];
    uint32_t count = 0u;
    int rc;

    if (count_out)
    {
        *count_out = 0u;
    }
    if (!db || !message_id || message_id_len == 0u)
    {
        return SAP_ERROR;
    }
    rc = make_retry_key(message_id, message_id_len, &key, &key_len);
    if (rc != SAP_OK)
    {
        return rc;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        free(key);
        return SAP_BUSY;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEDUPE, key, key_len, &cur, &cur_len);
    if (rc == SAP_NOTFOUND)
    {
        count = 1u;
    }
    else if (rc == SAP_OK)
    {
        if (cur_len != sizeof(raw_count))
        {
            free(key);
            txn_abort(txn);
            return SAP_ERROR;
        }
        count = rd32((const uint8_t *)cur);
        if (count == UINT32_MAX)
        {
            free(key);
            txn_abort(txn);
            return SAP_FULL;
        }
        count++;
    }
    else
    {
        free(key);
        txn_abort(txn);
        return rc;
    }

    wr32(raw_count, count);
    rc = txn_put_dbi(txn, SAP_WIT_DBI_DEDUPE, key, key_len, raw_count, sizeof(raw_count));
    free(key);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    if (rc != SAP_OK)
    {
        return rc;
    }
    if (count_out)
    {
        *count_out = count;
    }
    return SAP_OK;
}

static int retry_count_clear(DB *db, const uint8_t *message_id, uint32_t message_id_len)
{
    Txn *txn;
    uint8_t *key = NULL;
    uint32_t key_len = 0u;
    int rc;

    if (!db || !message_id || message_id_len == 0u)
    {
        return SAP_ERROR;
    }
    rc = make_retry_key(message_id, message_id_len, &key, &key_len);
    if (rc != SAP_OK)
    {
        return rc;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        free(key);
        return SAP_BUSY;
    }

    rc = txn_del_dbi(txn, SAP_WIT_DBI_DEDUPE, key, key_len);
    free(key);
    if (rc == SAP_NOTFOUND)
    {
        txn_abort(txn);
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

static int next_inbox_seq_for_worker(DB *db, uint64_t worker_id, uint64_t *next_seq_out)
{
    Txn *txn;
    Cursor *cur;
    uint8_t prefix[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint64_t last_seq = 0u;
    int have_any = 0;
    int rc;

    if (!db || !next_seq_out)
    {
        return SAP_ERROR;
    }
    *next_seq_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    cur = cursor_open_dbi(txn, SAP_WIT_DBI_INBOX);
    if (!cur)
    {
        txn_abort(txn);
        return SAP_ERROR;
    }

    sap_runner_v0_inbox_key_encode(worker_id, 0u, prefix);
    rc = cursor_seek_prefix(cur, prefix, 8u);
    if (rc == SAP_NOTFOUND)
    {
        cursor_close(cur);
        txn_abort(txn);
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    for (;;)
    {
        const void *key = NULL;
        const void *val = NULL;
        uint32_t key_len = 0u;
        uint32_t val_len = 0u;
        uint64_t key_worker = 0u;
        uint64_t key_seq = 0u;

        rc = cursor_get(cur, &key, &key_len, &val, &val_len);
        (void)val;
        (void)val_len;
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
        rc = sap_runner_v0_inbox_key_decode((const uint8_t *)key, key_len, &key_worker, &key_seq);
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
        if (key_worker != worker_id)
        {
            break;
        }

        have_any = 1;
        last_seq = key_seq;

        rc = cursor_next(cur);
        if (rc == SAP_NOTFOUND)
        {
            break;
        }
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
    }

    cursor_close(cur);
    txn_abort(txn);

    if (!have_any)
    {
        *next_seq_out = 0u;
        return SAP_OK;
    }
    if (last_seq == UINT64_MAX)
    {
        return SAP_FULL;
    }
    *next_seq_out = last_seq + 1u;
    return SAP_OK;
}

static int requeue_claimed_inbox_message(DB *db, uint64_t worker_id, uint64_t seq,
                                         const SapRunnerLeaseV0 *expected_lease)
{
    uint32_t attempt;

    if (!db || !expected_lease)
    {
        return SAP_ERROR;
    }

    for (attempt = 0u; attempt < RUNNER_REQUEUE_MAX_ATTEMPTS; attempt++)
    {
        uint64_t new_seq = 0u;
        int rc = next_inbox_seq_for_worker(db, worker_id, &new_seq);
        if (rc != SAP_OK)
        {
            return rc;
        }
        if (new_seq == seq)
        {
            if (new_seq == UINT64_MAX)
            {
                return SAP_FULL;
            }
            new_seq++;
        }

        rc = sap_runner_mailbox_v0_requeue(db, worker_id, seq, expected_lease, new_seq);
        if (rc == SAP_EXISTS || rc == SAP_CONFLICT || rc == SAP_BUSY)
        {
            continue;
        }
        return rc;
    }
    return SAP_BUSY;
}

int sap_runner_v0_bootstrap_dbis(DB *db)
{
    uint32_t i;

    if (!db)
    {
        return SAP_ERROR;
    }
    if (sap_wit_dbi_schema_count == 0u)
    {
        return SAP_ERROR;
    }
    if (sap_wit_dbi_schema[0].dbi != SAP_WIT_DBI_APP_STATE)
    {
        return SAP_ERROR;
    }

    for (i = 1u; i < sap_wit_dbi_schema_count; i++)
    {
        const SapWitDbiSchema *entry = &sap_wit_dbi_schema[i];
        int rc;

        if (entry->dbi != i)
        {
            return SAP_ERROR;
        }
        rc = dbi_open(db, entry->dbi, NULL, NULL, 0u);
        if (rc != SAP_OK)
        {
            return rc;
        }
    }
    return SAP_OK;
}

int sap_runner_v0_ensure_schema_version(DB *db, uint16_t expected_major, uint16_t expected_minor,
                                        int bootstrap_if_missing)
{
    Txn *txn;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_APP_STATE, k_runner_schema_key, RUNNER_SCHEMA_KEY_LEN, &val,
                     &val_len);
    txn_abort(txn);

    if (rc == SAP_OK)
    {
        return validate_schema_value(val, val_len, expected_major, expected_minor);
    }
    if (rc != SAP_NOTFOUND)
    {
        return rc;
    }
    if (!bootstrap_if_missing)
    {
        return SAP_NOTFOUND;
    }

    {
        uint8_t schema_val[RUNNER_SCHEMA_VAL_LEN];
        Txn *wtxn;
        const void *existing_val = NULL;
        uint32_t existing_len = 0u;

        encode_schema_value(schema_val, expected_major, expected_minor);

        wtxn = txn_begin(db, NULL, 0u);
        if (!wtxn)
        {
            return SAP_BUSY;
        }
        rc = txn_get_dbi(wtxn, SAP_WIT_DBI_APP_STATE, k_runner_schema_key, RUNNER_SCHEMA_KEY_LEN,
                         &existing_val, &existing_len);
        if (rc == SAP_OK)
        {
            rc = validate_schema_value(existing_val, existing_len, expected_major, expected_minor);
            if (rc != SAP_OK)
            {
                txn_abort(wtxn);
                return rc;
            }
        }
        else if (rc == SAP_NOTFOUND)
        {
            rc = txn_put_dbi(wtxn, SAP_WIT_DBI_APP_STATE, k_runner_schema_key,
                             RUNNER_SCHEMA_KEY_LEN, schema_val, RUNNER_SCHEMA_VAL_LEN);
            if (rc != SAP_OK)
            {
                txn_abort(wtxn);
                return rc;
            }
        }
        else
        {
            txn_abort(wtxn);
            return rc;
        }
        rc = txn_commit(wtxn);
        if (rc != SAP_OK)
        {
            return rc;
        }
    }

    return SAP_OK;
}

int sap_runner_v0_init(SapRunnerV0 *runner, const SapRunnerV0Config *cfg)
{
    int rc;

    if (!runner || !cfg || !cfg->db)
    {
        return SAP_ERROR;
    }

    memset(runner, 0, sizeof(*runner));
    rc = sap_runner_v0_bootstrap_dbis(cfg->db);
    if (rc != SAP_OK)
    {
        return rc;
    }

    rc = sap_runner_v0_ensure_schema_version(cfg->db, cfg->schema_major, cfg->schema_minor,
                                             cfg->bootstrap_schema_if_missing);
    if (rc != SAP_OK)
    {
        return rc;
    }

    runner->db = cfg->db;
    runner->worker_id = cfg->worker_id;
    runner->schema_major = cfg->schema_major;
    runner->schema_minor = cfg->schema_minor;
    runner->steps_completed = 0u;
    runner->state = SAP_RUNNER_V0_STATE_RUNNING;
    sap_runner_v0_metrics_reset(runner);
    return SAP_OK;
}

void sap_runner_v0_metrics_reset(SapRunnerV0 *runner)
{
    if (!runner)
    {
        return;
    }
    memset(&runner->metrics, 0, sizeof(runner->metrics));
}

void sap_runner_v0_metrics_snapshot(const SapRunnerV0 *runner, SapRunnerV0Metrics *metrics_out)
{
    if (!runner || !metrics_out)
    {
        return;
    }
    *metrics_out = runner->metrics;
}

void sap_runner_v0_set_replay_hook(SapRunnerV0 *runner, sap_runner_v0_replay_hook hook,
                                   void *hook_ctx)
{
    if (!runner)
    {
        return;
    }
    runner->replay_hook = hook;
    runner->replay_hook_ctx = hook_ctx;
}

void sap_runner_v0_shutdown(SapRunnerV0 *runner)
{
    if (!runner)
    {
        return;
    }
    runner->state = SAP_RUNNER_V0_STATE_STOPPED;
}

void sap_runner_v0_inbox_key_encode(uint64_t worker_id, uint64_t seq,
                                    uint8_t out[SAP_RUNNER_INBOX_KEY_V0_SIZE])
{
    if (!out)
    {
        return;
    }
    wr64be(out, worker_id);
    wr64be(out + 8, seq);
}

int sap_runner_v0_inbox_key_decode(const uint8_t *key, uint32_t key_len, uint64_t *worker_id_out,
                                   uint64_t *seq_out)
{
    if (!key || key_len != SAP_RUNNER_INBOX_KEY_V0_SIZE || !worker_id_out || !seq_out)
    {
        return SAP_ERROR;
    }
    *worker_id_out = rd64be(key);
    *seq_out = rd64be(key + 8);
    return SAP_OK;
}

int sap_runner_v0_inbox_put(DB *db, uint64_t worker_id, uint64_t seq, const uint8_t *frame,
                            uint32_t frame_len)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    int rc;

    if (!db || !frame || frame_len == 0u)
    {
        return SAP_ERROR;
    }

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    rc = txn_put_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), frame, frame_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
}

int sap_runner_v0_run_step(SapRunnerV0 *runner, const uint8_t *frame, uint32_t frame_len,
                           sap_runner_v0_message_handler handler, void *ctx)
{
    SapRunnerMessageV0 msg;
    int rc;

    if (!runner || !frame || frame_len == 0u || !handler)
    {
        return SAP_RUNNER_WIRE_EINVAL;
    }
    if (runner->state != SAP_RUNNER_V0_STATE_RUNNING)
    {
        return SAP_BUSY;
    }

    rc = sap_runner_message_v0_decode(frame, frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        return rc;
    }
    if (msg.to_worker != (int64_t)runner->worker_id)
    {
        return SAP_NOTFOUND;
    }

    rc = handler(runner, &msg, ctx);
    if (rc != SAP_OK)
    {
        return rc;
    }
    runner->steps_completed++;
    return SAP_OK;
}

int sap_runner_v0_poll_inbox(SapRunnerV0 *runner, uint32_t max_messages,
                             sap_runner_v0_message_handler handler, void *ctx,
                             uint32_t *processed_out)
{
    uint32_t processed = 0u;
    uint32_t i;

    if (processed_out)
    {
        *processed_out = 0u;
    }
    if (!runner || !handler)
    {
        return SAP_ERROR;
    }
    if (runner->state != SAP_RUNNER_V0_STATE_RUNNING)
    {
        return SAP_BUSY;
    }
    if (max_messages == 0u)
    {
        return SAP_OK;
    }

    for (i = 0u; i < max_messages; i++)
    {
        uint8_t *key = NULL;
        uint32_t key_len = 0u;
        uint8_t *frame = NULL;
        uint32_t frame_len = 0u;
        uint64_t key_worker = 0u;
        uint64_t key_seq = 0u;
        SapRunnerLeaseV0 lease = {0};
        int rc;

        rc = read_next_inbox_frame(runner->db, runner->worker_id, &key, &key_len, &frame,
                                   &frame_len);
        if (rc == SAP_NOTFOUND)
        {
            break;
        }
        if (rc != SAP_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        rc = sap_runner_v0_inbox_key_decode(key, key_len, &key_worker, &key_seq);
        if (rc != SAP_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        {
            int64_t now_ms = default_now_ms(NULL);
            int64_t deadline_ms = now_ms + RUNNER_LEASE_TTL_MS;

            rc = sap_runner_mailbox_v0_claim(runner->db, key_worker, key_seq, runner->worker_id,
                                             now_ms, deadline_ms, &lease);
        }
        if (rc == SAP_BUSY)
        {
            free(key);
            free(frame);
            break;
        }
        if (rc == SAP_NOTFOUND)
        {
            free(key);
            free(frame);
            continue;
        }
        if (rc != SAP_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        {
            int64_t step_start_ms = default_now_ms(NULL);
            int64_t step_end_ms;
            runner->metrics.step_attempts++;
            emit_replay_event(runner, SAP_RUNNER_V0_REPLAY_EVENT_INBOX_ATTEMPT, key_seq, SAP_OK,
                              frame, frame_len);
            rc = sap_runner_v0_run_step(runner, frame, frame_len, handler, ctx);
            step_end_ms = default_now_ms(NULL);
            metrics_note_latency(runner, step_start_ms, step_end_ms);
            emit_replay_event(runner, SAP_RUNNER_V0_REPLAY_EVENT_INBOX_RESULT, key_seq, (int32_t)rc,
                              frame, frame_len);
        }
        if (rc != SAP_OK)
        {
            int step_rc = rc;
            int disposition_rc = SAP_OK;
            int extracted = 0;
            const uint8_t *message_id = NULL;
            uint32_t message_id_len = 0u;

            metrics_note_failure(runner, step_rc);
            if (extract_message_id_from_frame(frame, frame_len, &message_id, &message_id_len) ==
                SAP_OK)
            {
                extracted = 1;
            }

            if (!extracted)
            {
                disposition_rc = sap_runner_dead_letter_v0_move(runner->db, key_worker, key_seq,
                                                                &lease, (int32_t)step_rc, 0u);
                if (disposition_rc == SAP_OK)
                {
                    runner->metrics.dead_letter_moves++;
                    emit_replay_event(runner, SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_DEAD_LETTER,
                                      key_seq, (int32_t)step_rc, frame, frame_len);
                }
            }
            else if (is_retryable_step_rc(step_rc))
            {
                uint32_t retry_count = 0u;
                int retry_rc =
                    retry_count_increment(runner->db, message_id, message_id_len, &retry_count);
                if (retry_rc != SAP_OK)
                {
                    free(key);
                    free(frame);
                    return retry_rc;
                }

                if (retry_count >= RUNNER_RETRY_BUDGET_MAX)
                {
                    disposition_rc = sap_runner_dead_letter_v0_move(
                        runner->db, key_worker, key_seq, &lease, (int32_t)step_rc, retry_count);
                    if (disposition_rc == SAP_OK)
                    {
                        runner->metrics.dead_letter_moves++;
                        emit_replay_event(runner,
                                          SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_DEAD_LETTER,
                                          key_seq, (int32_t)step_rc, frame, frame_len);
                        int clear_rc = retry_count_clear(runner->db, message_id, message_id_len);
                        if (clear_rc != SAP_OK)
                        {
                            free(key);
                            free(frame);
                            return clear_rc;
                        }
                    }
                }
                else
                {
                    disposition_rc =
                        requeue_claimed_inbox_message(runner->db, key_worker, key_seq, &lease);
                    if (disposition_rc == SAP_OK)
                    {
                        runner->metrics.requeues++;
                        emit_replay_event(runner, SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_REQUEUE,
                                          key_seq, (int32_t)step_rc, frame, frame_len);
                    }
                }
            }
            else
            {
                disposition_rc =
                    requeue_claimed_inbox_message(runner->db, key_worker, key_seq, &lease);
                if (disposition_rc == SAP_OK)
                {
                    runner->metrics.requeues++;
                    emit_replay_event(runner, SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_REQUEUE,
                                      key_seq, (int32_t)step_rc, frame, frame_len);
                }
            }

            free(key);
            free(frame);
            if (disposition_rc != SAP_OK)
            {
                return disposition_rc;
            }
            if (!extracted)
            {
                continue;
            }
            if (is_retryable_step_rc(step_rc))
            {
                continue;
            }
            return step_rc;
        }

        rc = sap_runner_mailbox_v0_ack(runner->db, key_worker, key_seq, &lease);
        if (rc != SAP_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        {
            const uint8_t *message_id = NULL;
            uint32_t message_id_len = 0u;
            if (extract_message_id_from_frame(frame, frame_len, &message_id, &message_id_len) ==
                SAP_OK)
            {
                int clear_rc = retry_count_clear(runner->db, message_id, message_id_len);
                if (clear_rc != SAP_OK)
                {
                    free(key);
                    free(frame);
                    return clear_rc;
                }
            }
        }
        free(key);
        free(frame);
        processed++;
        runner->metrics.step_successes++;
    }

    if (processed_out)
    {
        *processed_out = processed;
    }
    return SAP_OK;
}

int sap_runner_v0_worker_init(SapRunnerV0Worker *worker, const SapRunnerV0Config *cfg,
                              sap_runner_v0_message_handler handler, void *handler_ctx,
                              uint32_t max_batch)
{
    int rc;

    if (!worker || !cfg || !handler)
    {
        return SAP_ERROR;
    }

    memset(worker, 0, sizeof(*worker));
    rc = sap_runner_v0_init(&worker->runner, cfg);
    if (rc != SAP_OK)
    {
        return rc;
    }
    worker->handler = handler;
    worker->handler_ctx = handler_ctx;
    worker->max_batch = (max_batch == 0u) ? 1u : max_batch;
    worker->max_idle_sleep_ms = 1u;
    worker->now_ms_fn = NULL;
    worker->now_ms_ctx = NULL;
    worker->sleep_ms_fn = NULL;
    worker->sleep_ms_ctx = NULL;
    worker->ticks = 0u;
    worker->stop_requested = 0;
    worker->last_error = SAP_OK;
    return SAP_OK;
}

int sap_runner_v0_worker_tick(SapRunnerV0Worker *worker, uint32_t *processed_out)
{
    int rc;
    uint32_t processed = 0u;

    if (processed_out)
    {
        *processed_out = 0u;
    }
    if (!worker || !worker->handler)
    {
        return SAP_ERROR;
    }
    if (worker->stop_requested)
    {
        return SAP_BUSY;
    }

    rc = sap_runner_v0_poll_inbox(&worker->runner, worker->max_batch, worker->handler,
                                  worker->handler_ctx, &processed);
    if (rc != SAP_OK)
    {
        worker->last_error = rc;
        return rc;
    }

    if (processed < worker->max_batch)
    {
        TimerDispatchCtx timer_ctx = {0};
        uint32_t timer_budget;
        uint32_t timer_processed = 0u;

        timer_ctx.worker = worker;
        timer_ctx.count = 0u;
        timer_budget = worker->max_batch - processed;
        rc = sap_runner_timer_v0_drain_due(worker->runner.db, worker_now_ms(worker), timer_budget,
                                           timer_dispatch_handler, &timer_ctx, &timer_processed);
        if (rc != SAP_OK)
        {
            worker->last_error = rc;
            return rc;
        }
        processed += timer_processed;
    }

    if (processed_out)
    {
        *processed_out = processed;
    }
    worker->ticks++;
    return SAP_OK;
}

void sap_runner_v0_worker_set_idle_policy(SapRunnerV0Worker *worker, uint32_t max_idle_sleep_ms)
{
    if (!worker)
    {
        return;
    }
    worker->max_idle_sleep_ms = (max_idle_sleep_ms == 0u) ? 1u : max_idle_sleep_ms;
}

void sap_runner_v0_worker_set_time_hooks(SapRunnerV0Worker *worker, int64_t (*now_ms_fn)(void *ctx),
                                         void *now_ms_ctx,
                                         void (*sleep_ms_fn)(uint32_t sleep_ms, void *ctx),
                                         void *sleep_ms_ctx)
{
    if (!worker)
    {
        return;
    }
    worker->now_ms_fn = now_ms_fn;
    worker->now_ms_ctx = now_ms_ctx;
    worker->sleep_ms_fn = sleep_ms_fn;
    worker->sleep_ms_ctx = sleep_ms_ctx;
}

int sap_runner_v0_worker_compute_idle_sleep_ms(SapRunnerV0Worker *worker, uint32_t *sleep_ms_out)
{
    int rc;
    int64_t next_due = 0;
    uint32_t max_idle = 0u;

    if (!worker || !sleep_ms_out)
    {
        return SAP_ERROR;
    }
    *sleep_ms_out = 0u;

    max_idle = (worker->max_idle_sleep_ms == 0u) ? 1u : worker->max_idle_sleep_ms;
    rc = sap_runner_scheduler_v0_next_due(worker->runner.db, &next_due);
    if (rc == SAP_NOTFOUND)
    {
        *sleep_ms_out = max_idle;
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        return rc;
    }

    rc = sap_runner_scheduler_v0_compute_sleep_ms(worker_now_ms(worker), next_due, max_idle,
                                                  sleep_ms_out);
    return rc;
}

void sap_runner_v0_worker_request_stop(SapRunnerV0Worker *worker)
{
    if (!worker)
    {
        return;
    }
    worker->stop_requested = 1;
}

void sap_runner_v0_worker_shutdown(SapRunnerV0Worker *worker)
{
    if (!worker)
    {
        return;
    }
    sap_runner_v0_worker_request_stop(worker);
    sap_runner_v0_shutdown(&worker->runner);
}

#ifdef SAPLING_THREADED
static void *runner_thread_main(void *arg)
{
    SapRunnerV0Worker *worker = (SapRunnerV0Worker *)arg;

    while (!worker->stop_requested)
    {
        uint32_t processed = 0u;
        int rc = sap_runner_v0_worker_tick(worker, &processed);

        if (rc == SAP_BUSY && worker->stop_requested)
        {
            break;
        }
        if (rc != SAP_OK)
        {
            worker->last_error = rc;
            break;
        }
        if (processed == 0u)
        {
            uint32_t sleep_ms = 0u;
            rc = sap_runner_v0_worker_compute_idle_sleep_ms(worker, &sleep_ms);
            if (rc != SAP_OK)
            {
                worker->last_error = rc;
                break;
            }
            worker_sleep_ms(worker, sleep_ms);
        }
    }

    sap_runner_v0_shutdown(&worker->runner);
    return NULL;
}
#endif

int sap_runner_v0_worker_start(SapRunnerV0Worker *worker)
{
#ifdef SAPLING_THREADED
    if (!worker || !worker->handler)
    {
        return SAP_ERROR;
    }
    if (worker->thread_started)
    {
        return SAP_BUSY;
    }
    worker->stop_requested = 0;
    worker->last_error = SAP_OK;
    if (pthread_create(&worker->thread, NULL, runner_thread_main, worker) != 0)
    {
        return SAP_ERROR;
    }
    worker->thread_started = 1;
    return SAP_OK;
#else
    (void)worker;
    return SAP_ERROR;
#endif
}

int sap_runner_v0_worker_join(SapRunnerV0Worker *worker)
{
#ifdef SAPLING_THREADED
    if (!worker || !worker->thread_started)
    {
        return SAP_ERROR;
    }
    if (pthread_join(worker->thread, NULL) != 0)
    {
        return SAP_ERROR;
    }
    worker->thread_started = 0;
    return SAP_OK;
#else
    (void)worker;
    return SAP_ERROR;
#endif
}
