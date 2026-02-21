/*
 * runner_v0.c - phase-A runner lifecycle and schema guards
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/runner_v0.h"

#include "generated/wit_schema_dbis.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

#define RUNNER_SCHEMA_KEY_STR "runner.schema.version"
#define RUNNER_SCHEMA_KEY_LEN ((uint32_t)(sizeof(RUNNER_SCHEMA_KEY_STR) - 1u))
#define RUNNER_SCHEMA_VAL_LEN 8u

static const uint8_t k_runner_schema_key[] = RUNNER_SCHEMA_KEY_STR;
static const uint8_t k_runner_schema_magic[4] = {'R', 'S', 'V', '0'};

static uint16_t rd16(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
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

static int delete_inbox_if_match(DB *db, const uint8_t *key, uint32_t key_len, const uint8_t *frame,
                                 uint32_t frame_len)
{
    Txn *txn;
    const void *current_val = NULL;
    uint32_t current_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !frame || frame_len == 0u)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, key_len, &current_val, &current_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (current_len != frame_len || memcmp(current_val, frame, frame_len) != 0)
    {
        txn_abort(txn);
        return SAP_CONFLICT;
    }
    rc = txn_del_dbi(txn, SAP_WIT_DBI_INBOX, key, key_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    rc = txn_commit(txn);
    return rc;
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
    return SAP_OK;
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

        rc = sap_runner_v0_run_step(runner, frame, frame_len, handler, ctx);
        if (rc != SAP_OK)
        {
            free(key);
            free(frame);
            return rc;
        }

        rc = delete_inbox_if_match(runner->db, key, key_len, frame, frame_len);
        free(key);
        free(frame);
        if (rc != SAP_OK)
        {
            return rc;
        }
        processed++;
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
    worker->ticks = 0u;
    worker->stop_requested = 0;
    worker->last_error = SAP_OK;
    return SAP_OK;
}

int sap_runner_v0_worker_tick(SapRunnerV0Worker *worker, uint32_t *processed_out)
{
    int rc;

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
                                  worker->handler_ctx, processed_out);
    if (rc != SAP_OK)
    {
        worker->last_error = rc;
        return rc;
    }
    worker->ticks++;
    return SAP_OK;
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
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 1000000L; /* 1ms */
            nanosleep(&ts, NULL);
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
