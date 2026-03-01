/*
 * runner_threaded_pipeline_example.c - C-level 4-thread order pipeline sample
 *
 * SPDX-License-Identifier: MIT
 */
#include "sapling/sapling.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PIPELINE_ORDER_COUNT 64u
#define PIPELINE_TIMEOUT_MS 12000u
#define PIPELINE_POLL_SLEEP_MS 5u
#define PIPELINE_QUEUE_KEY_CAP 64u
#define PIPELINE_STATUS_KEY_CAP 64u

static const uint8_t k_queue_payment_prefix[] = {'q', '.', 'p', 'a', 'y', ':'};
static const uint8_t k_queue_inventory_prefix[] = {'q', '.', 'i', 'n', 'v', ':'};
static const uint8_t k_queue_shipping_prefix[] = {'q', '.', 's', 'h', 'i', 'p', ':'};

static const uint8_t k_key_orders_received[] = {'o', 'r', 'd', 'e', 'r', 's', '.', 'r',
                                                'e', 'c', 'e', 'i', 'v', 'e', 'd'};
static const uint8_t k_key_orders_paid[] = {'o', 'r', 'd', 'e', 'r', 's', '.', 'p', 'a', 'i', 'd'};
static const uint8_t k_key_orders_reserved[] = {'o', 'r', 'd', 'e', 'r', 's', '.', 'r',
                                                'e', 's', 'e', 'r', 'v', 'e', 'd'};
static const uint8_t k_key_orders_shipped[] = {'o', 'r', 'd', 'e', 'r', 's', '.',
                                               's', 'h', 'i', 'p', 'p', 'e', 'd'};
static const uint8_t k_key_inventory_available[] = {
    'i', 'n', 'v', 'e', 'n', 't', 'o', 'r', 'y', '.', 'a', 'v', 'a', 'i', 'l', 'a', 'b', 'l', 'e'};

static const uint8_t k_status_accepted[] = {'a', 'c', 'c', 'e', 'p', 't', 'e', 'd'};
static const uint8_t k_status_paid[] = {'p', 'a', 'i', 'd'};
static const uint8_t k_status_reserved[] = {'r', 'e', 's', 'e', 'r', 'v', 'e', 'd'};
static const uint8_t k_status_shipped[] = {'s', 'h', 'i', 'p', 'p', 'e', 'd'};

typedef struct
{
    DB *db;
    pthread_mutex_t db_mu;
    uint32_t order_count;
    int stop_requested;
} PipelineCtx;

typedef struct
{
    PipelineCtx *pipeline;
    const char *name;
    const uint8_t *in_prefix;
    const uint8_t *out_prefix;
    const uint8_t *counter_key;
    const uint8_t *status_value;
    uint64_t processed;
    uint32_t in_prefix_len;
    uint32_t out_prefix_len;
    uint32_t counter_key_len;
    uint32_t status_value_len;
    uint32_t compute_delay_ms;
    int adjust_inventory;
    int last_rc;
} StageThreadCtx;

static void *test_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void test_free(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

static SapMemArena *g_alloc = NULL;

static void wr64be(uint8_t out[8], uint64_t v)
{
    int i;

    for (i = 7; i >= 0; i--)
    {
        out[i] = (uint8_t)(v & 0xffu);
        v >>= 8;
    }
}

static uint64_t rd64be(const uint8_t in[8])
{
    uint64_t v = 0u;
    uint32_t i;

    for (i = 0u; i < 8u; i++)
    {
        v = (v << 8) | (uint64_t)in[i];
    }
    return v;
}

static int64_t wall_now_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000 + (int64_t)(ts.tv_nsec / 1000000L);
}

static void sleep_ms(uint32_t ms)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000u);
    ts.tv_nsec = (long)((ms % 1000u) * 1000000L);
    nanosleep(&ts, NULL);
}

static int pipeline_stop_requested(const PipelineCtx *ctx)
{
    if (!ctx)
    {
        return 1;
    }
    return __atomic_load_n(&ctx->stop_requested, __ATOMIC_ACQUIRE);
}

static void pipeline_request_stop(PipelineCtx *ctx)
{
    if (!ctx)
    {
        return;
    }
    __atomic_store_n(&ctx->stop_requested, 1, __ATOMIC_RELEASE);
}

static int build_order_status_key(uint64_t order_id, uint8_t *key_out, uint32_t key_cap,
                                  uint32_t *key_len_out)
{
    int n;

    if (!key_out || key_cap == 0u || !key_len_out)
    {
        return ERR_CORRUPT;
    }

    n = snprintf((char *)key_out, (size_t)key_cap, "order:%" PRIu64 ":status", order_id);
    if (n < 0 || (uint32_t)n >= key_cap)
    {
        return ERR_FULL;
    }

    *key_len_out = (uint32_t)n;
    return ERR_OK;
}

static int build_queue_key(const uint8_t *prefix, uint32_t prefix_len, uint64_t seq,
                           uint8_t *key_out, uint32_t key_cap, uint32_t *key_len_out)
{
    if (!prefix || prefix_len == 0u || !key_out || !key_len_out)
    {
        return ERR_CORRUPT;
    }
    if (prefix_len + 8u > key_cap)
    {
        return ERR_FULL;
    }

    memcpy(key_out, prefix, prefix_len);
    wr64be(key_out + prefix_len, seq);
    *key_len_out = prefix_len + 8u;
    return ERR_OK;
}

static int txn_read_u64_default(Txn *txn, const uint8_t *key, uint32_t key_len, uint64_t default_v,
                                uint64_t *value_out)
{
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!txn || !key || key_len == 0u || !value_out)
    {
        return ERR_CORRUPT;
    }

    rc = txn_get(txn, key, key_len, &val, &val_len);
    if (rc == ERR_NOT_FOUND)
    {
        *value_out = default_v;
        return ERR_OK;
    }
    if (rc != ERR_OK)
    {
        return rc;
    }
    if (!val || val_len != 8u)
    {
        return ERR_CONFLICT;
    }

    *value_out = rd64be((const uint8_t *)val);
    return ERR_OK;
}

static int txn_write_u64(Txn *txn, const uint8_t *key, uint32_t key_len, uint64_t value)
{
    uint8_t raw[8];

    if (!txn || !key || key_len == 0u)
    {
        return ERR_CORRUPT;
    }

    wr64be(raw, value);
    return txn_put(txn, key, key_len, raw, sizeof(raw));
}

static int pipeline_queue_pop(PipelineCtx *ctx, const uint8_t *prefix, uint32_t prefix_len,
                              uint64_t seq, uint64_t *order_id_out)
{
    uint8_t key[PIPELINE_QUEUE_KEY_CAP];
    uint32_t key_len = 0u;
    Txn *txn = NULL;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!ctx || !prefix || prefix_len == 0u || !order_id_out)
    {
        return ERR_CORRUPT;
    }

    rc = build_queue_key(prefix, prefix_len, seq, key, sizeof(key), &key_len);
    if (rc != ERR_OK)
    {
        return rc;
    }

    (void)pthread_mutex_lock(&ctx->db_mu);

    txn = txn_begin(ctx->db, NULL, 0u);
    if (!txn)
    {
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_BUSY;
    }

    rc = txn_get(txn, key, key_len, &val, &val_len);
    if (rc == ERR_NOT_FOUND)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_NOT_FOUND;
    }
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }
    if (!val || val_len != 8u)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_CONFLICT;
    }

    *order_id_out = rd64be((const uint8_t *)val);

    rc = txn_del(txn, key, key_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }

    rc = txn_commit(txn);
    (void)pthread_mutex_unlock(&ctx->db_mu);
    return rc;
}

static int pipeline_stage_commit(PipelineCtx *ctx, const StageThreadCtx *stage, uint64_t order_id,
                                 uint64_t out_seq)
{
    uint8_t status_key[PIPELINE_STATUS_KEY_CAP];
    uint32_t status_key_len = 0u;
    Txn *txn = NULL;
    uint64_t counter = 0u;
    int rc;

    if (!ctx || !stage || !stage->counter_key || stage->counter_key_len == 0u ||
        !stage->status_value || stage->status_value_len == 0u)
    {
        return ERR_CORRUPT;
    }

    rc = build_order_status_key(order_id, status_key, sizeof(status_key), &status_key_len);
    if (rc != ERR_OK)
    {
        return rc;
    }

    (void)pthread_mutex_lock(&ctx->db_mu);

    txn = txn_begin(ctx->db, NULL, 0u);
    if (!txn)
    {
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_BUSY;
    }

    rc = txn_read_u64_default(txn, stage->counter_key, stage->counter_key_len, 0u, &counter);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }
    rc = txn_write_u64(txn, stage->counter_key, stage->counter_key_len, counter + 1u);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }

    if (stage->adjust_inventory)
    {
        uint64_t available = 0u;

        rc = txn_read_u64_default(txn, k_key_inventory_available, sizeof(k_key_inventory_available),
                                  0u, &available);
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            (void)pthread_mutex_unlock(&ctx->db_mu);
            return rc;
        }
        if (available == 0u)
        {
            txn_abort(txn);
            (void)pthread_mutex_unlock(&ctx->db_mu);
            return ERR_CONFLICT;
        }
        rc = txn_write_u64(txn, k_key_inventory_available, sizeof(k_key_inventory_available),
                           available - 1u);
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            (void)pthread_mutex_unlock(&ctx->db_mu);
            return rc;
        }
    }

    rc = txn_put(txn, status_key, status_key_len, stage->status_value, stage->status_value_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }

    if (stage->out_prefix && stage->out_prefix_len > 0u)
    {
        uint8_t out_key[PIPELINE_QUEUE_KEY_CAP];
        uint32_t out_key_len = 0u;
        uint8_t raw_order[8];

        rc = build_queue_key(stage->out_prefix, stage->out_prefix_len, out_seq, out_key,
                             sizeof(out_key), &out_key_len);
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            (void)pthread_mutex_unlock(&ctx->db_mu);
            return rc;
        }

        wr64be(raw_order, order_id);
        rc = txn_put_flags(txn, out_key, out_key_len, raw_order, sizeof(raw_order), SAP_NOOVERWRITE,
                           NULL);
        if (rc != ERR_OK)
        {
            txn_abort(txn);
            (void)pthread_mutex_unlock(&ctx->db_mu);
            return rc;
        }
    }

    rc = txn_commit(txn);
    (void)pthread_mutex_unlock(&ctx->db_mu);
    return rc;
}

static int pipeline_write_u64(PipelineCtx *ctx, const uint8_t *key, uint32_t key_len,
                              uint64_t value)
{
    Txn *txn = NULL;
    int rc;

    if (!ctx || !key || key_len == 0u)
    {
        return ERR_CORRUPT;
    }

    (void)pthread_mutex_lock(&ctx->db_mu);

    txn = txn_begin(ctx->db, NULL, 0u);
    if (!txn)
    {
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_BUSY;
    }

    rc = txn_write_u64(txn, key, key_len, value);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }

    rc = txn_commit(txn);
    (void)pthread_mutex_unlock(&ctx->db_mu);
    return rc;
}

static int pipeline_read_u64(PipelineCtx *ctx, const uint8_t *key, uint32_t key_len,
                             uint64_t *value_out)
{
    Txn *txn = NULL;
    int rc;

    if (!ctx || !key || key_len == 0u || !value_out)
    {
        return ERR_CORRUPT;
    }

    (void)pthread_mutex_lock(&ctx->db_mu);

    txn = txn_begin(ctx->db, NULL, TXN_RDONLY);
    if (!txn)
    {
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_CORRUPT;
    }

    rc = txn_read_u64_default(txn, key, key_len, 0u, value_out);
    txn_abort(txn);
    (void)pthread_mutex_unlock(&ctx->db_mu);
    return rc;
}

static int pipeline_read_status(PipelineCtx *ctx, uint64_t order_id, uint8_t *status_out,
                                uint32_t status_cap, uint32_t *status_len_out)
{
    uint8_t key[PIPELINE_STATUS_KEY_CAP];
    uint32_t key_len = 0u;
    Txn *txn = NULL;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!ctx || !status_out || status_cap == 0u || !status_len_out)
    {
        return ERR_CORRUPT;
    }
    *status_len_out = 0u;

    rc = build_order_status_key(order_id, key, sizeof(key), &key_len);
    if (rc != ERR_OK)
    {
        return rc;
    }

    (void)pthread_mutex_lock(&ctx->db_mu);

    txn = txn_begin(ctx->db, NULL, TXN_RDONLY);
    if (!txn)
    {
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_CORRUPT;
    }

    rc = txn_get(txn, key, key_len, &val, &val_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return rc;
    }
    if (!val || val_len > status_cap)
    {
        txn_abort(txn);
        (void)pthread_mutex_unlock(&ctx->db_mu);
        return ERR_FULL;
    }

    memcpy(status_out, val, val_len);
    *status_len_out = val_len;

    txn_abort(txn);
    (void)pthread_mutex_unlock(&ctx->db_mu);
    return ERR_OK;
}

static void *stage_thread_main(void *arg)
{
    StageThreadCtx *stage = (StageThreadCtx *)arg;
    uint64_t seq = 1u;

    if (!stage || !stage->pipeline)
    {
        return NULL;
    }

    stage->last_rc = ERR_OK;
    stage->processed = 0u;

    while (seq <= stage->pipeline->order_count)
    {
        uint64_t order_id = 0u;
        int rc;

        if (pipeline_stop_requested(stage->pipeline))
        {
            break;
        }

        if (stage->in_prefix && stage->in_prefix_len > 0u)
        {
            rc = pipeline_queue_pop(stage->pipeline, stage->in_prefix, stage->in_prefix_len, seq,
                                    &order_id);
            if (rc == ERR_NOT_FOUND)
            {
                sleep_ms(1u);
                continue;
            }
            if (rc == ERR_BUSY)
            {
                sleep_ms(1u);
                continue;
            }
            if (rc != ERR_OK)
            {
                stage->last_rc = rc;
                pipeline_request_stop(stage->pipeline);
                break;
            }
        }
        else
        {
            order_id = seq;
        }

        if (stage->compute_delay_ms > 0u)
        {
            sleep_ms(stage->compute_delay_ms);
        }

        rc = pipeline_stage_commit(stage->pipeline, stage, order_id, seq);
        if (rc == ERR_BUSY)
        {
            sleep_ms(1u);
            continue;
        }
        if (rc != ERR_OK)
        {
            stage->last_rc = rc;
            pipeline_request_stop(stage->pipeline);
            break;
        }

        stage->processed++;
        seq++;
    }

    return NULL;
}

static int verify_pipeline_state(PipelineCtx *pipeline, uint32_t order_count)
{
    uint64_t received = 0u;
    uint64_t paid = 0u;
    uint64_t reserved = 0u;
    uint64_t shipped = 0u;
    uint64_t inventory = 0u;
    uint32_t i;

    if (!pipeline || order_count == 0u)
    {
        return ERR_CORRUPT;
    }

    if (pipeline_read_u64(pipeline, k_key_orders_received, sizeof(k_key_orders_received),
                          &received) != ERR_OK ||
        pipeline_read_u64(pipeline, k_key_orders_paid, sizeof(k_key_orders_paid), &paid) !=
            ERR_OK ||
        pipeline_read_u64(pipeline, k_key_orders_reserved, sizeof(k_key_orders_reserved),
                          &reserved) != ERR_OK ||
        pipeline_read_u64(pipeline, k_key_orders_shipped, sizeof(k_key_orders_shipped), &shipped) !=
            ERR_OK ||
        pipeline_read_u64(pipeline, k_key_inventory_available, sizeof(k_key_inventory_available),
                          &inventory) != ERR_OK)
    {
        return ERR_CORRUPT;
    }

    if (received != order_count || paid != order_count || reserved != order_count ||
        shipped != order_count || inventory != 0u)
    {
        return ERR_CONFLICT;
    }

    for (i = 0u; i < order_count; i++)
    {
        uint8_t status[32];
        uint32_t status_len = 0u;

        if (pipeline_read_status(pipeline, (uint64_t)i + 1u, status, sizeof(status), &status_len) !=
            ERR_OK)
        {
            return ERR_CORRUPT;
        }
        if (status_len != sizeof(k_status_shipped) ||
            memcmp(status, k_status_shipped, sizeof(k_status_shipped)) != 0)
        {
            return ERR_CONFLICT;
        }
    }

    return ERR_OK;
}

int main(void)
{
    SapArenaOptions g_alloc_opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL
    };
    sap_arena_init(&g_alloc, &g_alloc_opts);

    PipelineCtx pipeline;
    StageThreadCtx stages[4];
    pthread_t threads[4];
    int created[4] = {0, 0, 0, 0};
    const int64_t deadline_ms = wall_now_ms() + PIPELINE_TIMEOUT_MS;
    uint32_t i;
    int rc = 1;

    memset(&pipeline, 0, sizeof(pipeline));
    memset(stages, 0, sizeof(stages));

    pipeline.db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!pipeline.db)
    {
        fprintf(stderr, "runner-threaded-pipeline-example: db_open failed\n");
        goto done;
    }
    dbi_open(pipeline.db, 10u, NULL, NULL, 0u);
    if (pthread_mutex_init(&pipeline.db_mu, NULL) != 0)
    {
        fprintf(stderr, "runner-threaded-pipeline-example: mutex init failed\n");
        goto done;
    }
    pipeline.order_count = PIPELINE_ORDER_COUNT;
    pipeline.stop_requested = 0;

    if (pipeline_write_u64(&pipeline, k_key_inventory_available, sizeof(k_key_inventory_available),
                           PIPELINE_ORDER_COUNT) != ERR_OK)
    {
        fprintf(stderr, "runner-threaded-pipeline-example: inventory init failed\n");
        goto done;
    }

    stages[0].pipeline = &pipeline;
    stages[0].name = "ingest";
    stages[0].in_prefix = NULL;
    stages[0].in_prefix_len = 0u;
    stages[0].out_prefix = k_queue_payment_prefix;
    stages[0].out_prefix_len = sizeof(k_queue_payment_prefix);
    stages[0].counter_key = k_key_orders_received;
    stages[0].counter_key_len = sizeof(k_key_orders_received);
    stages[0].status_value = k_status_accepted;
    stages[0].status_value_len = sizeof(k_status_accepted);
    stages[0].compute_delay_ms = 1u;
    stages[0].adjust_inventory = 0;
    stages[0].last_rc = ERR_OK;

    stages[1].pipeline = &pipeline;
    stages[1].name = "payment";
    stages[1].in_prefix = k_queue_payment_prefix;
    stages[1].in_prefix_len = sizeof(k_queue_payment_prefix);
    stages[1].out_prefix = k_queue_inventory_prefix;
    stages[1].out_prefix_len = sizeof(k_queue_inventory_prefix);
    stages[1].counter_key = k_key_orders_paid;
    stages[1].counter_key_len = sizeof(k_key_orders_paid);
    stages[1].status_value = k_status_paid;
    stages[1].status_value_len = sizeof(k_status_paid);
    stages[1].compute_delay_ms = 2u;
    stages[1].adjust_inventory = 0;
    stages[1].last_rc = ERR_OK;

    stages[2].pipeline = &pipeline;
    stages[2].name = "inventory";
    stages[2].in_prefix = k_queue_inventory_prefix;
    stages[2].in_prefix_len = sizeof(k_queue_inventory_prefix);
    stages[2].out_prefix = k_queue_shipping_prefix;
    stages[2].out_prefix_len = sizeof(k_queue_shipping_prefix);
    stages[2].counter_key = k_key_orders_reserved;
    stages[2].counter_key_len = sizeof(k_key_orders_reserved);
    stages[2].status_value = k_status_reserved;
    stages[2].status_value_len = sizeof(k_status_reserved);
    stages[2].compute_delay_ms = 2u;
    stages[2].adjust_inventory = 1;
    stages[2].last_rc = ERR_OK;

    stages[3].pipeline = &pipeline;
    stages[3].name = "shipping";
    stages[3].in_prefix = k_queue_shipping_prefix;
    stages[3].in_prefix_len = sizeof(k_queue_shipping_prefix);
    stages[3].out_prefix = NULL;
    stages[3].out_prefix_len = 0u;
    stages[3].counter_key = k_key_orders_shipped;
    stages[3].counter_key_len = sizeof(k_key_orders_shipped);
    stages[3].status_value = k_status_shipped;
    stages[3].status_value_len = sizeof(k_status_shipped);
    stages[3].compute_delay_ms = 1u;
    stages[3].adjust_inventory = 0;
    stages[3].last_rc = ERR_OK;

    for (i = 0u; i < 4u; i++)
    {
        if (pthread_create(&threads[i], NULL, stage_thread_main, &stages[i]) != 0)
        {
            fprintf(stderr, "runner-threaded-pipeline-example: thread create failed for %s\n",
                    stages[i].name ? stages[i].name : "stage");
            pipeline_request_stop(&pipeline);
            goto done;
        }
        created[i] = 1;
    }

    for (;;)
    {
        uint64_t shipped = 0u;

        if (pipeline_read_u64(&pipeline, k_key_orders_shipped, sizeof(k_key_orders_shipped),
                              &shipped) != ERR_OK)
        {
            fprintf(stderr, "runner-threaded-pipeline-example: shipped counter read failed\n");
            pipeline_request_stop(&pipeline);
            goto done;
        }

        if (shipped >= PIPELINE_ORDER_COUNT)
        {
            break;
        }
        if (pipeline_stop_requested(&pipeline))
        {
            break;
        }
        if (wall_now_ms() > deadline_ms)
        {
            fprintf(stderr, "runner-threaded-pipeline-example: timeout waiting for completion\n");
            pipeline_request_stop(&pipeline);
            goto done;
        }
        sleep_ms(PIPELINE_POLL_SLEEP_MS);
    }

    for (i = 0u; i < 4u; i++)
    {
        if (created[i])
        {
            if (pthread_join(threads[i], NULL) != 0)
            {
                fprintf(stderr, "runner-threaded-pipeline-example: thread join failed for %s\n",
                        stages[i].name ? stages[i].name : "stage");
                goto done;
            }
            created[i] = 0;
        }
    }

    for (i = 0u; i < 4u; i++)
    {
        if (stages[i].last_rc != ERR_OK)
        {
            fprintf(stderr, "runner-threaded-pipeline-example: stage %s failed rc=%d\n",
                    stages[i].name ? stages[i].name : "stage", stages[i].last_rc);
            goto done;
        }
        if (stages[i].processed != PIPELINE_ORDER_COUNT)
        {
            fprintf(stderr,
                    "runner-threaded-pipeline-example: stage %s processed=%" PRIu64
                    " expected=%u\n",
                    stages[i].name ? stages[i].name : "stage", stages[i].processed,
                    PIPELINE_ORDER_COUNT);
            goto done;
        }
    }

    if (verify_pipeline_state(&pipeline, PIPELINE_ORDER_COUNT) != ERR_OK)
    {
        fprintf(stderr, "runner-threaded-pipeline-example: final verification failed\n");
        goto done;
    }

    printf("runner-threaded-pipeline-example: OK threads=4 orders=%u\n", PIPELINE_ORDER_COUNT);
    rc = 0;

done:
    pipeline_request_stop(&pipeline);

    for (i = 0u; i < 4u; i++)
    {
        if (created[i])
        {
            (void)pthread_join(threads[i], NULL);
            created[i] = 0;
        }
    }

    if (pipeline.db)
    {
        (void)pthread_mutex_destroy(&pipeline.db_mu);
        db_close(pipeline.db);
    }

    return rc;
}
