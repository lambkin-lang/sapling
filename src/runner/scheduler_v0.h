/*
 * scheduler_v0.h - phase-C timer wake scheduling helper scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_RUNNER_SCHEDULER_V0_H
#define SAPLING_RUNNER_SCHEDULER_V0_H

#include "runner/timer_v0.h"

#include <stdint.h>

/* Return earliest due_ts in timers DBI, or SAP_NOTFOUND when empty. */
int sap_runner_scheduler_v0_next_due(DB *db, int64_t *due_ts_out);

/* Compute sleep budget in milliseconds based on now, next_due, and cap. */
int sap_runner_scheduler_v0_compute_sleep_ms(int64_t now_ts, int64_t next_due_ts,
                                             uint32_t max_sleep_ms, uint32_t *sleep_ms_out);

#endif /* SAPLING_RUNNER_SCHEDULER_V0_H */
