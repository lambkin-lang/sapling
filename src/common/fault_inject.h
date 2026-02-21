/*
 * fault_inject.h - deterministic fault-injection scaffolding
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef SAPLING_FAULT_INJECT_H
#define SAPLING_FAULT_INJECT_H

#include <stdint.h>

#define SAP_FI_MAX_RULES 32u

typedef struct
{
    const char *site;
    uint32_t fail_at_hit; /* 1-based hit number */
    uint32_t hit_count;
    uint8_t active;
} SapFaultRule;

typedef struct
{
    SapFaultRule rules[SAP_FI_MAX_RULES];
    uint32_t num_rules;
} SapFaultInjector;

void sap_fi_reset(SapFaultInjector *fi);
int sap_fi_add_rule(SapFaultInjector *fi, const char *site, uint32_t fail_at_hit);
int sap_fi_should_fail(SapFaultInjector *fi, const char *site);

#endif /* SAPLING_FAULT_INJECT_H */
