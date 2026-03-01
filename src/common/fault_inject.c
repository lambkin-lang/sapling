/*
 * fault_inject.c - deterministic fault-injection scaffolding
 *
 * SPDX-License-Identifier: MIT
 */
#include "common/fault_inject.h"

#include <string.h>

void sap_fi_reset(SapFaultInjector *fi)
{
    if (!fi)
    {
        return;
    }
    memset(fi, 0, sizeof(*fi));
}

int sap_fi_add_rule(SapFaultInjector *fi, const char *site, uint32_t fail_at_hit)
{
    SapFaultRule *rule;

    if (!fi || !site || fail_at_hit == 0)
    {
        return -1;
    }
    if (fi->num_rules >= SAP_FI_MAX_RULES)
    {
        return -1;
    }

    rule = &fi->rules[fi->num_rules++];
    rule->site = site;
    rule->fail_at_hit = fail_at_hit;
    rule->fail_rate_pct = 0;
    rule->hit_count = 0;
    rule->fail_count = 0;
    rule->active = 1;
    return 0;
}

int sap_fi_add_rate_rule(SapFaultInjector *fi, const char *site, uint32_t fail_rate_pct)
{
    SapFaultRule *rule;

    if (!fi || !site || fail_rate_pct == 0 || fail_rate_pct > 100)
    {
        return -1;
    }
    if (fi->num_rules >= SAP_FI_MAX_RULES)
    {
        return -1;
    }

    rule = &fi->rules[fi->num_rules++];
    rule->site = site;
    rule->fail_at_hit = 0; /* rate mode */
    rule->fail_rate_pct = fail_rate_pct;
    rule->hit_count = 0;
    rule->fail_count = 0;
    rule->active = 1;
    return 0;
}

int sap_fi_should_fail(SapFaultInjector *fi, const char *site)
{
    uint32_t i;

    if (!fi || !site)
    {
        return 0;
    }

    for (i = 0; i < fi->num_rules; i++)
    {
        SapFaultRule *rule = &fi->rules[i];
        if (!rule->active || !rule->site || strcmp(rule->site, site) != 0)
        {
            continue;
        }
        rule->hit_count++;
        if (rule->fail_at_hit > 0)
        {
            /* Deterministic mode: fail on exact hit */
            if (rule->hit_count == rule->fail_at_hit)
            {
                rule->fail_count++;
                return 1;
            }
        }
        else if (rule->fail_rate_pct > 0)
        {
            /* Rate mode: distributed failures with exact long-run rate */
            uint32_t h = rule->hit_count;
            uint32_t p = rule->fail_rate_pct;
            if ((h * p) / 100 != (((h - 1) * p) / 100))
            {
                rule->fail_count++;
                return 1;
            }
        }
    }
    return 0;
}
