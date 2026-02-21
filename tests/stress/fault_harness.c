/*
 * fault_harness.c - phase-0 deterministic fault-injection harness scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "common/fault_inject.h"

int main(void)
{
    SapFaultInjector fi;
    int failed;

    sap_fi_reset(&fi);
    if (sap_fi_add_rule(&fi, "alloc.page", 3) != 0)
    {
        return 1;
    }

    /* First two hits pass, third hit fails deterministically. */
    if (sap_fi_should_fail(&fi, "alloc.page"))
    {
        return 2;
    }
    if (sap_fi_should_fail(&fi, "alloc.page"))
    {
        return 3;
    }
    failed = sap_fi_should_fail(&fi, "alloc.page");
    if (!failed)
    {
        return 4;
    }

    /* Further hits are currently pass-through for this scaffold. */
    if (sap_fi_should_fail(&fi, "alloc.page"))
    {
        return 5;
    }

    return 0;
}
