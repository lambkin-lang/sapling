/*
 * nomalloc.h — Compile-time enforcement of arena-only allocation
 *
 * When SAP_NO_MALLOC is defined, this header poisons malloc, calloc,
 * realloc, and free so that any accidental use in arena-migrated
 * subsystem files triggers a compile error.
 *
 * Include this as the LAST header in each migrated .c file.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_NOMALLOC_H
#define SAPLING_NOMALLOC_H

#ifdef SAP_NO_MALLOC
#pragma GCC poison malloc calloc realloc free
#endif

#endif /* SAPLING_NOMALLOC_H */
