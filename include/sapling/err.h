/*
 * err.h — unified error codes for all Sapling subsystems
 *
 * Every public function in Sapling, Seq, Text, Thatch, and Thatch-JSON
 * returns one of these codes (int).  The wire protocol (wire_v0.h) keeps
 * its own typed enum because it is an internal encoding layer.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_ERR_H
#define SAPLING_ERR_H

#define ERR_OK        0  /* success                                       */
#define ERR_OOM       1  /* allocation failure                            */
#define ERR_INVALID   2  /* invalid argument, NULL pointer, bad state     */
#define ERR_NOT_FOUND 3  /* key / field / index not found                 */
#define ERR_RANGE     4  /* index out of range / cursor past region       */
#define ERR_EMPTY     5  /* operation on empty collection                 */
#define ERR_FULL      6  /* key+value too large for a single page         */
#define ERR_READONLY  7  /* write attempted on a read-only transaction    */
#define ERR_BUSY      8  /* resource contention / metadata change blocked */
#define ERR_EXISTS    9  /* duplicate key (with NOOVERWRITE)              */
#define ERR_CONFLICT 10  /* compare-and-swap value mismatch               */
#define ERR_CORRUPT  11  /* data integrity failure                        */
#define ERR_PARSE    12  /* syntax error (JSON, etc.)                     */
#define ERR_TYPE     13  /* wrong type for the operation                  */

/* Diagnostic helper — returns a stable, human-readable string.
 * Safe to call with any int; returns "UNKNOWN" for unrecognised codes. */
const char *err_to_string(int err);

#endif /* SAPLING_ERR_H */
