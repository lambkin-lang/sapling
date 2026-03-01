/*
 * err.c — err_to_string diagnostic helper
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/err.h"

const char *err_to_string(int err)
{
    switch (err)
    {
    case ERR_OK:        return "OK";
    case ERR_OOM:       return "OOM";
    case ERR_INVALID:   return "INVALID";
    case ERR_NOT_FOUND: return "NOT_FOUND";
    case ERR_RANGE:     return "RANGE";
    case ERR_EMPTY:     return "EMPTY";
    case ERR_FULL:      return "FULL";
    case ERR_READONLY:  return "READONLY";
    case ERR_BUSY:      return "BUSY";
    case ERR_EXISTS:    return "EXISTS";
    case ERR_CONFLICT:  return "CONFLICT";
    case ERR_CORRUPT:   return "CORRUPT";
    case ERR_PARSE:     return "PARSE";
    case ERR_TYPE:      return "TYPE";
    default:            return "UNKNOWN";
    }
}
