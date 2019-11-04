/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "distributed/log_utils.h"


/*
 * In postgres, log can be configured differently for clients and servers.
 * It returns true if either of client or server log guc is configured to
 * log the given log level.
 */
bool
IsLoggableLevel(int logLevel)
{
	return log_min_messages <= logLevel || client_min_messages <= logLevel;
}
