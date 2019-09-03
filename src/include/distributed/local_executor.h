/*-------------------------------------------------------------------------
 *
 * local_executor.h
 *	Functions and global variables to control local query execution.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCAL_EXECUTION_H
#define LOCAL_EXECUTION_H

#include "distributed/citus_custom_scan.h"

/* enabled with GUCs*/
extern bool EnableLocalExecution;
extern bool LogLocalCommands;

extern bool LocalExecutionHappened;

extern bool ExecuteLocalTasks(CitusScanState *node, List **remoteTaskLis);
extern bool ShouldExecuteTasksLocally(DistributedPlan *distributedPlan);
extern void ErrorIfLocalExecutionHappened(void);
extern void DisableLocalExecution(void);
extern bool AnyTaskAccessesRemoteNode(List *taskList);
extern bool TaskAccessLocalNode(Task *task);

#endif /* LOCAL_EXECUTION_H */
