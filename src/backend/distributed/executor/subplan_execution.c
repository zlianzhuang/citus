/*-------------------------------------------------------------------------
 *
 * subplan_execution.c
 *
 * Functions for execution subplans prior to distributed table execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/intermediate_results.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "executor/executor.h"

static List * NodeList(List *nodeIdList);

int MaxIntermediateResult = 1048576; /* maximum size in KB the intermediate result can grow to */
/* when this is true, we enforce intermediate result size limit in all executors */
int SubPlanLevel = 0;


/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by sequentially executing each plan from the top.
 */
void
ExecuteSubPlans(DistributedPlan *distributedPlan)
{
	uint64 planId = distributedPlan->planId;
	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;

	/* If you're not a worker node, you should write local file to make sure
	 * you have the data too */
	bool writeLocalFile = GetLocalGroupId() == 0;


	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results of subplans will be stored in a directory that is
	 * derived from the distributed transaction ID.
	 */
	BeginOrContinueCoordinatedTransaction();

	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		DestReceiver *copyDest = NULL;
		ParamListInfo params = NULL;
		EState *estate = NULL;
		List *nodeIdList = subPlan->workerNodeList;
		List *nodeList = NodeList(nodeIdList);

		char *resultId = GenerateResultId(planId, subPlanId);

		SubPlanLevel++;
		estate = CreateExecutorState();
		copyDest = (DestReceiver *) CreateRemoteFileDestReceiver(resultId, estate,
																 nodeList,
																 writeLocalFile);

		ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

		SubPlanLevel--;
		FreeExecutorState(estate);
	}
}


static List *
NodeList(List *nodeIdList)
{
	List *allWorkers = ReadWorkerNodes(false);

	ListCell *lc = NULL;
	List *l = NIL;
	foreach(lc, nodeIdList)
	{
		int nodeId = lfirst_int(lc);

		ListCell *lc2 = NULL;
		foreach(lc2, allWorkers)
		{
			WorkerNode *w = lfirst(lc2);

			if (w->nodeId == nodeId)
			l = lappend(l , w);
		}

	}
return l;
}
