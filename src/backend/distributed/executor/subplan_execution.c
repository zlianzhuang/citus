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
#include "utils/builtins.h"


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
	List *nodeList = NIL;
	List *topLevelUsedSubPlanNodes = distributedPlan->usedSubPlanNodeList;
	ListCell *lc = NULL;


	/* If you're not a worker node, you should write local file to make sure
	 * you have the data too */
	bool writeLocalFile = GetLocalGroupId() == 0;


	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}


	/* calculate where each subPlan should go */
	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		CustomScan *customScan = (CustomScan *) subPlan->plan->planTree;

		/* TODO: improve this check */
		if (IsA(customScan, CustomScan) &&
			strcmp(customScan->methods->CustomName, "Citus Adaptive") == 0)
		{
			DistributedPlan *innerDistributedPlan = GetDistributedPlan(customScan);
			List *usedSubPlanList = innerDistributedPlan->usedSubPlanNodeList;
			ListCell *lc = NULL;

			foreach(lc, usedSubPlanList)
			{
				Const *resultIdConst = (Const *) lfirst(lc);
				Datum resultIdDatum = resultIdConst->constvalue;
				char *resultId = TextDatumGetCString(resultIdDatum);

				elog(DEBUG4, "subplan %s  is used in %lu", resultId, innerDistributedPlan->planId);
			}
		}
	}

	/* top level query also could have used sub plans */
	foreach(lc, topLevelUsedSubPlanNodes)
	{
		Const *resultIdConst = (Const *) lfirst(lc);
		Datum resultIdDatum = resultIdConst->constvalue;
		char *resultId = TextDatumGetCString(resultIdDatum);

		elog(DEBUG4, "subplan %s  is used in top query plan %ld  %d", resultId, distributedPlan->planId);
	}

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results of subplans will be stored in a directory that is
	 * derived from the distributed transaction ID.
	 */
	BeginOrContinueCoordinatedTransaction();

	nodeList = ActiveReadableNodeList();

	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		DestReceiver *copyDest = NULL;
		ParamListInfo params = NULL;
		EState *estate = NULL;

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
