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


typedef struct IntermediateResultHashKey
{
	char intermediate_result_id[NAMEDATALEN];
} IntermediateResultHashKey;

typedef struct IntermediateResultHashEntry
{
	IntermediateResultHashKey key;
	List *nodeList;
	bool alreadyAllNodes; /* TODO: not implemented, should help performance */
} IntermediateResultHashEntry;


static List * AppendAllAccessedNodesOfPlanToNodeList(List *workerNodeList,
													 DistributedPlan *distributedPlan);
static CustomScan * FetchCustomScanIfExists(Plan *plan);
static bool IsCitusCustomScan(Plan *plan);
static bool IsCitusPlan(Plan *plan);
static void RecordSubplanExecutionNodesForPlan(HTAB *intermediateResultsHash,
											   DistributedPlan *distributedPlan);

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

	HTAB *intermediateResultsHash = NULL;
	uint32 hashFlags = 0;
	HASHCTL info;


	/* If you're not a worker node, you should write local file to make sure
	 * you have the data too */
	bool writeLocalFile = GetLocalGroupId() == 0;

	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	/* TODO: move hash creation to some other function */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(IntermediateResultHashKey);
	info.entrysize = sizeof(IntermediateResultHashEntry);
	info.hash = string_hash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	intermediateResultsHash = hash_create("Intermediate results hash",
										  64, &info, hashFlags);

	/* TODO: move all this logic to somewhere else */
	/* calculate where each subPlan should go */
	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		CustomScan *customScan = FetchCustomScanIfExists(subPlan->plan->planTree);

		/* TODO: improve this check */
		if (customScan)
		{
			DistributedPlan *distributedPlanOfSubPlan = GetDistributedPlan(customScan);
			RecordSubplanExecutionNodesForPlan(intermediateResultsHash,
											   distributedPlanOfSubPlan);
		}
	}

	/*
	 *  Top level query also could have used sub plans, include them.
	 *
	 *  TODO: make this loop more integrated with the above.
	 */
	RecordSubplanExecutionNodesForPlan(intermediateResultsHash, distributedPlan);

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

		char *resultId = GenerateResultId(planId, subPlanId);

		IntermediateResultHashKey key;
		IntermediateResultHashEntry *entry = NULL;
		bool found = false;

		nodeList = NIL;

		strlcpy(key.intermediate_result_id, resultId, NAMEDATALEN);
		entry = hash_search(intermediateResultsHash, &key, HASH_ENTER, &found);

		if (found)
		{
			ListCell *lcInner = NULL;
			foreach(lcInner, entry->nodeList)
			{
				WorkerNode *w = GetWorkerNodeByNodeId(lfirst_int(lcInner));

				nodeList = lappend(nodeList, w);
				elog(DEBUG4, "%s is send to %s:%d", key.intermediate_result_id,
					 w->workerName, w->workerPort);
			}
		}
		else
		{
			/* Think more on this. Consider HAVING. */
			nodeList = NIL;
			elog(DEBUG4, "%s is not sent to any node, write to local only",
				 key.intermediate_result_id);
		}

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


static void
RecordSubplanExecutionNodesForPlan(HTAB *intermediateResultsHash,
								   DistributedPlan *distributedPlan)
{
	List *usedSubPlanList = distributedPlan->usedSubPlanNodeList;
	ListCell *usedSubPlanCell = NULL;

	foreach(usedSubPlanCell, usedSubPlanList)
	{
		Const *resultIdConst = (Const *) lfirst(usedSubPlanCell);
		Datum resultIdDatum = resultIdConst->constvalue;
		char *resultId = TextDatumGetCString(resultIdDatum);
		IntermediateResultHashKey key;
		IntermediateResultHashEntry *entry = NULL;
		bool found = false;

		strlcpy(key.intermediate_result_id, resultId, NAMEDATALEN);
		entry = hash_search(intermediateResultsHash, &key, HASH_ENTER, &found);

		if (!found)
		{
			entry->nodeList = NIL;
		}

		/* TODO: skip if all nodes are already in nodeList */
		entry->nodeList = AppendAllAccessedNodesOfPlanToNodeList(entry->nodeList,
																 distributedPlan);

		elog(DEBUG4, "subplan %s  is used in %lu", resultId,
			 distributedPlan->planId);
	}

	List *subPlanList = distributedPlan->subPlanList;
	ListCell *subPlanCell = NULL;
	foreach(subPlanCell, subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		CustomScan *customScan = FetchCustomScanIfExists(subPlan->plan->planTree);
		if (customScan)
		{
			DistributedPlan *distributedPlanOfSubPlan = GetDistributedPlan(customScan);
			RecordSubplanExecutionNodesForPlan(intermediateResultsHash,
											   distributedPlanOfSubPlan);
		}
	}
}


/* TODO: super inefficient, multiple recursions */
static CustomScan *
FetchCustomScanIfExists(Plan *plan)
{
	if (plan == NULL)
	{
		return NULL;
	}

	if (IsCitusCustomScan(plan))
	{
		return (CustomScan *) plan;
	}

	if (plan->lefttree != NULL && IsCitusPlan(plan->lefttree))
	{
		return FetchCustomScanIfExists(plan->lefttree);
	}

	if (plan->righttree != NULL && IsCitusPlan(plan->righttree))
	{
		return FetchCustomScanIfExists(plan->righttree);
	}

	return NULL;
}


/* todo: copied from another file */
static bool
IsCitusPlan(Plan *plan)
{
	if (plan == NULL)
	{
		return false;
	}

	if (IsCitusCustomScan(plan))
	{
		return true;
	}

	if (plan->lefttree != NULL && IsCitusPlan(plan->lefttree))
	{
		return true;
	}

	if (plan->righttree != NULL && IsCitusPlan(plan->righttree))
	{
		return true;
	}

	return false;
}


/*
 * IsCitusCustomScan returns whether Plan node is a CustomScan generated by Citus.
 */

/* todo: copied from another file */

static bool
IsCitusCustomScan(Plan *plan)
{
	CustomScan *customScan = NULL;
	Node *privateNode = NULL;

	if (plan == NULL)
	{
		return false;
	}

	if (!IsA(plan, CustomScan))
	{
		return false;
	}

	customScan = (CustomScan *) plan;
	if (list_length(customScan->custom_private) == 0)
	{
		return false;
	}

	privateNode = (Node *) linitial(customScan->custom_private);
	if (!CitusIsA(privateNode, DistributedPlan))
	{
		return false;
	}

	return true;
}


/*
 * TODO: this is super slow, improve the performance
 */
static List *
AppendAllAccessedNodesOfPlanToNodeList(List *workerNodeList,
									   DistributedPlan *distributedPlan)
{
	List *taskList = distributedPlan->workerJob->taskList;
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = lfirst(taskCell);
		ListCell *placementCell = NULL;
		foreach(placementCell, task->taskPlacementList)
		{
			ShardPlacement *placement = lfirst(placementCell);
			workerNodeList = list_append_unique_int(workerNodeList, placement->nodeId);
		}
	}

	return workerNodeList;
}
