/*-------------------------------------------------------------------------
 * collation.c
 *
 * This file contains functions to create, alter and drop policies on
 * distributed tables.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_manager.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "miscadmin.h"


static List * FilterNameListForDistributedCollations(List *objects, bool missing_ok,
													 List **addresses);
static void EnsureSequentialModeForCollationDDL(void);

/*
 * CreateCollationDDLCommand returns a "CREATE COLLATION..." SQL string for creating the
 * given collation if not exists.
 */
List *
CreateCollationDDLCommand(Oid collationId)
{
	List *result = NIL;
	char *schemaName = NULL;
	StringInfoData collationNameDef;
	const char *quotedCollationName = NULL;
	const char *providerString = NULL;
	HeapTuple heapTuple = NULL;
	Form_pg_collation collationForm = NULL;
	char collprovider;
	const char *collcollate;
	const char *collctype;
	const char *collname;
	Oid collnamespace;
	Oid collowner;
#if PG_VERSION_NUM >= 120000
	bool collisdeterministic;
#endif

	heapTuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collationId));
	if (!HeapTupleIsValid(heapTuple))
	{
		elog(ERROR, "citus cache lookup failed for collation %u", collationId);
	}

	collationForm = (Form_pg_collation) GETSTRUCT(heapTuple);
	collprovider = collationForm->collprovider;
	collcollate = NameStr(collationForm->collcollate);
	collctype = NameStr(collationForm->collctype);
	collnamespace = collationForm->collnamespace;
	collowner = collationForm->collowner;
	collname = NameStr(collationForm->collname);
#if PG_VERSION_NUM >= 120000
	collisdeterministic = collationForm->collisdeterministic;
#endif

	ReleaseSysCache(heapTuple);
	schemaName = get_namespace_name(collnamespace);
	quotedCollationName = quote_qualified_identifier(schemaName, collname);
	providerString =
		collprovider == COLLPROVIDER_DEFAULT ? "default" :
		collprovider == COLLPROVIDER_ICU ? "icu" :
		collprovider == COLLPROVIDER_LIBC ? "libc" : NULL;

	if (providerString == NULL)
	{
		elog(ERROR, "unknown collation provider: %c", collprovider);
	}

	initStringInfo(&collationNameDef);
	appendStringInfo(&collationNameDef,
					 "CREATE COLLATION %s (provider = '%s'",
					 quotedCollationName, providerString);

	if (strcmp(collcollate, collctype))
	{
		appendStringInfo(&collationNameDef,
						 ", locale = %s",
						 quote_literal_cstr(collcollate));
	}
	else
	{
		appendStringInfo(&collationNameDef,
						 ", lc_collate = %s, lc_ctype = %s",
						 quote_literal_cstr(collcollate),
						 quote_literal_cstr(collctype));
	}

#if PG_VERSION_NUM >= 120000
	if (!collisdeterministic)
	{
		appendStringInfoString(&collationNameDef, ", deterministic = false");
	}
#endif


	appendStringInfoChar(&collationNameDef, ')');

	result = lappend(result, collationNameDef.data);

	initStringInfo(&collationNameDef);
	appendStringInfo(&collationNameDef,
					 "ALTER COLLATION %s OWNER TO %s",
					 quotedCollationName,
					 quote_identifier(GetUserNameFromId(collowner, false)));

	result = lappend(result, collationNameDef.data);

	return result;
}


ObjectAddress
AlterCollationOwnerObjectAddress(AlterOwnerStmt *stmt)
{
	Relation relation;

	Assert(stmt->objectType == OBJECT_COLLATION);

	return get_object_address(stmt->objectType, stmt->object, &relation,
							  AccessExclusiveLock, false);
}


/*
 * FilterNameListForDistributedTypes takes a list of objects to delete, for Types this
 * will be a list of TypeName. This list is filtered against the types that are
 * distributed.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 */
static List *
FilterNameListForDistributedCollations(List *objects, bool missing_ok,
									   List **objectAddresses)
{
	ListCell *objectCell = NULL;
	List *result = NIL;

	*objectAddresses = NIL;

	foreach(objectCell, objects)
	{
		/* TODO ??? */
		List *collName = lfirst(objectCell);
		Oid collOid = LookupCollation(NULL, collName, 0);
		ObjectAddress collAddress = { 0 };

		if (!OidIsValid(collOid))
		{
			continue;
		}

		ObjectAddressSet(collAddress, CollationRelationId, collOid);
		if (IsObjectDistributed(&collAddress))
		{
			ObjectAddress *address = palloc0(sizeof(ObjectAddress));
			*address = collAddress;
			*objectAddresses = lappend(*objectAddresses, address);
			result = lappend(result, collName);
		}
	}
	return result;
}


List *
PlanDropCollationStmt(DropStmt *stmt)
{
	/*
	 * We swap the list of objects to remove during deparse so we need a reference back to
	 * the old list to put back
	 */
	List *oldTypes = stmt->objects;
	List *distributedTypes = NIL;
	const char *dropStmtSql = NULL;
	ListCell *addressCell = NULL;
	List *distributedTypeAddresses = NIL;
	List *commands = NIL;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	distributedTypes = FilterNameListForDistributedCollations(oldTypes, stmt->missing_ok,
															  &distributedTypeAddresses);
	if (list_length(distributedTypes) <= 0)
	{
		/* no distributed types to drop */
		return NIL;
	}

	/*
	 * managing collations can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * collations, so we block the call.
	 */
	EnsureCoordinator();

	/*
	 * remove the entries for the distributed objects on dropping
	 */
	foreach(addressCell, distributedTypeAddresses)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(addressCell);
		UnmarkObjectDistributed(address);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedTypes;
	dropStmtSql = DeparseTreeNode((Node *) stmt);
	stmt->objects = oldTypes;

	/* to prevent recursion with mx we disable ddl propagation */
	EnsureSequentialModeForCollationDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) dropStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PlanAlterCollationOwnerStmt is called for change of ownership of collations
 * before the ownership is changed on the local instance.
 *
 * If the type for which the owner is changed is distributed we execute the change on all
 * the workers to keep the type in sync across the cluster.
 */
List *
PlanAlterCollationOwnerStmt(AlterOwnerStmt *stmt, const char *queryString)
{
	const ObjectAddress *typeAddress = NULL;
	const char *sql = NULL;
	List *commands = NULL;

	Assert(stmt->objectType == OBJECT_TYPE);

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(typeAddress))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);
	sql = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForCollationDDL();
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) sql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * PlanRenameCollationStmt is called when the user is renaming the collation. The invocation happens
 * before the statement is applied locally.
 *
 * As the collation already exists we have access to the ObjectAddress for the collation, this is
 * used to check if the collation is distributed. If the collation is distributed the rename is
 * executed on all the workers to keep the collation in sync across the cluster.
 */
List *
PlanRenameCollationStmt(RenameStmt *stmt, const char *queryString)
{
	const char *renameStmtSql = NULL;
	const ObjectAddress *typeAddress = NULL;
	List *commands = NIL;

	typeAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(typeAddress))
	{
		return NIL;
	}

	/* fully qualify */
	QualifyTreeNode((Node *) stmt);

	/* deparse sql*/
	renameStmtSql = DeparseTreeNode((Node *) stmt);

	/* to prevent recursion with mx we disable ddl propagation */
	EnsureSequentialModeForCollationDDL();

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) renameStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * EnsureSequentialModeForCollationDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the begining.
 *
 * As collations are node scoped objects there exists only 1 instance of the collation used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the type the type needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForCollationDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify collation because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a collation, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Collation is created or altered. To make sure subsequent "
							   "commands see the collation correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}
