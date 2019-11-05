#include "postgres.h"

#include "nodes/parsenodes.h"
#include "utils/syscache.h"
#include "access/table.h"
#include "catalog/pg_authid.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "access/heapam.h"
#include "distributed/worker_transaction.h"
#include "distributed/commands/utility_hook.h"

static const char * CreatePasswordPropagationQuery(const char *rolename);


bool EnablePasswordPropagation = true;

/*
 * ProcessAlterRoleStmt actually creates the plan we need to execute for alter
 * role statement.
 */
List *
ProcessAlterRoleStmt(AlterRoleStmt *stmt, const char *queryString)
{
	ListCell *optionCell = NULL;
	List *commands = NIL;
	const char *passwordPropagationQuery = NULL;

	foreach(optionCell, stmt->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		if (strcasecmp(option->defname, "password") == 0 && EnablePasswordPropagation)
		{
			passwordPropagationQuery = CreatePasswordPropagationQuery(
				stmt->role->rolename);
			commands = lappend(commands, (void *) passwordPropagationQuery);
		}
	}
	if (list_length(commands) <= 0)
	{
		return NIL;
	}
	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * CreatePasswordPropagationQuery creates the query string for updating the
 * password of the roles in the worker nodes.
 * 
 * While creating the query we don't need to check if the user exists. If it
 * doesn't we the WHERE clause will simply filter out everything.
 */

static const char *
CreatePasswordPropagationQuery(const char *rolename)
{
	StringInfoData passwordPropagationQuery;
	Relation pgAuthId;
	HeapTuple tuple;
	bool isNull;
    TupleDesc pgAuthIdDescription;
    Datum passwordDatum;

	pgAuthId = heap_open(AuthIdRelationId, AccessShareLock);
	tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(rolename));

	pgAuthIdDescription = RelationGetDescr(pgAuthId);
	passwordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
									   pgAuthIdDescription, &isNull);

	heap_close(pgAuthId, AccessShareLock);
	ReleaseSysCache(tuple);

	initStringInfo(&passwordPropagationQuery);
	appendStringInfo(&passwordPropagationQuery,
					 "ALTER ROLE %s PASSWORD '%s'",
					 rolename,
					 TextDatumGetCString(passwordDatum));

	elog(WARNING, "%s", passwordPropagationQuery.data);

	return pstrdup(passwordPropagationQuery.data);
}
