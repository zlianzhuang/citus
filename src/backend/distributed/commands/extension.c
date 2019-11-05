/*-------------------------------------------------------------------------
 *
 * extension.c
 *    Commands for creating and altering extensions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_version.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "distributed/metadata/distobject.h"
#include "nodes/parsenodes.h"
#include "server/commands/extension.h"
#include "server/catalog/pg_extension_d.h"
#include "server/nodes/pg_list.h"

/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parsetree);


/*
 * IsCitusExtensionStmt returns whether a given utility is a CREATE or ALTER
 * EXTENSION statement which references the citus extension. This function
 * returns false for all other inputs.
 */
bool
IsCitusExtensionStmt(Node *parsetree)
{
	char *extensionName = "";

	if (IsA(parsetree, CreateExtensionStmt))
	{
		extensionName = ((CreateExtensionStmt *) parsetree)->extname;
	}
	else if (IsA(parsetree, AlterExtensionStmt))
	{
		extensionName = ((AlterExtensionStmt *) parsetree)->extname;
	}

	return (strcmp(extensionName, "citus") == 0);
}


/*
 * ErrorIfUnstableCreateOrAlterExtensionStmt compares CITUS_EXTENSIONVERSION
 * and version given CREATE/ALTER EXTENSION statement will create/update to. If
 * they are not same in major or minor version numbers, this function errors
 * out. It ignores the schema version.
 */
void
ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree)
{
	char *newExtensionVersion = ExtractNewExtensionVersion(parsetree);

	if (newExtensionVersion != NULL)
	{
		/*  explicit version provided in CREATE or ALTER EXTENSION UPDATE; verify */
		if (!MajorVersionsCompatible(newExtensionVersion, CITUS_EXTENSIONVERSION))
		{
			ereport(ERROR, (errmsg("specified version incompatible with loaded "
								   "Citus library"),
							errdetail("Loaded library requires %s, but %s was specified.",
									  CITUS_MAJORVERSION, newExtensionVersion),
							errhint("If a newer library is present, restart the database "
									"and try the command again.")));
		}
	}
	else
	{
		/*
		 * No version was specified, so PostgreSQL will use the default_version
		 * from the citus.control file.
		 */
		CheckAvailableVersion(ERROR);
	}
}


/*
 * ExtractNewExtensionVersion returns the new extension version specified by
 * a CREATE or ALTER EXTENSION statement. Other inputs are not permitted. This
 * function returns NULL for statements with no explicit version specified.
 */
static char *
ExtractNewExtensionVersion(Node *parsetree)
{
	char *newVersion = NULL;
	List *optionsList = NIL;
	ListCell *optionsCell = NULL;

	if (IsA(parsetree, CreateExtensionStmt))
	{
		optionsList = ((CreateExtensionStmt *) parsetree)->options;
	}
	else if (IsA(parsetree, AlterExtensionStmt))
	{
		optionsList = ((AlterExtensionStmt *) parsetree)->options;
	}
	else
	{
		/* input must be one of the two above types */
		Assert(false);
	}

	foreach(optionsCell, optionsList)
	{
		DefElem *defElement = (DefElem *) lfirst(optionsCell);
		if (strncmp(defElement->defname, "new_version", NAMEDATALEN) == 0)
		{
			newVersion = strVal(defElement->arg);
			break;
		}
	}

	return newVersion;
}

static bool
ShouldPropagateExtensionCreate()
{
	// TODO: @onurctirtir implement this function
	
	return true;
}

// TODO: @onurctirtir
// For now, I am not sure if we need to break CreateExtension prop logic into Plan and Process phases
List *
PlanCreateExtensionStmt(CreateExtensionStmt *stmt, const char *queryString)
{
	List *commands = NIL;

	if (!ShouldPropagateExtensionCreate())
	{
		return NIL;
	}

	EnsureCoordinator();

	//QualifyTreeNode((Node *) stmt);

	// TODO: @onurctirtir, are they needed ??
	//createExtensionStmtSql = DeparseCreateEnumStmt(stmt);
	//createExtensionStmtSql = WrapCreateOrReplace(createExtensionStmtSql);
	//EnsureSequentialModeForTypeDDL();

	/* TODO: @onurctirtir, to prevent recursion with mx we disable ddl propagation, should we ?? */
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) queryString,
						  ENABLE_DDL_PROPAGATION);

	elog(WARNING, queryString);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}

void
ProcessCreateExtensionStmt(CreateExtensionStmt *stmt, const char *queryString)
{
	// TODO: @onurctirtir implement me
	const ObjectAddress *extensionAddress = NULL;
	extensionAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	
	if (!ShouldPropagateExtensionCreate()) {
		return;
	}

	// TODO: @onurctirtir hope it can find the dependencies of the extension
	EnsureDependenciesExistsOnAllNodes(extensionAddress);

	MarkObjectDistributed(extensionAddress);
}

List *
ProcessDropExtensionStmt(DropStmt *stmt, const char *queryString)
{
	// TODO: @onurctirtir implement me
	List *commands = NIL;
	bool missingOk = true;
	ListCell *dropExtensionEntry = NULL;

	// iterate each extension in drop stmt 
	foreach(dropExtensionEntry, stmt->objects)
	{
		char *extensionName = strVal(lfirst(dropExtensionEntry));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		// TODO: this logic could be moved to GetObjectAddress function but a bit tricky
		Oid extensionoid = get_extension_oid(extensionName, missingOk);
		
		ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

		elog(DEBUG1, extensionName);

		if (extensionoid == InvalidOid || !IsObjectDistributed(address))
		{
			continue;
		}

		UnmarkObjectDistributed(address);

		commands = lappend(commands, (void *) queryString);

		elog(DEBUG1, queryString);
	}

	return NodeDDLTaskList(ALL_WORKERS, commands);
}
