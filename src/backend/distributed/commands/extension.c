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
#include "nodes/makefuncs.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "distributed/metadata/distobject.h"
#include "server/access/genam.h"
#include "server/commands/extension.h"
#include "server/catalog/pg_extension_d.h"
#include "server/nodes/parsenodes.h"
#include "server/nodes/pg_list.h"
#include "server/utils/fmgroids.h"
#include "server/postgres.h"
#include "utils/lsyscache.h"


/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parsetree);
static bool ShouldPropagateExtensionCreate();
static void QualifyCreateExtensionStmt(CreateExtensionStmt *stmt);
static Oid PgAvailExtVerOid(void);
static void GetLatestVersion(const char *extensionName);
static bool ShouldPropagateExtensionCreate(void);

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


List *
PlanCreateExtensionStmt(CreateExtensionStmt *stmt, const char *queryString)
{
	// TODO: @onurctirtir implement me

	// TODO: @onurctirtir which lock should I take ??

	List *commands = NIL;
	const char *createExtensionStmtSql = NULL;

	if (!ShouldPropagateExtensionCreate())
	{
		return NIL;
	}

	EnsureCoordinator();

	// TODO: @onurctirtir This function should be renamed
	QualifyCreateExtensionStmt(stmt);

	// TODO: @onurctirtir, is this needed ??
	createExtensionStmtSql = DeparseTreeNode((Node*) stmt);
	// TODO: @onurctirtir not sure about below call ?
	//EnsureSequentialModeForTypeDDL();

	/* TODO: @onurctirtir, to prevent recursion with mx we disable ddl propagation, should we ?? */
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) createExtensionStmtSql,
						  ENABLE_DDL_PROPAGATION);

	// DEBUG
	GetLatestVersion(NULL);
	elog(DEBUG1, queryString);
	elog(DEBUG1, createExtensionStmtSql);

	return NULL; 
	// TODO: for debug return NodeDDLTaskList(ALL_WORKERS, commands);
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

static void
QualifyCreateExtensionStmt(CreateExtensionStmt *stmt)
{
	//TODO: @onurctirtir implement this function

	// we may need to qualiy DefElem list
	List *optionsList = stmt->options;
	ListCell *optionsCell = NULL;
	
	bool newVersionSpecified  = false;
	bool oldVersionSpecified  = false;
	bool schemaSpecified  = false;

	// check if the above ones are specified in createExtension statement
	foreach(optionsCell, optionsList)
	{
		// TODO:: @onurctirtir
		// check if lookup functions alredy exist in postgres or citus codebase
		DefElem *defElement = (DefElem *) lfirst(optionsCell);

		if (strncmp(defElement->defname, "new_version", NAMEDATALEN) == 0)
		{
			newVersionSpecified = true;
		}
		else if (strncmp(defElement->defname, "old_version", NAMEDATALEN) == 0)
		{
			oldVersionSpecified = true;
		}
		else if (strncmp(defElement->defname, "schema", NAMEDATALEN) == 0)
		{
			schemaSpecified = true;
		}
		else if (strncmp(defElement->defname, "cascade", NAMEDATALEN) == 0)
		{
			continue;
		}
		else
		{
			// TODO: @onurctirtir we do not expect other than the above ones, remove this condition before merge
			Assert(false);
		}	
	}

	// manipulate stmt so the missing specifiers just found above are also included in stmt
	// TODO: where to get version num
	if (!newVersionSpecified)
	{
		DefElem *newDefElement = makeDefElem("new_version", (Node*)(makeString("version_num")), -1);
		optionsList = lappend(optionsList, newDefElement);
	}
	// TODO: where to get version num
	if (!oldVersionSpecified)
	{
		DefElem *newDefElement = makeDefElem("old_version", (Node*)(makeString("version_num")), -1);
		optionsList = lappend(optionsList, newDefElement);

	}
	// TODO: where to schema name
	if (!schemaSpecified)
	{
		DefElem *newDefElement = makeDefElem("schema", (Node*)(makeString("schema_name")), -1);
		optionsList = lappend(optionsList, newDefElement);
	}	
}

static bool
ShouldPropagateExtensionCreate(void)
{
	if (!EnableDependencyCreation)
	{
		/*
		 * if we disabled object propagation, then we should not propagate anything
		 */
		return false;
	}

	return true;
}

/*
 * Fetch latest available version of an extension from pg_catalog.pg_available_extension_versions
 */ 
static void
GetLatestVersion(const char *extensionName)
{
	Oid pgAvailExtVerOid = InvalidOid;

	// scanKeyInıt params
	AttrNumber attributeNumber = 1;
	StrategyNumber strategyNumber = BTEqualStrategyNumber;
	RegProcedure regProcedure =  F_TEXTEQ;
	Datum extensionObjectDatum = (Datum) 0; 
	ScanKeyData scanKey[1];

	// systable_beginscan params
	Relation pgAvailExtensionVersions;
	Oid indexOid = InvalidOid; // index id of TODO: @onurctirtir
	bool indexOK = false;
	Snapshot snapshot = NULL;
	int nkeys = 1;

	// resulting objects
	SysScanDesc scanDesriptor = NULL;
	HeapTuple resultTuple = NULL;
	
	bool result = false;

	// get oid of pg_catalog.pg_available_extension_versions
	pgAvailExtVerOid = PgAvailExtVerOid();
	
	// get datum representing pg_catalog.pg_available_extension_versions
	extensionObjectDatum = ObjectIdGetDatum(pgAvailExtVerOid);

	// TODO: @onurctirtir which lock should I take ??
	pgAvailExtensionVersions = heap_open(pgAvailExtVerOid, AccessShareLock);

	// TODO: @onurctirtir add additional keys to fetch only the latest version
	ScanKeyInit(&scanKey[0], attributeNumber, strategyNumber, regProcedure, extensionObjectDatum);
	
	// XXX: gives segmentation after this call 
	scanDesriptor = systable_beginscan(
		pgAvailExtensionVersions, indexOid, indexOK, snapshot, nkeys, scanKey);

	resultTuple = systable_getnext(scanDesriptor);
	if (HeapTupleIsValid(resultTuple))
	{
		result = true;
	}

	systable_endscan(scanDesriptor);
	relation_close(pgAvailExtensionVersions, AccessShareLock);

	//return result;
}

//TODO: @onurctirtir rename and move this function ?
/* 
 * Return oid of pg_catalog.pg_available_extension_versions 
 */
static Oid
PgAvailExtVerOid(void)
{
	bool pgCatalogMissingOk = true;
	Oid pgCatalogOid = InvalidOid;
	Oid pgAvailExtVerOid = InvalidOid;

	//TODO: @onurctirtir assertions before merger

	pgCatalogOid = get_namespace_oid("pg_catalog", pgCatalogMissingOk);
	Assert(pgCatalogOid != InvalidOid);
	pgAvailExtVerOid = get_relname_relid("pg_available_extension_versions", pgCatalogOid);
	Assert(pgAvailExtVerOid != InvalidOid);

	return pgAvailExtVerOid;
}

