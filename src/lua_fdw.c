/*-------------------------------------------------------------------------
 *
 * Lua Foreign Data Wrapper for PostgreSQL
 *
 * Copyright (c) 2016 Sean Pringle (lua_fdw)
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Andrew Dunstan <andrew@dunslane.net> (blackhole_fdw)
 * Author: Sean Pringle <sean.pringle@gmail.com> (lua_fdw)
 *
 *-------------------------------------------------------------------------
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "postgres.h"

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/explain.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

/*
 * SQL functions
 */
extern Datum lua_fdw_handler(PG_FUNCTION_ARGS);
extern Datum lua_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(lua_fdw_handler);
PG_FUNCTION_INFO_V1(lua_fdw_validator);

void
_PG_init(
	void
);

void
_PG_fini(
	void
);

const char*
get_pg_type_str (
	Oid id
);

const char*
lua_popstring (
	lua_State *lua
);

double
lua_popnumber (
	lua_State *lua
);

lua_State*
lua_start (
	const char *script,
	const char *inject
);

void
lua_stop (
	lua_State *lua
);

int
lua_ereport (
	lua_State *lua
);

static bool
is_valid_option (
	const char *option,
	Oid context
);

/* callback functions */

static void
luaGetForeignRelSize(
	PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid
);

static void
luaGetForeignPaths(
	PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid
);

static ForeignScan
*luaGetForeignPlan(
	PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid,
	ForeignPath *best_path,
	List *tlist,
	List *scan_clauses,
	Plan *outer_plan
);

static void
luaBeginForeignScan(
	ForeignScanState *node,
	int eflags
);

static TupleTableSlot
*luaIterateForeignScan(
	ForeignScanState *node
);

static void
luaReScanForeignScan(
	ForeignScanState *node
);

static void
luaEndForeignScan(
	ForeignScanState *node
);

static void
luaAddForeignUpdateTargets(
	Query *parsetree,
	RangeTblEntry *target_rte,
	Relation target_relation
);

static List
*luaPlanForeignModify(
	PlannerInfo *root,
	ModifyTable *plan,
	Index resultRelation,
	int subplan_index
);

static void
luaBeginForeignModify(
	ModifyTableState *mtstate,
	ResultRelInfo *rinfo,
	List *fdw_private,
	int subplan_index,
	int eflags
);

static TupleTableSlot
*luaExecForeignInsert(
	EState *estate,
	ResultRelInfo *rinfo,
	TupleTableSlot *slot,
	TupleTableSlot *planSlot
);

static TupleTableSlot
*luaExecForeignUpdate(
	EState *estate,
	ResultRelInfo *rinfo,
	TupleTableSlot *slot,
	TupleTableSlot *planSlot
);

static TupleTableSlot
*luaExecForeignDelete(
	EState *estate,
	ResultRelInfo *rinfo,
	TupleTableSlot *slot,
	TupleTableSlot *planSlot
);

static void
luaEndForeignModify(
	EState *estate,
	ResultRelInfo *rinfo
);

static int
luaIsForeignRelUpdatable(
	Relation rel
);

static void
luaExplainForeignScan(
	ForeignScanState *node,
	struct ExplainState * es
);

static void
luaExplainForeignModify(
	ModifyTableState *mtstate,
	ResultRelInfo *rinfo,
	List *fdw_private,
	int subplan_index,
	struct ExplainState * es
);

static bool
luaAnalyzeForeignTable(
	Relation relation,
	AcquireSampleRowsFunc *func,
	BlockNumber *totalpages
);

static void
luaGetForeignJoinPaths(
	PlannerInfo *root,
	RelOptInfo *joinrel,
	RelOptInfo *outerrel,
	RelOptInfo *innerrel,
	JoinType jointype,
	JoinPathExtraData *extra
);

static RowMarkType
luaGetForeignRowMarkType(
	RangeTblEntry *rte,
	LockClauseStrength strength
);

static HeapTuple
luaRefetchForeignRow(
	EState *estate,
	ExecRowMark *erm,
	Datum rowid,
	bool *updated
);

static List
*luaImportForeignSchema(
	ImportForeignSchemaStmt *stmt,
	Oid serverOid
);

/*
 * structures used by the FDW
 *
 * These next structures are not actually used by lua,but something like
 * them will be needed by anything more complicated that does actual work.
 */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct luaFdwOption
{
	const char *optname;
	Oid optcontext; /* Oid of catalog in which option may appear */
};

/*
 * The plan state is set up in luaGetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in luaGetForeignPaths.
 */
typedef struct
{
	lua_State *lua;
} LuaFdwPlanState;

/*
 * The scan state is for maintaining state for a scan, eiher for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in luaBeginForeignScan and stashed in node->fdw_state
 * and subsequently used in luaIterateForeignScan,
 * luaEndForeignScan and luaReScanForeignScan.
 */
typedef struct
{
	lua_State *lua;
} LuaFdwScanState;

/*
 * The modify state is for maintaining state of modify operations.
 *
 * It is set up in luaBeginForeignModify and stashed in
 * rinfo->ri_FdwState and subsequently used in luaExecForeignInsert,
 * luaExecForeignUpdate, luaExecForeignDelete and
 * luaEndForeignModify.
 */
typedef struct
{
	lua_State *lua;
} LuaFdwModifyState;

/*
 * Valid options for lua_fdw.
 */
static const struct luaFdwOption valid_options[] = {
	{"script", ForeignTableRelationId},
	{"inject", ForeignTableRelationId},

//	/* Format options */
//	/* oids option is not supported */
//	{"format", ForeignTableRelationId},
//	{"header", ForeignTableRelationId},
//	{"delimiter", ForeignTableRelationId},
//	{"quote", ForeignTableRelationId},
//	{"escape", ForeignTableRelationId},
//	{"null", ForeignTableRelationId},
//	{"encoding", ForeignTableRelationId},
//	{"force_not_null", AttributeRelationId},
//	{"force_null", AttributeRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

const char*
get_pg_type_str (Oid id)
{
	switch (id)
	{
		default:
			return "text";
	}
}

const char*
lua_popstring (lua_State *lua)
{
	const char *str = lua_tostring(lua, -1);
	lua_pop(lua, 1);
	return str;
}

double
lua_popnumber (lua_State *lua)
{
	double n = lua_tonumber(lua, -1);
	lua_pop(lua, 1);
	return n;
}

lua_State*
lua_start (const char *script, const char *inject)
{
	lua_State *lua;

	lua = luaL_newstate();
	luaL_openlibs(lua);

	//elog(WARNING, "function %s %lx", __func__, (uint64_t)lua);

	lua_createtable(lua, 0, 0);

	lua_pushstring(lua, "ereport");
	lua_pushcfunction(lua, lua_ereport);
	lua_settable(lua, -3);

	lua_pushstring(lua, "DEBUG5");
	lua_pushnumber(lua, DEBUG5);
	lua_settable(lua, -3);

	lua_pushstring(lua, "DEBUG4");
	lua_pushnumber(lua, DEBUG4);
	lua_settable(lua, -3);

	lua_pushstring(lua, "DEBUG3");
	lua_pushnumber(lua, DEBUG3);
	lua_settable(lua, -3);

	lua_pushstring(lua, "DEBUG2");
	lua_pushnumber(lua, DEBUG2);
	lua_settable(lua, -3);

	lua_pushstring(lua, "DEBUG1");
	lua_pushnumber(lua, DEBUG1);
	lua_settable(lua, -3);

	lua_pushstring(lua, "INFO");
	lua_pushnumber(lua, INFO);
	lua_settable(lua, -3);

	lua_pushstring(lua, "NOTICE");
	lua_pushnumber(lua, NOTICE);
	lua_settable(lua, -3);

	lua_pushstring(lua, "WARNING");
	lua_pushnumber(lua, WARNING);
	lua_settable(lua, -3);

	lua_pushstring(lua, "ERROR");
	lua_pushnumber(lua, ERROR);
	lua_settable(lua, -3);

	lua_pushstring(lua, "LOG");
	lua_pushnumber(lua, LOG);
	lua_settable(lua, -3);

	lua_pushstring(lua, "FATAL");
	lua_pushnumber(lua, FATAL);
	lua_settable(lua, -3);

	lua_pushstring(lua, "PANIC");
	lua_pushnumber(lua, PANIC);
	lua_settable(lua, -3);

	lua_setglobal(lua, "fdw");

	if ((script && luaL_dofile(lua, script) != 0) || (inject && luaL_dostring(lua, inject) != 0))
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(lua, -1))));

	return lua;
}

void
lua_stop (lua_State *lua)
{
	//elog(WARNING, "function %s %lx", __func__, (uint64_t)lua);
	lua_close(lua);
}

int
lua_ereport (lua_State *lua)
{
	const char *message = lua_popstring(lua);
	int level = lua_popnumber(lua);
	ereport(level, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw: %s", message)));
	return 0;
}

void
_PG_init ()
{
	//elog(WARNING, "function %s", __func__);
}

void
_PG_fini ()
{
	//elog(WARNING, "function %s", __func__);
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option (const char *option, Oid context)
{
	const struct luaFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}


Datum
lua_fdw_handler (PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	//elog(WARNING, "function %s", __func__);

	/*
	 * assign the handlers for the FDW
	 *
	 * This function might be called a number of times. In particular, it is
	 * likely to be called for each INSERT statement. For an explanation, see
	 * core postgres file src/optimizer/plan/createplan.c where it calls
	 * GetFdwRoutineByRelId(().
	 */

	/* Required by notations: S=SELECT I=INSERT U=UPDATE D=DELETE */

	/* these are required */
	fdwroutine->GetForeignRelSize = luaGetForeignRelSize; /* S U D */
	fdwroutine->GetForeignPaths = luaGetForeignPaths;		/* S U D */
	fdwroutine->GetForeignPlan = luaGetForeignPlan;		/* S U D */
	fdwroutine->BeginForeignScan = luaBeginForeignScan;	/* S U D */
	fdwroutine->IterateForeignScan = luaIterateForeignScan;		/* S */
	fdwroutine->ReScanForeignScan = luaReScanForeignScan; /* S */
	fdwroutine->EndForeignScan = luaEndForeignScan;		/* S U D */

	/* remainder are optional - use NULL if not required */
	/* support for insert / update / delete */
	fdwroutine->IsForeignRelUpdatable = luaIsForeignRelUpdatable;
	fdwroutine->AddForeignUpdateTargets = luaAddForeignUpdateTargets;		/* U D */
	fdwroutine->PlanForeignModify = luaPlanForeignModify; /* I U D */
	fdwroutine->BeginForeignModify = luaBeginForeignModify;		/* I U D */
	fdwroutine->ExecForeignInsert = luaExecForeignInsert; /* I */
	fdwroutine->ExecForeignUpdate = luaExecForeignUpdate; /* U */
	fdwroutine->ExecForeignDelete = luaExecForeignDelete; /* D */
	fdwroutine->EndForeignModify = luaEndForeignModify;	/* I U D */

	/* support for EXPLAIN */
	fdwroutine->ExplainForeignScan = luaExplainForeignScan;		/* EXPLAIN S U D */
	fdwroutine->ExplainForeignModify = luaExplainForeignModify;	/* EXPLAIN I U D */

	/* support for ANALYSE */
	fdwroutine->AnalyzeForeignTable = luaAnalyzeForeignTable;		/* ANALYZE only */

	/* Support functions for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = luaImportForeignSchema;

	/* Support for scanning foreign joins */
	fdwroutine->GetForeignJoinPaths = luaGetForeignJoinPaths;

	/* Support for locking foreign rows */
	fdwroutine->GetForeignRowMarkType = luaGetForeignRowMarkType;
	fdwroutine->RefetchForeignRow = luaRefetchForeignRow;

	PG_RETURN_POINTER(fdwroutine);
}

Datum
lua_fdw_validator (PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell *cell;

	//elog(WARNING, "function %s", __func__);

	/*
	 * Check that only options supported by lua_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			const struct luaFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->optname);
			}

			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("invalid option \"%s\"", def->defname),
					buf.len > 0
						? errhint("Valid options in this context are: %s", buf.data)
						: errhint("There are no valid options in this context.")
				)
			);
		}
	}

	PG_RETURN_VOID();
}

static void
luaGetForeignRelSize (PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	LuaFdwPlanState *plan_state;
	ForeignTable *table;
	ListCell *cell;
	Relation rel;
	TupleDesc desc;
	const char *script = NULL;
	const char *inject = NULL;
	lua_State *lua;
	int i;

	//elog(WARNING, "function %s", __func__);

	/*
	 * Obtain relation size estimates for a foreign table. This is called at
	 * the beginning of planning for a query that scans a foreign table. root
	 * is the planner's global information about the query; baserel is the
	 * planner's information about this table; and foreigntableid is the
	 * pg_class OID of the foreign table. (foreigntableid could be obtained
	 * from the planner data structures, but it's passed explicitly to save
	 * effort.)
	 *
	 * This function should update baserel->rows to be the expected number of
	 * rows returned by the table scan, after accounting for the filtering
	 * done by the restriction quals. The initial value of baserel->rows is
	 * just a constant default estimate, which should be replaced if at all
	 * possible. The function may also choose to update baserel->width if it
	 * can compute a better estimate of the average result row width.
	 */

	rel = heap_open(foreigntableid, AccessShareLock);
	desc = RelationGetDescr(rel);

	table = GetForeignTable(foreigntableid);

	foreach(cell, table->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "script") == 0)
			script = defGetString(def);

		if (strcmp(def->defname, "inject") == 0)
			inject = defGetString(def);
	}

	plan_state = palloc0(sizeof(LuaFdwPlanState));
	baserel->fdw_private = (void *) plan_state;
	baserel->rows = 0;

	/* initialize required state in plan_state */

	lua = plan_state->lua = lua_start(script, inject);

	lua_getglobal(lua, "fdw");

	lua_pushstring(lua, "table");
	lua_pushstring(lua, get_rel_name(foreigntableid));
	lua_settable(lua, -3); // table

	lua_pushstring(lua, "columns");
	lua_createtable(lua, 0, 0);
	for (i = 0; i < desc->natts; i++)
	{
		lua_pushstring(lua, desc->attrs[i]->attname.data);
		switch (desc->attrs[i]->atttypid)
		{
			case INT4OID:
			case INT8OID:
				lua_pushstring(lua, "integer");
				break;
			default:
				lua_pushstring(lua, "text");
		}
		lua_settable(lua, -3);
	}
	lua_settable(lua, -3); // columns

	lua_pop(lua, 1); // fdw

	lua_getglobal(lua, "EstimateRowCount");

	if (lua_isfunction(lua, -1))
	{
		if (lua_pcall(lua, 0, 1, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(lua, -1))));
		else
		{
			if (lua_isnumber(lua, -1))
				baserel->rows = lua_tonumber(lua, -1);

			lua_pop(lua, 1);
		}
	}

	lua_getglobal(lua, "EstimateRowWidth");

	if (lua_isfunction(lua, -1))
	{
		if (lua_pcall(lua, 0, 1, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(lua, -1))));
		else
		{
			if (lua_isnumber(lua, -1))
				baserel->width = lua_tonumber(lua, -1);

			lua_pop(lua, 1);
		}
	}

	heap_close(rel, AccessShareLock);
}

static void
luaGetForeignPaths (PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	LuaFdwPlanState *plan_state;
	Cost startup_cost, total_cost;

	/*
	 * Create possible access paths for a scan on a foreign table. This is
	 * called during query planning. The parameters are the same as for
	 * GetForeignRelSize, which has already been called.
	 *
	 * This function must generate at least one access path (ForeignPath node)
	 * for a scan on the foreign table and must call add_path to add each such
	 * path to baserel->pathlist. It's recommended to use
	 * create_foreignscan_path to build the ForeignPath nodes. The function
	 * can generate multiple access paths, e.g., a path which has valid
	 * pathkeys to represent a pre-sorted result. Each access path must
	 * contain cost estimates, and can contain any FDW-private information
	 * that is needed to identify the specific scan method intended.
	 */

	//elog(WARNING, "function %s", __func__);

	plan_state = baserel->fdw_private;

	startup_cost = 0;
	total_cost = startup_cost + baserel->rows;

	lua_getglobal(plan_state->lua, "EstimateStartupCost");

	if (lua_isfunction(plan_state->lua, -1))
	{
		if (lua_pcall(plan_state->lua, 0, 2, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(plan_state->lua, -1))));
		else
		{
			if (lua_isnumber(plan_state->lua, -1))
				startup_cost = lua_tonumber(plan_state->lua, -1);

			lua_pop(plan_state->lua, 1);
		}
	}
	else
	{
		lua_pop(plan_state->lua, 1);
	}

	lua_getglobal(plan_state->lua, "EstimateTotalCost");

	if (lua_isfunction(plan_state->lua, -1))
	{
		if (lua_pcall(plan_state->lua, 0, 2, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(plan_state->lua, -1))));
		else
		{
			if (lua_isnumber(plan_state->lua, -1))
				total_cost = lua_tonumber(plan_state->lua, -1);

			lua_pop(plan_state->lua, 1);
		}
	}
	else
	{
		lua_pop(plan_state->lua, 1);
	}

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
#if (PG_VERSION_NUM >= 90600)
									 NULL,      /* default pathtarget */
#endif
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
#if (PG_VERSION_NUM >= 90500)
									 NULL,      /* no extra plan */
#endif
									 NIL));		/* no fdw_private data */
}

static ForeignScan *
luaGetForeignPlan (
	PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid,
	ForeignPath *best_path,
	List *tlist,
	List *scan_clauses,
	Plan *outer_plan
){
	LuaFdwPlanState *plan_state;
	lua_State *lua;
	ListCell *lc;
	RestrictInfo *rinfo;
	OpExpr *op;
	Node *arg1, *arg2;
	Relation rel;
	TupleDesc desc;
	int attno;
	int clause;
	int is_eq, is_like, is_lt, is_gt;
	Oid id;
	char scratch[50];

	/*
	 * Create a ForeignScan plan node from the selected foreign access path.
	 * This is called at the end of query planning. The parameters are as for
	 * GetForeignRelSize, plus the selected ForeignPath (previously produced
	 * by GetForeignPaths), the target list to be emitted by the plan node,
	 * and the restriction clauses to be enforced by the plan node.
	 *
	 * This function must create and return a ForeignScan plan node; it's
	 * recommended to use make_foreignscan to build the ForeignScan node.
	 *
	 */

	Index scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check. So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */

	//elog(WARNING, "function %s", __func__);

	plan_state = baserel->fdw_private;
	lua = plan_state->lua;
	//pfree(plan_state);
	baserel->fdw_private = NULL;

	rel = heap_open(foreigntableid, AccessShareLock);
	desc = RelationGetDescr(rel);

	lua_getglobal(lua, "fdw");
	lua_pushstring(lua, "clauses");
	lua_createtable(lua, 0, 0);
	clause = 1;

	foreach(lc, scan_clauses)
	{
		rinfo = (RestrictInfo *) lfirst(lc);
		Assert(IsA(rinfo, RestrictInfo));

		if (rinfo->type == T_RestrictInfo && !rinfo->orclause && rinfo->clause->type == T_OpExpr)
		{
			op = (OpExpr*) rinfo->clause;
			elog(WARNING, "op: %d", op->opno);

			if (list_length(op->args) == 2)
			{
				arg1 = list_nth(op->args, 0);
				arg2 = list_nth(op->args, 1);
				elog(WARNING, "arg1: %d arg2: %d", arg1->type, arg2->type);

				if (arg1->type == T_Var && arg2->type == T_Const)
				{
					id = ((Const*)arg2)->consttype;
					attno = ((Var*)arg1)->varattno-1;
					elog(WARNING, "id: %d", id);

					is_eq =
						op->opno == 15 // int48eq
						|| op->opno == Int4EqualOperator
						|| op->opno == TextEqualOperator
						|| op->opno == 2060 // timestamp_eq
					;

					is_like =
						op->opno == OID_TEXT_LIKE_OP
					;

					is_gt = 0;

					is_lt =
						op->opno == Int4LessOperator
						|| op->opno == Int8LessOperator
						|| op->opno == 2062 // timestamp_lt
					;

					if ((is_eq || is_like) && (id == TEXTOID || id == INT4OID || id == INT8OID || id == TIMESTAMPOID))
					{
						lua_pushnumber(lua, clause++);
						lua_createtable(lua, 0, 0);

						lua_pushstring(lua, "column");
						lua_pushstring(lua, desc->attrs[attno]->attname.data);
						lua_settable(lua, -3);

						lua_pushstring(lua, "operator");
						lua_pushstring(lua,
							is_like ? "like":
							is_lt   ? "lt":
							is_gt   ? "gt":
							"eq"
						);
						lua_settable(lua, -3);

						lua_pushstring(lua, "type");

						switch (id)
						{
							case INT4OID:
							case INT8OID:
								lua_pushstring(lua, "integer");
								break;
							case TIMESTAMPOID:
								lua_pushstring(lua, "timestamp");
								break;
							default:
								lua_pushstring(lua, "text");
						}
						lua_settable(lua, -3);

						lua_pushstring(lua, "constant");

						switch (id)
						{
							case INT4OID:
								snprintf(scratch, sizeof(scratch), "%d", DatumGetInt32(((Const*)arg2)->constvalue));
								lua_pushstring(lua, scratch);
								break;
							case INT8OID:
								snprintf(scratch, sizeof(scratch), "%ld", DatumGetInt64(((Const*)arg2)->constvalue));
								lua_pushstring(lua, scratch);
								break;
							case TIMESTAMPOID:
								lua_pushstring(lua, DatumGetCString(DirectFunctionCall1(timestamp_out, ((Const*)arg2)->constvalue)));
								break;
							default:
								lua_pushstring(lua, DatumGetCString(DirectFunctionCall1(textout, ((Const*)arg2)->constvalue)));
						}

						lua_settable(lua, -3);
						lua_settable(lua, -3); // #clause
					}
				}
			}
		}
	}

	lua_settable(lua, -3); // clauses
	lua_pop(lua, 1);

	heap_close(rel, AccessShareLock);

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							(List*)lua,	/* no private state either */
							NIL,	/* no custom tlist */
							NIL,    /* no remote quals */
							outer_plan);
}

static void
luaBeginForeignScan (ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	LuaFdwScanState *scan_state;

	/*
	 * Begin executing a foreign scan. This is called during executor startup.
	 * It should perform any initialization needed before the scan can start,
	 * but not start executing the actual scan (that should be done upon the
	 * first call to IterateForeignScan). The ForeignScanState node has
	 * already been created, but its fdw_state field is still NULL.
	 * Information about the table to scan is accessible through the
	 * ForeignScanState node (in particular, from the underlying ForeignScan
	 * plan node, which contains any FDW-private information provided by
	 * GetForeignPlan). eflags contains flag bits describing the executor's
	 * operating mode for this plan node.
	 *
	 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
	 * should not perform any externally-visible actions; it should only do
	 * the minimum required to make the node state valid for
	 * ExplainForeignScan and EndForeignScan.
	 *
	 */

	//elog(WARNING, "function %s", __func__);

	scan_state = palloc0(sizeof(LuaFdwScanState));
	node->fdw_state = scan_state;
	scan_state->lua = (lua_State*)plan->fdw_private;

	lua_getglobal(scan_state->lua, "ScanStart");

	if (lua_isfunction(scan_state->lua, -1))
	{
		if (lua_pcall(scan_state->lua, 0, 0, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(scan_state->lua, -1))));
	}
	else
	{
		lua_pop(scan_state->lua, 1);
	}
}

static TupleTableSlot *
luaIterateForeignScan(ForeignScanState *node)
{
	LuaFdwScanState *scan_state;
	TupleTableSlot *slot;
	TupleDesc desc;
	int i;
	char *value;

	Oid pgtype;
	regproc typeinput;
	HeapTuple tuple;
	int typemod;
	bool tuple_ok;

	/*
	 * Fetch one row from the foreign source, returning it in a tuple table
	 * slot (the node's ScanTupleSlot should be used for this purpose). Return
	 * NULL if no more rows are available. The tuple table slot infrastructure
	 * allows either a physical or virtual tuple to be returned; in most cases
	 * the latter choice is preferable from a performance standpoint. Note
	 * that this is called in a short-lived memory context that will be reset
	 * between invocations. Create a memory context in BeginForeignScan if you
	 * need longer-lived storage, or use the es_query_cxt of the node's
	 * EState.
	 *
	 * The rows returned must match the column signature of the foreign table
	 * being scanned. If you choose to optimize away fetching columns that are
	 * not needed, you should insert nulls in those column positions.
	 *
	 * Note that PostgreSQL's executor doesn't care whether the rows returned
	 * violate any NOT NULL constraints that were defined on the foreign table
	 * columns — but the planner does care, and may optimize queries
	 * incorrectly if NULL values are present in a column declared not to
	 * contain them. If a NULL value is encountered when the user has declared
	 * that none should be present, it may be appropriate to raise an error
	 * (just as you would need to do in the case of a data type mismatch).
	 */

	//elog(WARNING, "function %s", __func__);

	slot = node->ss.ss_ScanTupleSlot;
	desc = slot->tts_tupleDescriptor;

	memset (slot->tts_values, 0, sizeof(Datum) * desc->natts);
	memset (slot->tts_isnull, true, sizeof(bool) * desc->natts);

	ExecClearTuple(slot);

	/* get the next record, if any, and fill in the slot */

	scan_state = (LuaFdwScanState *) node->fdw_state;
	lua_getglobal(scan_state->lua, "ScanIterate");

	if (lua_isfunction(scan_state->lua, -1))
	{
		if (lua_pcall(scan_state->lua, 0, 1, 0) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(scan_state->lua, -1))));
		}
		else
		{
			if (lua_istable(scan_state->lua, -1))
			{
				for (i = 0; i < desc->natts; i++)
				{
					lua_pushstring(scan_state->lua, desc->attrs[i]->attname.data);
					lua_gettable(scan_state->lua, -2);

					slot->tts_isnull[i] = true;

					pgtype = desc->attrs[i]->atttypid;
					tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtype));
					tuple_ok = HeapTupleIsValid(tuple);

					if (!tuple_ok)
					{
						ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("cache lookup failed for type %u", pgtype)));
					}
					else
					if (lua_isstring(scan_state->lua, -1) && (value = (char*)lua_tostring(scan_state->lua, -1)))
					{
						slot->tts_isnull[i] = false;

						typeinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
						typemod = ((Form_pg_type)GETSTRUCT(tuple))->typtypmod;

						slot->tts_values[i] = OidFunctionCall3(typeinput, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid), Int32GetDatum(typemod));
					}
					lua_pop(scan_state->lua, 1);
					ReleaseSysCache(tuple);
				}
				ExecStoreVirtualTuple(slot);
			}
			lua_pop(scan_state->lua, 1);
		}
	}
	else
	{
		lua_pop(scan_state->lua, 1);
	}

	return slot;
}


static void
luaReScanForeignScan(ForeignScanState *node)
{
	LuaFdwScanState *scan_state;

	/*
	 * Restart the scan from the beginning. Note that any parameters the scan
	 * depends on may have changed value, so the new scan does not necessarily
	 * return exactly the same rows.
	 */

	//elog(WARNING, "function %s", __func__);

	scan_state = (LuaFdwScanState *) node->fdw_state;
	lua_getglobal(scan_state->lua, "ScanRestart");

	if (lua_isfunction(scan_state->lua, -1))
	{
		if (lua_pcall(scan_state->lua, 0, 0, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(scan_state->lua, -1))));
	}
}


static void
luaEndForeignScan(ForeignScanState *node)
{
	LuaFdwScanState *scan_state;

	/*
	 * End the scan and release resources. It is normally not important to
	 * release palloc'd memory, but for example open files and connections to
	 * remote servers should be cleaned up.
	 */

	//elog(WARNING, "function %s", __func__);

	scan_state = (LuaFdwScanState *) node->fdw_state;
	lua_getglobal(scan_state->lua, "ScanEnd");

	if (lua_isfunction(scan_state->lua, -1))
	{
		if (lua_pcall(scan_state->lua, 0, 0, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(scan_state->lua, -1))));
	}

	lua_stop(scan_state->lua);
	node->fdw_state = NULL;
}


static void
luaAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation)
{
	/*
	 * UPDATE and DELETE operations are performed against rows previously
	 * fetched by the table-scanning functions. The FDW may need extra
	 * information, such as a row ID or the values of primary-key columns, to
	 * ensure that it can identify the exact row to update or delete. To
	 * support that, this function can add extra hidden, or "junk", target
	 * columns to the list of columns that are to be retrieved from the
	 * foreign table during an UPDATE or DELETE.
	 *
	 * To do that, add TargetEntry items to parsetree->targetList, containing
	 * expressions for the extra values to be fetched. Each such entry must be
	 * marked resjunk = true, and must have a distinct resname that will
	 * identify it at execution time. Avoid using names matching ctidN or
	 * wholerowN, as the core system can generate junk columns of these names.
	 *
	 * This function is called in the rewriter, not the planner, so the
	 * information available is a bit different from that available to the
	 * planning routines. parsetree is the parse tree for the UPDATE or DELETE
	 * command, while target_rte and target_relation describe the target
	 * foreign table.
	 *
	 * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
	 * expressions are added. (This will make it impossible to implement
	 * DELETE operations, though UPDATE may still be feasible if the FDW
	 * relies on an unchanging primary key to identify rows.)
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "AddForeignUpdateTargets");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "AddForeignUpdateTargets: %s", lua_tostring(lua, -1));
//	}
}


static List *
luaPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
	/*
	 * Perform any additional planning actions needed for an insert, update,
	 * or delete on a foreign table. This function generates the FDW-private
	 * information that will be attached to the ModifyTable plan node that
	 * performs the update action. This private information must have the form
	 * of a List, and will be delivered to BeginForeignModify during the
	 * execution stage.
	 *
	 * root is the planner's global information about the query. plan is the
	 * ModifyTable plan node, which is complete except for the fdwPrivLists
	 * field. resultRelation identifies the target foreign table by its
	 * rangetable index. subplan_index identifies which target of the
	 * ModifyTable plan node this is, counting from zero; use this if you want
	 * to index into plan->plans or other substructure of the plan node.
	 *
	 * If the PlanForeignModify pointer is set to NULL, no additional
	 * plan-time actions are taken, and the fdw_private list delivered to
	 * BeginForeignModify will be NIL.
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "PlanForeignModify");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "PlanForeignModify: %s", lua_tostring(lua, -1));
//	}

	return NULL;
}


static void
luaBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags)
{
	LuaFdwModifyState *modify_state;

	/*
	 * Begin executing a foreign table modification operation. This routine is
	 * called during executor startup. It should perform any initialization
	 * needed prior to the actual table modifications. Subsequently,
	 * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
	 * called for each tuple to be inserted, updated, or deleted.
	 *
	 * mtstate is the overall state of the ModifyTable plan node being
	 * executed; global data about the plan and execution state is available
	 * via this structure. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. (The ri_FdwState field of ResultRelInfo is
	 * available for the FDW to store any private state it needs for this
	 * operation.) fdw_private contains the private data generated by
	 * PlanForeignModify, if any. subplan_index identifies which target of the
	 * ModifyTable plan node this is. eflags contains flag bits describing the
	 * executor's operating mode for this plan node.
	 *
	 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
	 * should not perform any externally-visible actions; it should only do
	 * the minimum required to make the node state valid for
	 * ExplainForeignModify and EndForeignModify.
	 *
	 * If the BeginForeignModify pointer is set to NULL, no action is taken
	 * during executor startup.
	 */

	//elog(WARNING, "function %s", __func__);

	modify_state = palloc0(sizeof(LuaFdwModifyState));
	rinfo->ri_FdwState = modify_state;
	modify_state->lua = lua_start(NULL, NULL);

	lua_getglobal(modify_state->lua, "BeginForeignModify");

	if (lua_isfunction(modify_state->lua, -1) && lua_pcall(modify_state->lua, 0, 0, 0) != 0)
	{
		elog(ERROR, "BeginForeignModify: %s", lua_tostring(modify_state->lua, -1));
	}
}


static TupleTableSlot *
luaExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	LuaFdwModifyState *modify_state;

	/*
	 * Insert one tuple into the foreign table. estate is global execution
	 * state for the query. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. slot contains the tuple to be inserted; it will
	 * match the rowtype definition of the foreign table. planSlot contains
	 * the tuple that was generated by the ModifyTable plan node's subplan; it
	 * differs from slot in possibly containing additional "junk" columns.
	 * (The planSlot is typically of little interest for INSERT cases, but is
	 * provided for completeness.)
	 *
	 * The return value is either a slot containing the data that was actually
	 * inserted (this might differ from the data supplied, for example as a
	 * result of trigger actions), or NULL if no row was actually inserted
	 * (again, typically as a result of triggers). The passed-in slot can be
	 * re-used for this purpose.
	 *
	 * The data in the returned slot is used only if the INSERT query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignInsert pointer is set to NULL, attempts to insert
	 * into the foreign table will fail with an error message.
	 *
	 */

	//elog(WARNING, "function %s", __func__);

	modify_state = (LuaFdwModifyState *) rinfo->ri_FdwState;
	lua_getglobal(modify_state->lua, "ExecForeignInsert");

	if (lua_isfunction(modify_state->lua, -1) && lua_pcall(modify_state->lua, 0, 0, 0) != 0)
	{
		elog(ERROR, "ExecForeignInsert: %s", lua_tostring(modify_state->lua, -1));
	}

	return slot;
}


static TupleTableSlot *
luaExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	LuaFdwModifyState *modify_state;

	/*
	 * Update one tuple in the foreign table. estate is global execution state
	 * for the query. rinfo is the ResultRelInfo struct describing the target
	 * foreign table. slot contains the new data for the tuple; it will match
	 * the rowtype definition of the foreign table. planSlot contains the
	 * tuple that was generated by the ModifyTable plan node's subplan; it
	 * differs from slot in possibly containing additional "junk" columns. In
	 * particular, any junk columns that were requested by
	 * AddForeignUpdateTargets will be available from this slot.
	 *
	 * The return value is either a slot containing the row as it was actually
	 * updated (this might differ from the data supplied, for example as a
	 * result of trigger actions), or NULL if no row was actually updated
	 * (again, typically as a result of triggers). The passed-in slot can be
	 * re-used for this purpose.
	 *
	 * The data in the returned slot is used only if the UPDATE query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
	 * foreign table will fail with an error message.
	 *
	 */

	//elog(WARNING, "function %s", __func__);

	modify_state = (LuaFdwModifyState *) rinfo->ri_FdwState;
	lua_getglobal(modify_state->lua, "ExecForeignUpdate");

	if (lua_isfunction(modify_state->lua, -1) && lua_pcall(modify_state->lua, 0, 0, 0) != 0)
	{
		elog(ERROR, "ExecForeignUpdate: %s", lua_tostring(modify_state->lua, -1));
	}

	return slot;
}


static TupleTableSlot *
luaExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	LuaFdwModifyState *modify_state;

	/*
	 * Delete one tuple from the foreign table. estate is global execution
	 * state for the query. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. slot contains nothing useful upon call, but can
	 * be used to hold the returned tuple. planSlot contains the tuple that
	 * was generated by the ModifyTable plan node's subplan; in particular, it
	 * will carry any junk columns that were requested by
	 * AddForeignUpdateTargets. The junk column(s) must be used to identify
	 * the tuple to be deleted.
	 *
	 * The return value is either a slot containing the row that was deleted,
	 * or NULL if no row was deleted (typically as a result of triggers). The
	 * passed-in slot can be used to hold the tuple to be returned.
	 *
	 * The data in the returned slot is used only if the DELETE query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignDelete pointer is set to NULL, attempts to delete
	 * from the foreign table will fail with an error message.
	 */

	/* ----
	 * LuaFdwModifyState *modify_state =
	 *	 (LuaFdwModifyState *) rinfo->ri_FdwState;
	 * ----
	 */

	//elog(WARNING, "function %s", __func__);

	modify_state = (LuaFdwModifyState *) rinfo->ri_FdwState;
	lua_getglobal(modify_state->lua, "ExecForeignDelete");

	if (lua_isfunction(modify_state->lua, -1) && lua_pcall(modify_state->lua, 0, 0, 0) != 0)
	{
		elog(ERROR, "ExecForeignDelete: %s", lua_tostring(modify_state->lua, -1));
	}

	return slot;
}


static void
luaEndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
	LuaFdwModifyState *modify_state;

	/*
	 * End the table update and release resources. It is normally not
	 * important to release palloc'd memory, but for example open files and
	 * connections to remote servers should be cleaned up.
	 *
	 * If the EndForeignModify pointer is set to NULL, no action is taken
	 * during executor shutdown.
	 */

	//elog(WARNING, "function %s", __func__);

	modify_state = (LuaFdwModifyState *) rinfo->ri_FdwState;
	lua_getglobal(modify_state->lua, "EndForeignModify");

	if (lua_isfunction(modify_state->lua, -1) && lua_pcall(modify_state->lua, 0, 0, 0) != 0)
	{
		elog(ERROR, "EndForeignModify: %s", lua_tostring(modify_state->lua, -1));
	}
}

static int
luaIsForeignRelUpdatable(Relation rel)
{
	/*
	 * Report which update operations the specified foreign table supports.
	 * The return value should be a bit mask of rule event numbers indicating
	 * which operations are supported by the foreign table, using the CmdType
	 * enumeration; that is, (1 << CMD_UPDATE) = 4 for UPDATE, (1 <<
	 * CMD_INSERT) = 8 for INSERT, and (1 << CMD_DELETE) = 16 for DELETE.
	 *
	 * If the IsForeignRelUpdatable pointer is set to NULL, foreign tables are
	 * assumed to be insertable, updatable, or deletable if the FDW provides
	 * ExecForeignInsert, ExecForeignUpdate, or ExecForeignDelete
	 * respectively. This function is only needed if the FDW supports some
	 * tables that are updatable and some that are not. (Even then, it's
	 * permissible to throw an error in the execution routine instead of
	 * checking in this function. However, this function is used to determine
	 * updatability for display in the information_schema views.)
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "IsForeignRelUpdatable");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "IsForeignRelUpdatable: %s", lua_tostring(lua, -1));
//	}

	return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}


static void
luaExplainForeignScan (ForeignScanState *node, struct ExplainState * es)
{
	LuaFdwScanState *scan_state;

	/*
	 * Print additional EXPLAIN output for a foreign table scan. This function
	 * can call ExplainPropertyText and related functions to add fields to the
	 * EXPLAIN output. The flag fields in es can be used to determine what to
	 * print, and the state of the ForeignScanState node can be inspected to
	 * provide run-time statistics in the EXPLAIN ANALYZE case.
	 *
	 * If the ExplainForeignScan pointer is set to NULL, no additional
	 * information is printed during EXPLAIN.
	 */

	//elog(WARNING, "function %s", __func__);

	scan_state = (LuaFdwScanState *) node->fdw_state;
	lua_getglobal(scan_state->lua, "ScanExplain");

	if (lua_isfunction(scan_state->lua, -1))
	{
		if (lua_pcall(scan_state->lua, 0, 1, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(scan_state->lua, -1))));
		else
		{
			if (lua_isstring(scan_state->lua, -1))
				ExplainPropertyText("lua_fdw", lua_tostring(scan_state->lua, -1), es);

			lua_pop(scan_state->lua, 1);
		}
	}
}

static void
luaExplainForeignModify (ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, struct ExplainState * es)
{
	LuaFdwModifyState *modify_state;

	/*
	 * Print additional EXPLAIN output for a foreign table update. This
	 * function can call ExplainPropertyText and related functions to add
	 * fields to the EXPLAIN output. The flag fields in es can be used to
	 * determine what to print, and the state of the ModifyTableState node can
	 * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
	 * case. The first four arguments are the same as for BeginForeignModify.
	 *
	 * If the ExplainForeignModify pointer is set to NULL, no additional
	 * information is printed during EXPLAIN.
	 */

	//elog(WARNING, "function %s", __func__);

	modify_state = (LuaFdwModifyState *) rinfo->ri_FdwState;
	lua_getglobal(modify_state->lua, "ExplainForeignModify");

	if (lua_isfunction(modify_state->lua, -1))
	{
		if (lua_pcall(modify_state->lua, 0, 1, 0) != 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("lua_fdw lua error: %s", lua_tostring(modify_state->lua, -1))));
		else
		{
			if (lua_isstring(modify_state->lua, -1))
				ExplainPropertyText("lua_fdw", lua_tostring(modify_state->lua, -1), es);

			lua_pop(modify_state->lua, 1);
		}
	}
}

static bool
luaAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	/* ----
	 * This function is called when ANALYZE is executed on a foreign table. If
	 * the FDW can collect statistics for this foreign table, it should return
	 * true, and provide a pointer to a function that will collect sample rows
	 * from the table in func, plus the estimated size of the table in pages
	 * in totalpages. Otherwise, return false.
	 *
	 * If the FDW does not support collecting statistics for any tables, the
	 * AnalyzeForeignTable pointer can be set to NULL.
	 *
	 * If provided, the sample collection function must have the signature:
	 *
	 *	  int
	 *	  AcquireSampleRowsFunc (Relation relation, int elevel,
	 *							 HeapTuple *rows, int targrows,
	 *							 double *totalrows,
	 *							 double *totaldeadrows);
	 *
	 * A random sample of up to targrows rows should be collected from the
	 * table and stored into the caller-provided rows array. The actual number
	 * of rows collected must be returned. In addition, store estimates of the
	 * total numbers of live and dead rows in the table into the output
	 * parameters totalrows and totaldeadrows. (Set totaldeadrows to zero if
	 * the FDW does not have any concept of dead rows.)
	 * ----
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "AnalyzeForeignTable");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "AnalyzeForeignTable: %s", lua_tostring(lua, -1));
//	}

	return false;
}

static void
luaGetForeignJoinPaths (PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel, RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra)
{
	/*
	 * Create possible access paths for a join of two (or more) foreign tables
	 * that all belong to the same foreign server. This optional function is
	 * called during query planning. As with GetForeignPaths, this function
	 * should generate ForeignPath path(s) for the supplied joinrel, and call
	 * add_path to add these paths to the set of paths considered for the
	 * join. But unlike GetForeignPaths, it is not necessary that this
	 * function succeed in creating at least one path, since paths involving
	 * local joining are always possible.
	 *
	 * Note that this function will be invoked repeatedly for the same join
	 * relation, with different combinations of inner and outer relations; it
	 * is the responsibility of the FDW to minimize duplicated work.
	 *
	 * If a ForeignPath path is chosen for the join, it will represent the
	 * entire join process; paths generated for the component tables and
	 * subsidiary joins will not be used. Subsequent processing of the join
	 * path proceeds much as it does for a path scanning a single foreign
	 * table. One difference is that the scanrelid of the resulting
	 * ForeignScan plan node should be set to zero, since there is no single
	 * relation that it represents; instead, the fs_relids field of the
	 * ForeignScan node represents the set of relations that were joined. (The
	 * latter field is set up automatically by the core planner code, and need
	 * not be filled by the FDW.) Another difference is that, because the
	 * column list for a remote join cannot be found from the system catalogs,
	 * the FDW must fill fdw_scan_tlist with an appropriate list of
	 * TargetEntry nodes, representing the set of columns it will supply at
	 * runtime in the tuples it returns.
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "GetForeignJoinPaths");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "GetForeignJoinPaths: %s", lua_tostring(lua, -1));
//	}

}


static RowMarkType
luaGetForeignRowMarkType (RangeTblEntry *rte, LockClauseStrength strength)
{
	/*
	 * Report which row-marking option to use for a foreign table. rte is the
	 * RangeTblEntry node for the table and strength describes the lock
	 * strength requested by the relevant FOR UPDATE/SHARE clause, if any. The
	 * result must be a member of the RowMarkType enum type.
	 *
	 * This function is called during query planning for each foreign table
	 * that appears in an UPDATE, DELETE, or SELECT FOR UPDATE/SHARE query and
	 * is not the target of UPDATE or DELETE.
	 *
	 * If the GetForeignRowMarkType pointer is set to NULL, the ROW_MARK_COPY
	 * option is always used. (This implies that RefetchForeignRow will never
	 * be called, so it need not be provided either.)
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "GetForeignRowMarkType");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "GetForeignRowMarkType: %s", lua_tostring(lua, -1));
//	}

	return ROW_MARK_COPY;

}

static HeapTuple
luaRefetchForeignRow(EState *estate, ExecRowMark *erm, Datum rowid, bool *updated)
{
	/*
	 * Re-fetch one tuple from the foreign table, after locking it if
	 * required. estate is global execution state for the query. erm is the
	 * ExecRowMark struct describing the target foreign table and the row lock
	 * type (if any) to acquire. rowid identifies the tuple to be fetched.
	 * updated is an output parameter.
	 *
	 * This function should return a palloc'ed copy of the fetched tuple, or
	 * NULL if the row lock couldn't be obtained. The row lock type to acquire
	 * is defined by erm->markType, which is the value previously returned by
	 * GetForeignRowMarkType. (ROW_MARK_REFERENCE means to just re-fetch the
	 * tuple without acquiring any lock, and ROW_MARK_COPY will never be seen
	 * by this routine.)
	 *
	 * In addition, *updated should be set to true if what was fetched was an
	 * updated version of the tuple rather than the same version previously
	 * obtained. (If the FDW cannot be sure about this, always returning true
	 * is recommended.)
	 *
	 * Note that by default, failure to acquire a row lock should result in
	 * raising an error; a NULL return is only appropriate if the SKIP LOCKED
	 * option is specified by erm->waitPolicy.
	 *
	 * The rowid is the ctid value previously read for the row to be
	 * re-fetched. Although the rowid value is passed as a Datum, it can
	 * currently only be a tid. The function API is chosen in hopes that it
	 * may be possible to allow other datatypes for row IDs in future.
	 *
	 * If the RefetchForeignRow pointer is set to NULL, attempts to re-fetch
	 * rows will fail with an error message.
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "RefetchForeignRow");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "RefetchForeignRow: %s", lua_tostring(lua, -1));
//	}

	return NULL;

}


static List *
luaImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	/*
	 * Obtain a list of foreign table creation commands. This function is
	 * called when executing IMPORT FOREIGN SCHEMA, and is passed the parse
	 * tree for that statement, as well as the OID of the foreign server to
	 * use. It should return a list of C strings, each of which must contain a
	 * CREATE FOREIGN TABLE command. These strings will be parsed and executed
	 * by the core server.
	 *
	 * Within the ImportForeignSchemaStmt struct, remote_schema is the name of
	 * the remote schema from which tables are to be imported. list_type
	 * identifies how to filter table names: FDW_IMPORT_SCHEMA_ALL means that
	 * all tables in the remote schema should be imported (in this case
	 * table_list is empty), FDW_IMPORT_SCHEMA_LIMIT_TO means to include only
	 * tables listed in table_list, and FDW_IMPORT_SCHEMA_EXCEPT means to
	 * exclude the tables listed in table_list. options is a list of options
	 * used for the import process. The meanings of the options are up to the
	 * FDW. For example, an FDW could use an option to define whether the NOT
	 * NULL attributes of columns should be imported. These options need not
	 * have anything to do with those supported by the FDW as database object
	 * options.
	 *
	 * The FDW may ignore the local_schema field of the
	 * ImportForeignSchemaStmt, because the core server will automatically
	 * insert that name into the parsed CREATE FOREIGN TABLE commands.
	 *
	 * The FDW does not have to concern itself with implementing the filtering
	 * specified by list_type and table_list, either, as the core server will
	 * automatically skip any returned commands for tables excluded according
	 * to those options. However, it's often useful to avoid the work of
	 * creating commands for excluded tables in the first place. The function
	 * IsImportableForeignTable() may be useful to test whether a given
	 * foreign-table name will pass the filter.
	 */

	//elog(WARNING, "function %s", __func__);

//	lua_getglobal(lua, "ImportForeignSchema");
//
//	if (lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0)
//	{
//		elog(ERROR, "ImportForeignSchema: %s", lua_tostring(lua, -1));
//	}

	return NULL;
}
