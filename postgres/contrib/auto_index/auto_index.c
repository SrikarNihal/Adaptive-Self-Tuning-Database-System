#include "postgres.h"
#include "executor/executor.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

static ExecutorRun_hook_type prev_ExecutorRun = NULL;

/* Our hook */
void auto_index_executor_run(QueryDesc *queryDesc,
                             ScanDirection direction,
                             uint64 count,
                             bool execute_once)
{
    if (queryDesc && queryDesc->sourceText)
    {
        char *query = queryDesc->sourceText;

        /* Only process SELECT queries with WHERE */
        if (strstr(query, "SELECT") && strstr(query, "WHERE"))
        {
            elog(WARNING, "Candidate query: %s", query);
        }
    }

    /* Call original execution */
    if (prev_ExecutorRun)
        prev_ExecutorRun(queryDesc, direction, count, execute_once);
    else
        standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

/* Init */
void _PG_init(void)
{
    elog(WARNING, "AUTO_INDEX LOADED");

    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = auto_index_executor_run;
}

/* Cleanup */
void _PG_fini(void)
{
    ExecutorRun_hook = prev_ExecutorRun;
}