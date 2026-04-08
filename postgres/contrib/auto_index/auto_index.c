#include "postgres.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include <string.h>

PG_MODULE_MAGIC;

static ExecutorRun_hook_type prev_ExecutorRun = NULL;

void auto_index_executor_run(QueryDesc *queryDesc,
                             ScanDirection direction,
                             uint64 count,
                             bool execute_once)
{
    /* STEP 1: Let query execute FIRST (IMPORTANT FIX) */
    if (prev_ExecutorRun)
        prev_ExecutorRun(queryDesc, direction, count, execute_once);
    else
        standard_ExecutorRun(queryDesc, direction, count, execute_once);

    /* STEP 2: Now safe to analyze and create index */
    if (queryDesc && queryDesc->sourceText)
    {
        char *query = queryDesc->sourceText;

        /* Process only SELECT with WHERE */
        if (strstr(query, "SELECT") && strstr(query, "WHERE"))
        {
            SPI_connect();

            /* STEP 3: Run EXPLAIN */
            char explain_query[512];
            snprintf(explain_query, sizeof(explain_query),
                     "EXPLAIN (FORMAT JSON) %s", query);

            SPI_exec(explain_query, 1);

            if (SPI_processed > 0)
            {
                char *plan = SPI_getvalue(SPI_tuptable->vals[0],
                                          SPI_tuptable->tupdesc, 1);

                /* STEP 4: Detect Sequential Scan */
                if (plan && strstr(plan, "Seq Scan"))
                {
                    elog(WARNING, "Seq Scan detected");

                    /* STEP 5: Extract table */
                    char table[64] = {0};
                    sscanf(query, "SELECT * FROM %63s", table);

                    /* STEP 6: Extract column */
                    char column[64] = {0};
                    char *where_ptr = strstr(query, "WHERE");
                    if (where_ptr)
                        sscanf(where_ptr + 6, "%63s", column);

                    elog(WARNING, "Table: %s Column: %s", table, column);

                    /* STEP 7: Update scan stats */
                    char stats_query[512];
                    snprintf(stats_query, sizeof(stats_query),
                        "INSERT INTO column_scan_stats(table_name, column_name, scan_count) "
                        "VALUES ('%s','%s',1) "
                        "ON CONFLICT (table_name, column_name) "
                        "DO UPDATE SET scan_count = column_scan_stats.scan_count + 1;",
                        table, column);

                    SPI_exec(stats_query, 0);

                    /* STEP 8: Get scan count */
                    char check_query[256];
                    snprintf(check_query, sizeof(check_query),
                        "SELECT scan_count FROM column_scan_stats "
                        "WHERE table_name='%s' AND column_name='%s';",
                        table, column);

                    SPI_exec(check_query, 1);

                    if (SPI_processed > 0)
                    {
                        int scan_count = atoi(
                            SPI_getvalue(SPI_tuptable->vals[0],
                                         SPI_tuptable->tupdesc, 1));

                        elog(WARNING, "Scan count: %d", scan_count);

                        /* STEP 9: Threshold check */
                        if (scan_count > 5)
                        {
                            char index_query[256];
                            snprintf(index_query, sizeof(index_query),
                                "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s);",
                                table, column, table, column);

                            SPI_exec(index_query, 0);

                            elog(WARNING, "Index created on %s(%s)", table, column);
                        }
                    }
                }
            }

            SPI_finish();
        }
    }
}

/* INIT */
void _PG_init(void)
{
    elog(WARNING, "AUTO_INDEX LOADED");

    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = auto_index_executor_run;
}

/* CLEANUP */
void _PG_fini(void)
{
    ExecutorRun_hook = prev_ExecutorRun;
}