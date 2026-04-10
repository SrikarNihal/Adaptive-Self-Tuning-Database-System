#include "postgres.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"
#include "nodes/plannodes.h"
#include "utils/snapmgr.h"
#include <string.h>
#include <ctype.h>

PG_MODULE_MAGIC;

ExecutorRun_hook_type prev_ExecutorRun = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

bool auto_index_in_progress = false;

typedef struct PendingIndex {
    char table_name[64];
    char column_name[64];
    struct PendingIndex *next;
} PendingIndex;

PendingIndex *pending_index_head = NULL;

char *ci_strstr(char *text, char *pattern) {
    int nlen = strlen(pattern);

    if (nlen == 0)
        return (char *)text;
    for (; *text; text++) {
        if (strlen(text) < nlen)
            return NULL;
        if (pg_strncasecmp(text, pattern, nlen) == 0)
            return (char *)text;
    }

    return NULL;
}

bool plan_has_seqscan(Plan *plan){
    if (!plan) return false;

    if (nodeTag(plan) == T_SeqScan)
        return true;

    return plan_has_seqscan(plan->lefttree) || plan_has_seqscan(plan->righttree);
}


void enqueue_index(char *table, char *column){
    MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

    PendingIndex *pi = palloc(sizeof(PendingIndex));
    strlcpy(pi->table_name, table, sizeof(pi->table_name));
    strlcpy(pi->column_name, column, sizeof(pi->column_name));

    pi->next = pending_index_head;
    pending_index_head = pi;

    MemoryContextSwitchTo(old);
}


void auto_index_executor_run(QueryDesc *queryDesc,ScanDirection direction,uint64 count,bool execute_once){
    if (prev_ExecutorRun) prev_ExecutorRun(queryDesc, direction, count, execute_once);
    else standard_ExecutorRun(queryDesc, direction, count, execute_once);

    if (auto_index_in_progress) return;

    if (!queryDesc || !queryDesc->sourceText || !queryDesc->plannedstmt) return;

    char *query = queryDesc->sourceText;

    if (!ci_strstr(query, "SELECT") || !ci_strstr(query, "WHERE")) return;

    if (!plan_has_seqscan(queryDesc->plannedstmt->planTree)) return;

    char *from_ptr = ci_strstr(query, "FROM");
    if (!from_ptr) return;

    from_ptr += 4;
    while (*from_ptr == ' ') from_ptr++;

    char table[64] = {0};
    sscanf(from_ptr, "%63s", table);

    for (char *p = table; *p; p++){
        if (!isalnum((unsigned char)*p) && *p != '_'){
            *p = '\0'; 
            break; 
        }
    }

    char *where_ptr = ci_strstr(query, "WHERE");
    if (!where_ptr) return;

    where_ptr += 5;
    while (*where_ptr == ' ') where_ptr++;

    char column[64] = {0};
    sscanf(where_ptr, "%63s", column);

    for (char *p = column; *p; p++){
        if (*p == '=' || *p == '>' || *p == '<' || *p == '!'){
            *p = '\0'; 
            break; 
        }
    }

    if (table[0] == '\0' || column[0] == '\0') return;

    elog(WARNING, "AUTO_INDEX: SeqScan on %s(%s) — queued", table, column);

    enqueue_index(table, column);
}


void auto_index_executor_end(QueryDesc *queryDesc){
    if (prev_ExecutorEnd) prev_ExecutorEnd(queryDesc);
    else standard_ExecutorEnd(queryDesc);

    if (pending_index_head == NULL || auto_index_in_progress) return;

    auto_index_in_progress = true;

    PushActiveSnapshot(GetTransactionSnapshot());

    if (SPI_connect() != SPI_OK_CONNECT){
        elog(WARNING, "AUTO_INDEX: SPI_connect failed");
        PopActiveSnapshot();
        auto_index_in_progress = false;
        return;
    }

    PendingIndex *pi = pending_index_head;
    pending_index_head = NULL;

    while (pi){
        PendingIndex *next = pi->next;

        elog(WARNING, "AUTO_INDEX: processing %s(%s)",pi->table_name, pi->column_name);

        char stats_query[512];
        snprintf(stats_query, sizeof(stats_query),
            "INSERT INTO column_scan_stats(table_name, column_name, scan_count) "
            "VALUES ('%s','%s',1) "
            "ON CONFLICT (table_name, column_name) "
            "DO UPDATE SET scan_count = column_scan_stats.scan_count + 1;",
            pi->table_name, pi->column_name);

        SPI_exec(stats_query, 0);

        char check_query[256];
        snprintf(check_query, sizeof(check_query),
            "SELECT scan_count FROM column_scan_stats "
            "WHERE table_name='%s' AND column_name='%s';",
            pi->table_name, pi->column_name);

        SPI_exec(check_query, 1);

        int scan_count = 0;
        if (SPI_processed > 0){
            scan_count = atoi(SPI_getvalue(SPI_tuptable->vals[0],SPI_tuptable->tupdesc, 1));
        }

        elog(WARNING, "AUTO_INDEX: scan_count=%d", scan_count);

        if (scan_count > 5){
            StringInfoData buf;
            initStringInfo(&buf);

            appendStringInfo(&buf,
                "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s);",
                pi->table_name, pi->column_name,pi->table_name, pi->column_name);

            SPI_exec(buf.data, 0);

            elog(WARNING, "AUTO_INDEX: index created on %s(%s)",pi->table_name, pi->column_name);

            pfree(buf.data);
        }

        pfree(pi);
        pi = next;
    }

    SPI_finish();

    PopActiveSnapshot();

    auto_index_in_progress = false;
}

void _PG_init(void){
    elog(WARNING, "AUTO_INDEX LOADED");

    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = auto_index_executor_run;

    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = auto_index_executor_end;
}

void _PG_fini(void){
    ExecutorRun_hook = prev_ExecutorRun;
    ExecutorEnd_hook = prev_ExecutorEnd;
}