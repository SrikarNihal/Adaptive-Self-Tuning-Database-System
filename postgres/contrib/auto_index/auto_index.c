
#include "postgres.h"

#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


#define STR_INNER(x) #x
#define STR(x)       STR_INNER(x)

PG_MODULE_MAGIC;

#define AI_SCORE_THRESHOLD  50.0   
#define AI_MIN_ROWS  1000.0
#define AI_DECAY_FACTOR  0.9
#define AI_MAX_AGE_DAYS  7
#define AI_QUEUE_SIZE  256
#define AI_MAX_COLS_PER_SCAN  16


typedef struct ShmEntry
{
    char table[NAMEDATALEN];
    char column[NAMEDATALEN];
    double rows;
} ShmEntry;

typedef struct AutoIndexShmem
{
    int      head;
    int      tail;
    ShmEntry entries[AI_QUEUE_SIZE];
} AutoIndexShmem;

static AutoIndexShmem *ai_shm  = NULL;
static LWLock         *ai_lock = NULL;


static ExecutorRun_hook_type   prev_ExecutorRun   = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEnd   = NULL;
static shmem_request_hook_type prev_shmem_request = NULL;
static shmem_startup_hook_type prev_shmem_startup = NULL;


static Size ai_shmem_size(void){
    return sizeof(AutoIndexShmem);
}

static void ai_shmem_request(void){
    if (prev_shmem_request)
        prev_shmem_request();

    RequestAddinShmemSpace(ai_shmem_size());
    RequestNamedLWLockTranche("auto_index", 1);
}

static void ai_shmem_startup(void){
    bool found;

    if (prev_shmem_startup)
        prev_shmem_startup();

    ai_lock = &GetNamedLWLockTranche("auto_index")->lock;

    ai_shm = (AutoIndexShmem *)
        ShmemInitStruct("auto_index", ai_shmem_size(), &found);

    if (!found)
    {
        ai_shm->head = 0;
        ai_shm->tail = 0;
    }
}

static const char * const ai_internal_tables[] = {
    "column_scan_stats",
    "write_stats",
    "pg_namespace",
    "pg_class",
    "pg_statistic",
    "pg_stat_user_tables",
    "pg_index",
    "pg_attribute",
    NULL
};

static bool
ai_is_internal_table(const char *tname)
{
    int i;
    for (i = 0; ai_internal_tables[i] != NULL; i++)
        if (strcmp(tname, ai_internal_tables[i]) == 0)
            return true;
    return false;
}

typedef struct{
    Oid   relid;
    int   ncols;
    char *columns[AI_MAX_COLS_PER_SCAN];
} QualCtx;

static bool find_var(Node *node, void *context){
    QualCtx *ctx = (QualCtx *) context;
    char    *colname;
    int      k;

    if (!node)
        return false;

    if (IsA(node, Var))
    {
        Var *v = (Var *) node;

        if (v->varattno > 0 && ctx->ncols < AI_MAX_COLS_PER_SCAN)
        {
            colname = get_attname(ctx->relid, v->varattno, true);

            if (colname)
            {
                for (k = 0; k < ctx->ncols; k++)
                    if (strcmp(ctx->columns[k], colname) == 0)
                        return false; 

                ctx->columns[ctx->ncols++] = colname;
            }
        }
        return false;
    }

    return expression_tree_walker(node, find_var, context);
}

static void walk_plan(Plan *plan, PlannedStmt *pstmt){
    SeqScan       *ss;
    RangeTblEntry *rte;
    const char    *tname;
    QualCtx        ctx;
    int            k;
    int            next_tail;

    if (!plan)
        return;

    if (nodeTag(plan) == T_SeqScan)
    {
        ss = (SeqScan *) plan;

        if (ss->scan.scanrelid > 0 &&
            ss->scan.scanrelid <= (Index) list_length(pstmt->rtable))
        {
            rte = (RangeTblEntry *)
                list_nth(pstmt->rtable, ss->scan.scanrelid - 1);

            if (rte->rtekind == RTE_RELATION)
            {
                tname     = get_rel_name(rte->relid);
                ctx.relid = rte->relid;
                ctx.ncols = 0;

                find_var((Node *) plan->qual, &ctx);

                if (tname && ctx.ncols > 0 && ai_shm &&
                    !ai_is_internal_table(tname))
                {
                    LWLockAcquire(ai_lock, LW_EXCLUSIVE);

                    for (k = 0; k < ctx.ncols; k++)
                    {
                        next_tail = (ai_shm->tail + 1) % AI_QUEUE_SIZE;

                        if (next_tail != ai_shm->head){
                            strlcpy(ai_shm->entries[ai_shm->tail].table,
                                    tname, NAMEDATALEN);
                            strlcpy(ai_shm->entries[ai_shm->tail].column,
                                    ctx.columns[k], NAMEDATALEN);
                            ai_shm->tail = next_tail;
                        }
                        else{
                            elog(LOG,"AUTO_INDEX: queue full, dropping %s(%s)",tname, ctx.columns[k]);
                            break;
                        }
                    }

                    LWLockRelease(ai_lock);
                }
            }
        }
    }

    walk_plan(plan->lefttree,  pstmt);
    walk_plan(plan->righttree, pstmt);
}

void auto_index_executor_run(QueryDesc *qd,ScanDirection dir,uint64 count,bool once){
    if (prev_ExecutorRun)
        prev_ExecutorRun(qd, dir, count, once);
    else
        standard_ExecutorRun(qd, dir, count, once);

    if (!qd || !qd->plannedstmt || !ai_shm)
        return;

    if (qd->operation == CMD_INSERT || qd->operation == CMD_UPDATE || qd->operation == CMD_DELETE){
        elog(LOG, "AUTO_INDEX: ExecutorRun operation=%d", qd->operation);
        RangeTblEntry *rte;
        const char *tname;
        int next_tail;

        if (qd->plannedstmt->rtable && list_length(qd->plannedstmt->rtable) > 0){
            rte = (RangeTblEntry *) list_nth(qd->plannedstmt->rtable, 0);

            if (rte && rte->rtekind == RTE_RELATION){
                tname = get_rel_name(rte->relid);

                if (tname && ai_shm && !ai_is_internal_table(tname)){
                    LWLockAcquire(ai_lock, LW_EXCLUSIVE);

                    next_tail = (ai_shm->tail + 1) % AI_QUEUE_SIZE;

                    if (next_tail != ai_shm->head){
                        strlcpy(ai_shm->entries[ai_shm->tail].table,
                                tname, NAMEDATALEN);

                        ai_shm->entries[ai_shm->tail].column[0] = '\0';
                        double rows = (qd->estate && qd->estate->es_processed > 0)? (double) qd->estate->es_processed: 1.0;
                        
                        ai_shm->entries[ai_shm->tail].rows = rows;

                        elog(LOG, "AUTO_INDEX: tracked write on %s", tname);

                        ai_shm->tail = next_tail;
                    }

                    LWLockRelease(ai_lock);
                }
            }
        }
    }

    walk_plan(qd->plannedstmt->planTree, qd->plannedstmt);
}

void auto_index_executor_end(QueryDesc *qd){
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(qd);
    else
        standard_ExecutorEnd(qd);
}

static double
score_candidate(HeapTuple tup, TupleDesc tupdesc)
{
    bool   isnull;
    double scan_count;
    double seq_cost;
    double selectivity;
    double correlation;
    double write_count;
    double rnd_cost;
    double reltuples;
    double benefit;
    double penalty;

    scan_count  = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 1, &isnull));
    if (isnull) return 0.0;
    seq_cost    = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 2, &isnull));
    if (isnull) return 0.0;
    selectivity = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 3, &isnull));
    if (isnull) selectivity = 0.05;
    correlation = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 4, &isnull));
    if (isnull) correlation = 0.5;
    write_count = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 5, &isnull));
    if (isnull) write_count = 0.0;
    rnd_cost    = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 6, &isnull));
    if (isnull) rnd_cost = 4.0;
    reltuples   = DatumGetFloat8(SPI_getbinval(tup, tupdesc, 7, &isnull));
    if (isnull || reltuples < AI_MIN_ROWS) return 0.0;

    if (selectivity > 0.20)
        return 0.0;

    benefit = scan_count* seq_cost* (1.0 - selectivity)* (0.5 + 0.5 * correlation);

    penalty = write_count * rnd_cost * 0.1;

    return benefit - penalty;
}

PGDLLEXPORT void auto_index_worker_main(Datum arg);

void
auto_index_worker_main(Datum arg){
    Oid   argtypes[2];
    Datum values[2];
    char  nulls[2];
    int   rc;

    ShmEntry drain[AI_QUEUE_SIZE];
    int      drain_count;
    int      i;

    uint64  n;
    uint64  j;
    char  (*tables)[NAMEDATALEN];
    char  (*cols)[NAMEDATALEN];

    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "AUTO_INDEX worker started");

    argtypes[0] = TEXTOID;
    argtypes[1] = TEXTOID;
    nulls[0]    = ' ';
    nulls[1]    = ' ';

    for (;;){
        drain_count = 0;

        LWLockAcquire(ai_lock, LW_EXCLUSIVE);
        while (ai_shm->head != ai_shm->tail && drain_count < AI_QUEUE_SIZE)
        {
            drain[drain_count++] = ai_shm->entries[ai_shm->head];
            ai_shm->head = (ai_shm->head + 1) % AI_QUEUE_SIZE;
        }
        LWLockRelease(ai_lock);

        if (drain_count > 0){
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());

            for (i = 0; i < drain_count; i++){

                if (drain[i].column[0] == '\0'){
                    values[0] = CStringGetTextDatum(drain[i].table);
                    values[1] = Float8GetDatum(drain[i].rows);

                    argtypes[0] = TEXTOID;
                    argtypes[1] = FLOAT8OID;


                    SPI_execute_with_args(
                        "INSERT INTO write_stats "
                        "    (table_name, write_count, last_seen) "
                        "VALUES ($1, $2, now()) "
                        "ON CONFLICT (table_name) "
                        "DO UPDATE SET "
                        "    write_count = write_stats.write_count + 1, "
                        "    last_seen  = now()",
                        2, argtypes, values, nulls,
                        false, 0);
                }
                else{
                    values[0] = CStringGetTextDatum(drain[i].table);
                    values[1] = CStringGetTextDatum(drain[i].column);
                    argtypes[0] = TEXTOID;
                    argtypes[1] = TEXTOID;

                    SPI_execute_with_args(
                        "INSERT INTO column_scan_stats "
                        "    (table_name, column_name, scan_count, last_seen) "
                        "VALUES ($1, $2, 1, now()) "
                        "ON CONFLICT (table_name, column_name) "
                        "DO UPDATE SET "
                        "    scan_count = column_scan_stats.scan_count + 1, "
                        "    last_seen  = now()",
                        2, argtypes, values, nulls,
                        false, 0);
                }
            }

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }


        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        SPI_execute(
            "SELECT s.scan_count::float8,"
            "       (pc.relpages *"
            "           current_setting('seq_page_cost')::float8) AS seq_cost,"
            "       CASE"
            "           WHEN ps.n_distinct < 0"
            "               THEN GREATEST(-ps.n_distinct, 0.001)"
            "           WHEN ps.n_distinct > 0 AND pc.reltuples > 0"
            "               THEN GREATEST(1.0 / ps.n_distinct, 0.001)"
            "           ELSE 0.05"
            "       END AS selectivity,"
            "       COALESCE(ABS(ps.correlation), 0.5) AS correlation,"
            "       w.write_count::float8 AS write_count,"
            "       current_setting('random_page_cost')::float8 AS rnd_cost,"
            "       pc.reltuples::float8,"
            "       s.table_name,"
            "       s.column_name "
            "FROM   column_scan_stats s "
            "LEFT JOIN write_stats w "
            "ON w.table_name = s.table_name "
            "JOIN   pg_class pc"
            "           ON pc.relname = s.table_name"
            "          AND pc.relkind = 'r' "
            "LEFT   JOIN pg_stats ps"
            "           ON ps.tablename = s.table_name"
            "          AND ps.attname   = s.column_name "
            "LEFT   JOIN pg_stat_user_tables psu"
            "           ON psu.relname = s.table_name "
            "WHERE  NOT EXISTS ("
            "    SELECT 1"
            "    FROM   pg_index     pi"
            "    JOIN   pg_attribute pa"
            "               ON pa.attrelid = pi.indrelid"
            "              AND pa.attnum   = pi.indkey[0]"
            "    WHERE  pi.indrelid = pc.oid"
            "      AND  pa.attname  = s.column_name"
            ") "
            "AND    pc.reltuples > 1000 "
            "AND    s.scan_count > 0",
            true, 0);

        n = SPI_processed;

        tables = (char (*)[NAMEDATALEN]) MemoryContextAlloc(TopMemoryContext,sizeof(*tables) * (n > 0 ? n : 1));
        cols   = (char (*)[NAMEDATALEN])MemoryContextAlloc(TopMemoryContext,sizeof(*cols) * (n > 0 ? n : 1));

        memset(tables,0,sizeof(*tables) * (n > 0 ? n : 1));
        memset(cols,0,sizeof(*cols)* (n > 0 ? n : 1));

        for (j = 0; j < n; j++){
            double score;
            char  *tv;
            char  *cv;

            score = score_candidate(SPI_tuptable->vals[j],SPI_tuptable->tupdesc);

            tv = SPI_getvalue(SPI_tuptable->vals[j],
                              SPI_tuptable->tupdesc, 8);
            cv = SPI_getvalue(SPI_tuptable->vals[j],
                              SPI_tuptable->tupdesc, 9);

            elog(LOG,"AUTO_INDEX: candidate %s(%s) score=%.2f threshold=%.2f",tv ? tv : "?", cv ? cv : "?",score, (double) AI_SCORE_THRESHOLD);

            if (score >= AI_SCORE_THRESHOLD){
                strlcpy(tables[j], tv ? tv : "", NAMEDATALEN);
                strlcpy(cols[j],   cv ? cv : "", NAMEDATALEN);
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();

        for (j = 0; j < n; j++){
            StringInfoData buf;

            if (tables[j][0] == '\0' || cols[j][0] == '\0')
                continue;

            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());

            initStringInfo(&buf);
            appendStringInfo(&buf,
                "CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
                quote_identifier(psprintf("idx_%s_%s",
                                          tables[j], cols[j])),
                quote_identifier(tables[j]),
                quote_identifier(cols[j]));

            elog(LOG, "AUTO_INDEX: %s", buf.data);

            SPI_execute(buf.data, false, 0);
            pfree(buf.data);

            values[0] = CStringGetTextDatum(tables[j]);
            values[1] = CStringGetTextDatum(cols[j]);

            SPI_execute_with_args(
                "DELETE FROM column_scan_stats "
                "WHERE table_name = $1 AND column_name = $2",
                2, argtypes, values, nulls,
                false, 0);

            elog(LOG, "AUTO_INDEX: removed %s(%s) from column_scan_stats",
                 tables[j], cols[j]);

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }

        pfree(tables);
        pfree(cols);


        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        SPI_execute(
            "UPDATE column_scan_stats "
            "SET scan_count = scan_count * " STR(AI_DECAY_FACTOR) " "
            "WHERE last_seen < now() - interval '15 seconds'",
            false, 0);
        
        SPI_execute(
            "UPDATE write_stats "
            "SET write_count = write_count * " STR(AI_DECAY_FACTOR) " "
            "WHERE last_seen < now() - interval '15 seconds'",
            false, 0);

        SPI_execute(
            "DELETE FROM column_scan_stats "
            "WHERE scan_count < 1",
            false, 0);

        SPI_execute(
            "DELETE FROM column_scan_stats "
            "WHERE last_seen < now() - interval '"
                STR(AI_MAX_AGE_DAYS) " days'",
            false, 0);

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();

        rc = WaitLatch(MyLatch,
                       WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       10000L,
                       PG_WAIT_EXTENSION);

        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        ResetLatch(MyLatch);
    }
}

void _PG_init(void){
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("auto_index must be loaded via "
                        "shared_preload_libraries")));

    prev_shmem_request = shmem_request_hook;
    shmem_request_hook = ai_shmem_request;

    prev_shmem_startup = shmem_startup_hook;
    shmem_startup_hook = ai_shmem_startup;

    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = auto_index_executor_run;

    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = auto_index_executor_end;

    memset(&worker, 0, sizeof(worker));

    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS |
                              BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;

    snprintf(worker.bgw_name,          BGW_MAXLEN, "auto_index worker");
    snprintf(worker.bgw_type,          BGW_MAXLEN, "auto_index");
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "auto_index");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_index_worker_main");

    RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
    ExecutorRun_hook = prev_ExecutorRun;
    ExecutorEnd_hook = prev_ExecutorEnd;
}