/*
 * auto_index.c
 * PostgreSQL extension with background worker and cost-based heuristic.
 *
 * Architecture:
 *
 *   Executor hooks (run in every backend process)
 *   -----------------------------------------------
 *   Walk the plan tree, detect SeqScans on filtered columns, write
 *   entries into a fixed-size shared memory ring buffer (LWLock protected).
 *   No SPI, no transactions — returns immediately.
 *
 *   Background worker
 *   -----------------
 *   Wakes every 10 s:
 *     Phase 1 — drain shared queue, upsert into column_scan_stats.
 *     Phase 2 — score candidates via cost-based heuristic,
 *               create index if score >= AI_SCORE_THRESHOLD.
 *     Phase 3 — decay all scan counts by AI_DECAY_FACTOR each cycle,
 *               delete rows with scan_count < 1 (effectively zero),
 *               delete rows not seen for more than AI_MAX_AGE_DAYS days.
 *
 * Decay model
 * -----------
 *   scan_count *= AI_DECAY_FACTOR   (default 0.9, applied every 10 s cycle)
 *
 *   A column scanned once and never again decays to < 1 after ~21 cycles
 *   (~3.5 minutes with default settings).  A column scanned steadily every
 *   cycle stays relevant.  Recent activity always outweighs old history.
 *
 *   Hard expiry (AI_MAX_AGE_DAYS) ensures rows for tables/columns that
 *   no longer exist or are permanently cold get cleaned up regardless.
 *
 * Cost-based scoring
 * ------------------
 *   benefit = scan_count * seq_page_cost * relpages
 *             * (1 - selectivity)
 *             * (0.5 + 0.5*|correlation|)
 *   penalty = write_count * random_page_cost * 0.1
 *   score   = benefit - penalty
 *
 * One-time superuser setup
 * ------------------------
 *   shared_preload_libraries = 'auto_index'   -- postgresql.conf, restart
 *
 *   CREATE TABLE column_scan_stats (
 *       table_name  TEXT        NOT NULL,
 *       column_name TEXT        NOT NULL,
 *       scan_count  FLOAT8      NOT NULL DEFAULT 0,
 *       last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
 *       PRIMARY KEY (table_name, column_name)
 *   );
 *
 *   Note: scan_count is FLOAT8 (not INT) because decay produces fractions.
 */

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

/* stringify helper for embedding #define values in SQL strings */
#define STR_INNER(x) #x
#define STR(x)       STR_INNER(x)

PG_MODULE_MAGIC;

/* ----------------------------------------------------------------
 * Tunables
 * ---------------------------------------------------------------- */

/*
 * Minimum score before creating an index.
 * Raise to be more conservative; lower to index more aggressively.
 */
#define AI_SCORE_THRESHOLD  50.0   /* lower = more aggressive indexing */

/*
 * Minimum table size (rows).  The planner prefers seq scan on tiny tables.
 */
#define AI_MIN_ROWS  1000.0

/*
 * Decay factor applied to scan_count on every bgworker cycle.
 * 0.9 → a count of 100 halves in ~6.6 cycles (~66 s with 10 s sleep).
 * Must be in (0, 1).
 */
#define AI_DECAY_FACTOR  0.9

/*
 * Hard expiry: delete rows whose last_seen is older than this many days.
 * Catches tables/columns that are permanently gone or cold.
 */
#define AI_MAX_AGE_DAYS  7

/* Ring buffer capacity */
#define AI_QUEUE_SIZE  256

/* Max columns collected per SeqScan qual */
#define AI_MAX_COLS_PER_SCAN  16

/* ----------------------------------------------------------------
 * Shared memory queue
 * ---------------------------------------------------------------- */

typedef struct ShmEntry
{
    char table[NAMEDATALEN];
    char column[NAMEDATALEN];
} ShmEntry;

typedef struct AutoIndexShmem
{
    int      head;
    int      tail;
    ShmEntry entries[AI_QUEUE_SIZE];
} AutoIndexShmem;

static AutoIndexShmem *ai_shm  = NULL;
static LWLock         *ai_lock = NULL;

/* ----------------------------------------------------------------
 * Globals
 * ---------------------------------------------------------------- */

static ExecutorRun_hook_type   prev_ExecutorRun   = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEnd   = NULL;
static shmem_request_hook_type prev_shmem_request = NULL;
static shmem_startup_hook_type prev_shmem_startup = NULL;

/* ----------------------------------------------------------------
 * Shared memory helpers
 * ---------------------------------------------------------------- */

static Size
ai_shmem_size(void)
{
    return sizeof(AutoIndexShmem);
}

static void
ai_shmem_request(void)
{
    if (prev_shmem_request)
        prev_shmem_request();

    RequestAddinShmemSpace(ai_shmem_size());
    RequestNamedLWLockTranche("auto_index", 1);
}

static void
ai_shmem_startup(void)
{
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

/* ----------------------------------------------------------------
 * Internal table skip-list
 * Tables the bgworker itself touches — never collect stats on these.
 * ---------------------------------------------------------------- */

static const char * const ai_internal_tables[] = {
    "column_scan_stats",
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

/* ----------------------------------------------------------------
 * Plan walker — collects ALL filtered columns from a SeqScan qual
 * ---------------------------------------------------------------- */

typedef struct
{
    Oid   relid;
    int   ncols;
    char *columns[AI_MAX_COLS_PER_SCAN];
} QualCtx;

static bool
find_var(Node *node, void *context)
{
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
                        return false;   /* duplicate — skip */

                ctx->columns[ctx->ncols++] = colname;
            }
        }
        return false;   /* always continue walking */
    }

    return expression_tree_walker(node, find_var, context);
}

static void
walk_plan(Plan *plan, PlannedStmt *pstmt)
{
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

                        if (next_tail != ai_shm->head)
                        {
                            strlcpy(ai_shm->entries[ai_shm->tail].table,
                                    tname, NAMEDATALEN);
                            strlcpy(ai_shm->entries[ai_shm->tail].column,
                                    ctx.columns[k], NAMEDATALEN);
                            ai_shm->tail = next_tail;
                        }
                        else
                        {
                            elog(LOG,
                                 "AUTO_INDEX: queue full, dropping %s(%s)",
                                 tname, ctx.columns[k]);
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

/* ----------------------------------------------------------------
 * Executor hooks — no SPI, no transactions, no blocking
 * ---------------------------------------------------------------- */

void
auto_index_executor_run(QueryDesc    *qd,
                        ScanDirection dir,
                        uint64        count,
                        bool          once)
{
    if (prev_ExecutorRun)
        prev_ExecutorRun(qd, dir, count, once);
    else
        standard_ExecutorRun(qd, dir, count, once);

    if (!qd || !qd->plannedstmt || !ai_shm)
        return;

    walk_plan(qd->plannedstmt->planTree, qd->plannedstmt);
}

void
auto_index_executor_end(QueryDesc *qd)
{
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(qd);
    else
        standard_ExecutorEnd(qd);
}

/* ----------------------------------------------------------------
 * Cost-based scoring
 *
 * Columns passed from the scoring SELECT (1-based):
 *   1  scan_count   FLOAT8
 *   2  seq_cost     FLOAT8
 *   3  selectivity  FLOAT8
 *   4  correlation  FLOAT8
 *   5  write_count  FLOAT8
 *   6  rnd_cost     FLOAT8
 *   7  reltuples    FLOAT8
 * ---------------------------------------------------------------- */

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

    /* index not useful if it would return > 20% of table */
    if (selectivity > 0.20)
        return 0.0;

    benefit = scan_count
              * seq_cost
              * (1.0 - selectivity)
              * (0.5 + 0.5 * correlation);

    penalty = write_count * rnd_cost * 0.1;

    return benefit - penalty;
}

/* ----------------------------------------------------------------
 * Background worker
 * ---------------------------------------------------------------- */

PGDLLEXPORT void auto_index_worker_main(Datum arg);

void
auto_index_worker_main(Datum arg)
{
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

    for (;;)
    {
        /* ----------------------------------------------------------------
         * Phase 1a: snapshot ring buffer under LWLock.
         * ---------------------------------------------------------------- */
        drain_count = 0;

        LWLockAcquire(ai_lock, LW_EXCLUSIVE);
        while (ai_shm->head != ai_shm->tail && drain_count < AI_QUEUE_SIZE)
        {
            drain[drain_count++] = ai_shm->entries[ai_shm->head];
            ai_shm->head = (ai_shm->head + 1) % AI_QUEUE_SIZE;
        }
        LWLockRelease(ai_lock);

        /* ----------------------------------------------------------------
         * Phase 1b: upsert drained entries.
         *
         * scan_count += 1 on conflict.
         * last_seen  = now() on every touch so the decay/expiry logic
         *              knows this column is still active.
         * ---------------------------------------------------------------- */
        if (drain_count > 0)
        {
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());

            for (i = 0; i < drain_count; i++)
            {
                elog(LOG, "AUTO_INDEX: recording SeqScan on %s(%s)",
                     drain[i].table, drain[i].column);

                values[0] = CStringGetTextDatum(drain[i].table);
                values[1] = CStringGetTextDatum(drain[i].column);

                SPI_execute_with_args(
                    "INSERT INTO column_scan_stats"
                    "    (table_name, column_name, scan_count, last_seen) "
                    "VALUES ($1, $2, 1, now()) "
                    "ON CONFLICT (table_name, column_name) "
                    "DO UPDATE SET"
                    "    scan_count = column_scan_stats.scan_count + 1,"
                    "    last_seen  = now()",
                    2, argtypes, values, nulls,
                    false, 0);
            }

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }

        /* ----------------------------------------------------------------
         * Phase 2: score candidates and create indexes.
         * ---------------------------------------------------------------- */
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
            "       COALESCE(psu.n_tup_ins + psu.n_tup_upd +"
            "                psu.n_tup_del, 0)::float8 AS write_count,"
            "       current_setting('random_page_cost')::float8 AS rnd_cost,"
            "       pc.reltuples::float8,"
            "       s.table_name,"
            "       s.column_name "
            "FROM   column_scan_stats s "
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

        tables = (char (*)[NAMEDATALEN])
            MemoryContextAlloc(TopMemoryContext,
                               sizeof(*tables) * (n > 0 ? n : 1));
        cols   = (char (*)[NAMEDATALEN])
            MemoryContextAlloc(TopMemoryContext,
                               sizeof(*cols)   * (n > 0 ? n : 1));

        memset(tables, 0, sizeof(*tables) * (n > 0 ? n : 1));
        memset(cols,   0, sizeof(*cols)   * (n > 0 ? n : 1));

        for (j = 0; j < n; j++)
        {
            double score;
            char  *tv;
            char  *cv;

            score = score_candidate(SPI_tuptable->vals[j],
                                    SPI_tuptable->tupdesc);

            tv = SPI_getvalue(SPI_tuptable->vals[j],
                              SPI_tuptable->tupdesc, 8);
            cv = SPI_getvalue(SPI_tuptable->vals[j],
                              SPI_tuptable->tupdesc, 9);

            elog(LOG,
                 "AUTO_INDEX: candidate %s(%s) score=%.2f threshold=%.2f",
                 tv ? tv : "?", cv ? cv : "?",
                 score, (double) AI_SCORE_THRESHOLD);

            if (score >= AI_SCORE_THRESHOLD)
            {
                strlcpy(tables[j], tv ? tv : "", NAMEDATALEN);
                strlcpy(cols[j],   cv ? cv : "", NAMEDATALEN);
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();

        /* create one index per transaction */
        for (j = 0; j < n; j++)
        {
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

            /*
             * Remove the row from column_scan_stats now that the index
             * exists.  The NOT EXISTS guard in the scoring query would
             * have filtered it out next cycle anyway, but deleting it
             * immediately keeps the table clean and avoids the decay
             * logic wasting work on a column that is already indexed.
             */
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

        /* ----------------------------------------------------------------
         * Phase 3: decay + expiry.
         *
         * 3a. Multiply every scan_count by AI_DECAY_FACTOR.
         *     This gives recent scans more weight than old ones.
         *     A count of 100 with no new scans will reach < 1 in
         *     ceil(log(1/100) / log(0.9)) = 44 cycles (~7 min at 10 s).
         *
         * 3b. Delete rows whose decayed count fell below 1.
         *     They contribute nothing to scoring and just waste space.
         *
         * 3c. Delete rows not seen for more than AI_MAX_AGE_DAYS days.
         *     Hard expiry for tables/columns that are permanently gone
         *     or cold — they would never accumulate enough count to
         *     trigger decay-deletion on their own if scan_count was
         *     left at a small but non-zero value by repeated slow decay.
         * ---------------------------------------------------------------- */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        /* 3a: decay — only rows not touched in this cycle.
         * Rows updated in the current cycle (last_seen >= now() - 15s)
         * are skipped so a freshly inserted row with scan_count=1
         * is not immediately decayed to 0.9 and deleted.
         * The 15s window is slightly larger than the 10s sleep to
         * account for transaction execution time. */
        SPI_execute(
            "UPDATE column_scan_stats "
            "SET scan_count = scan_count * " STR(AI_DECAY_FACTOR) " "
            "WHERE last_seen < now() - interval '15 seconds'",
            false, 0);

        /* 3b: remove effectively-zero rows */
        SPI_execute(
            "DELETE FROM column_scan_stats "
            "WHERE scan_count < 1",
            false, 0);

        /* 3c: hard expiry */
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

/* ----------------------------------------------------------------
 * Init / fini
 * ---------------------------------------------------------------- */

void
_PG_init(void)
{
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