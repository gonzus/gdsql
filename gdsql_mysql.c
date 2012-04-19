#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include <gdsql_date.h>
#include <gdsql_log.h>
#include <gdsql_hidden.h>
#include <gdsql_util.h>

#define DBNAME "MySQL"

typedef struct DbData {
    MYSQL* db;
} DbData;

#define STMT_MAX_PARAMS        100
#define STMT_MAX_PARAM_LENGTH   50

typedef struct Param {
    char buf[STMT_MAX_PARAMS][STMT_MAX_PARAM_LENGTH+1];
    unsigned long len[STMT_MAX_PARAMS];
    MYSQL_BIND bind[STMT_MAX_PARAMS];
    int next;
} Param;

#define STMT_MAX_RESULTS        100
#define STMT_MAX_RESULT_LENGTH   50

typedef struct TimeColResult {
    MYSQL_TIME stamp;
    double* result;
} TimeColResult;

typedef union ColResult {
    TimeColResult time;
} ColResult;

typedef struct Result {
    my_bool null[STMT_MAX_RESULTS];
    unsigned long len[STMT_MAX_RESULTS];
    my_bool error[STMT_MAX_RESULTS];
    MYSQL_BIND bind[STMT_MAX_RESULTS];
    ColResult colres[STMT_MAX_RESULTS];
    char pos[STMT_MAX_RESULTS];
    int next;
} Result;

typedef struct StmtData {
    MYSQL_STMT* ps;
    Param param;
    Result result;
} StmtData;

static int gdsql_mysql_init(void);
static int gdsql_mysql_fini(void);

static int gdsql_mysql_db_alloc(void);
static int gdsql_mysql_db_free(void);

static int gdsql_mysql_db_open(gdsql_dbh* db);
static int gdsql_mysql_db_close(gdsql_dbh* db);

static int gdsql_mysql_stmt_create(gdsql_stmth* stmt);
static int gdsql_mysql_stmt_prepare(gdsql_stmth* stmt);

static int gdsql_mysql_stmt_bindp_null(gdsql_stmth* stmt,
                                       int pos);
static int gdsql_mysql_stmt_bindp_int(gdsql_stmth* stmt,
                                      int pos,
                                      int val);
static int gdsql_mysql_stmt_bindp_double(gdsql_stmth* stmt,
                                         int pos,
                                         double val);
static int gdsql_mysql_stmt_bindp_string(gdsql_stmth* stmt,
                                         int pos,
                                         const char* val,
                                         int len);
static int gdsql_mysql_stmt_bindp_date(gdsql_stmth* stmt,
                                       int pos,
                                       double val);
static int gdsql_mysql_stmt_bindp_boolean(gdsql_stmth* stmt,
                                          int pos,
                                          int val);

static int gdsql_mysql_stmt_bindr_int(gdsql_stmth* stmt,
                                      int pos,
                                      int* var);
static int gdsql_mysql_stmt_bindr_double(gdsql_stmth* stmt,
                                         int pos,
                                         double* var);
static int gdsql_mysql_stmt_bindr_string(gdsql_stmth* stmt,
                                         int pos,
                                         char* var,
                                         int len);
static int gdsql_mysql_stmt_bindr_date(gdsql_stmth* stmt,
                                       int pos,
                                       double* var);
static int gdsql_mysql_stmt_bindr_boolean(gdsql_stmth* stmt,
                                          int pos,
                                          int* var);

static int gdsql_mysql_stmt_step(gdsql_stmth* stmt);
static int gdsql_mysql_stmt_is_column_null(gdsql_stmth* stmt,
                                           int pos);
static int gdsql_mysql_stmt_finalize(gdsql_stmth* stmt);


int gdsql_mysql_boot(void)
{
    static DbOps ops = {
        gdsql_mysql_init,
        gdsql_mysql_fini,
        gdsql_mysql_db_alloc,
        gdsql_mysql_db_free,
        gdsql_mysql_db_open,
        gdsql_mysql_db_close,
        gdsql_mysql_stmt_create,
        gdsql_mysql_stmt_prepare,
        gdsql_mysql_stmt_bindp_null,
        gdsql_mysql_stmt_bindp_int,
        gdsql_mysql_stmt_bindp_double,
        gdsql_mysql_stmt_bindp_string,
        gdsql_mysql_stmt_bindp_date,
        gdsql_mysql_stmt_bindp_boolean,
        gdsql_mysql_stmt_bindr_int,
        gdsql_mysql_stmt_bindr_double,
        gdsql_mysql_stmt_bindr_string,
        gdsql_mysql_stmt_bindr_date,
        gdsql_mysql_stmt_bindr_boolean,
        gdsql_mysql_stmt_step,
        gdsql_mysql_stmt_is_column_null,
        gdsql_mysql_stmt_finalize,
    };

    GDSQL_Log(LOG_INFO,
              ("%s: booting",
               DBNAME));
    set_dbops(GDSQL_DB_MYSQL, &ops);
    return 0;
}


static int gdsql_mysql_init(void)
{
    int ret = 0;
    
    GDSQL_Log(LOG_INFO,
              ("%s: initializing library",
               DBNAME));
    ret = mysql_library_init(0, 0, 0);

    return ret;
}

static int gdsql_mysql_fini(void)
{
    int ret = 0;
    
    GDSQL_Log(LOG_INFO,
              ("%s: terminating library",
               DBNAME));
    mysql_library_end();

    return ret;
}

static int gdsql_mysql_db_alloc(void)
{
    return 0;
}

static int gdsql_mysql_db_free(void)
{
    return 0;
}

static int gdsql_mysql_db_open(gdsql_dbh* db)
{
    db->data = 0;
    
    GDSQL_Log(LOG_INFO,
              ("%s: creating database object",
               DBNAME));
    MYSQL* sql_db;
    sql_db = mysql_init(0);
    if (sql_db == 0)
        return 1;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    DbData* ddata = (DbData*) malloc(sizeof(DbData));
    ddata->db = sql_db;
    db->data = ddata;

    GDSQL_Log(LOG_INFO,
              ("%s: Opening connection to [%s:%hd:%s:%s:%s]",
               DBNAME, db->host, db->port, db->name, db->user, db->password));
    if (mysql_real_connect(ddata->db,
                           db->host,
                           db->user,
                           db->password,
                           db->name,
                           db->port,
                           0,
                           0) == 0) {
        GDSQL_Log(LOG_WARNING,
                  ("%s: Failed to connect to database: %s",
                   DBNAME, mysql_error(ddata->db)));
        return 2;
    }
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_db_close(gdsql_dbh* db)
{
    DbData* ddata = (DbData*) db->data;
    int ret = 0;

    do {
        if (ddata == 0) {
            ret = 1;
            break;
        }

        do {
            if (ddata->db == 0) {
                ret = 2;
                break;
            }

            GDSQL_Log(LOG_INFO,
                      ("%s: closing connection to [%s]",
                       DBNAME, db->name));
            mysql_close(ddata->db);
            GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
        } while (0);

        free(ddata);
        db->data = 0;
    } while (0);

    return ret;
}

static int gdsql_mysql_stmt_create(gdsql_stmth* stmt)
{
    if (stmt->gdsql_db == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: creating statement",
               DBNAME));
    StmtData* sdata = (StmtData*) malloc(sizeof(StmtData));
    memset(sdata, 0, sizeof(StmtData));
    memset(sdata->result.pos, -1, STMT_MAX_RESULTS);
    stmt->data = sdata;
    return 0;
}

static int gdsql_mysql_stmt_prepare(gdsql_stmth* stmt)
{
    if (stmt->gdsql_db == 0)
        return 1;
    
    DbData* ddata = (DbData*) stmt->gdsql_db->data;
    if (ddata == 0)
        return 2;
    if (ddata->db == 0)
        return 3;
    
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 4;

    GDSQL_Log(LOG_INFO,
              ("%s: creating statement",
               DBNAME));
    MYSQL_STMT* sql_ps = 0;
    sql_ps = mysql_stmt_init(ddata->db);
    if (sql_ps == 0)
        return 5;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
    
    GDSQL_Log(LOG_INFO,
              ("%s: preparing statement [%s]",
               DBNAME, stmt->query));
    unsigned long len = strlen(stmt->query);
    if (mysql_stmt_prepare(sql_ps,
                           stmt->query,
                           len) != 0)
        return 6;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
    
    sdata->ps = sql_ps;
    return 0;
}

static int gdsql_mysql_stmt_bindp_null(gdsql_stmth* stmt,
                                       int pos)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Param* param = &sdata->param;
    if (param->next >= STMT_MAX_PARAMS)
        return 3;

    GDSQL_Log(LOG_INFO,
              ("%s: binding NULL param pos %d",
               DBNAME, pos));

    memset(&param->bind[param->next], 0, sizeof(MYSQL_BIND));
    param->bind[param->next].buffer_type = MYSQL_TYPE_NULL;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindp_int(gdsql_stmth* stmt,
                                      int pos,
                                      int val)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Param* param = &sdata->param;
    if (param->next >= STMT_MAX_PARAMS)
        return 3;

    GDSQL_Log(LOG_INFO,
              ("%s: binding int param pos %d to %d",
               DBNAME, pos, val));
    int* ip = (int*) param->buf[param->next];
    *ip = val;

    memset(&param->bind[param->next], 0, sizeof(MYSQL_BIND));
    param->bind[param->next].buffer_type = MYSQL_TYPE_LONG;
    param->bind[param->next].buffer = param->buf[param->next];
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindp_double(gdsql_stmth* stmt,
                                         int pos,
                                         double val)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Param* param = &sdata->param;
    if (param->next >= STMT_MAX_PARAMS)
        return 3;

    GDSQL_Log(LOG_INFO,
              ("%s: binding double param pos %d to %lf",
               DBNAME, pos, val));
    double* dp = (double*) param->buf[param->next];
    *dp = val;

    memset(&param->bind[param->next], 0, sizeof(MYSQL_BIND));
    param->bind[param->next].buffer_type = MYSQL_TYPE_DOUBLE;
    param->bind[param->next].buffer = param->buf[param->next];
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindp_string(gdsql_stmth* stmt,
                                         int pos,
                                         const char* val,
                                         int len)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Param* param = &sdata->param;
    if (param->next >= STMT_MAX_PARAMS)
        return 3;

    if (len < 0)
        len = strlen(val);
    if (len >= STMT_MAX_PARAM_LENGTH)
        return 4;

    GDSQL_Log(LOG_INFO,
              ("%s: binding string param pos %d to [%d:%s]",
               DBNAME, pos, len, val));

    memset(&param->bind[param->next], 0, sizeof(MYSQL_BIND));
    memcpy(param->buf[param->next], val, len);
    param->len[param->next] = len;
    param->bind[param->next].buffer_type = MYSQL_TYPE_STRING;
    param->bind[param->next].buffer = param->buf[param->next];
    param->bind[param->next].length = &param->len[param->next];
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindp_date(gdsql_stmth* stmt,
                                       int pos,
                                       double val)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Param* param = &sdata->param;
    if (param->next >= STMT_MAX_PARAMS)
        return 3;

    int Y, M, D;
    int h, m, s;
    gdsql_jul2cal(val, &Y, &M, &D, &h, &m, &s);
    GDSQL_Log(LOG_INFO,
              ("%s: binding date param pos %d to %lf = %04d/%02d/%02d %02d:%02d:%02d",
               DBNAME, pos, val,
               Y, M, D, h, m, s));

    MYSQL_TIME* ts = (MYSQL_TIME*) param->buf[param->next];
    ts->year = Y;
    ts->month = M;
    ts->day = D;
    ts->hour = h;
    ts->minute = m;
    ts->second = s;
    
    memset(&param->bind[param->next], 0, sizeof(MYSQL_BIND));
    param->bind[param->next].buffer_type = MYSQL_TYPE_TIMESTAMP;
    param->bind[param->next].buffer = param->buf[param->next];
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindp_boolean(gdsql_stmth* stmt,
                                          int pos,
                                          int val)
{
    return gdsql_mysql_stmt_bindp_int(stmt, pos, val);
}

static int gdsql_mysql_stmt_bindr_int(gdsql_stmth* stmt,
                                      int pos,
                                      int* var)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Result* result = &sdata->result;
    if (result->next >= STMT_MAX_RESULTS)
        return 3;

    if (result->pos[result->next] >= 0)
        return 4;
    result->pos[result->next] = --pos;

    GDSQL_Log(LOG_INFO,
              ("%s: binding int result pos %d to %p",
               DBNAME, pos, var));

    memset(&result->bind[result->next], 0, sizeof(MYSQL_BIND));
    result->bind[result->next].buffer_type = MYSQL_TYPE_LONG;
    result->bind[result->next].buffer = (char*) var;
    result->bind[result->next].is_null = &result->null[result->next];
    result->bind[result->next].length = &result->len[result->next];
    result->bind[result->next].error = &result->error[result->next];
    ++result->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindr_double(gdsql_stmth* stmt,
                                         int pos,
                                         double* var)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Result* result = &sdata->result;
    if (result->next >= STMT_MAX_RESULTS)
        return 3;

    if (result->pos[result->next] >= 0)
        return 4;
    result->pos[result->next] = --pos;

    GDSQL_Log(LOG_INFO,
              ("%s: binding double result pos %d to %p",
               DBNAME, pos, var));

    memset(&result->bind[result->next], 0, sizeof(MYSQL_BIND));
    result->bind[result->next].buffer_type = MYSQL_TYPE_DOUBLE;
    result->bind[result->next].buffer = (char*) var;
    result->bind[result->next].is_null = &result->null[result->next];
    result->bind[result->next].length = &result->len[result->next];
    result->bind[result->next].error = &result->error[result->next];
    ++result->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindr_string(gdsql_stmth* stmt,
                                         int pos,
                                         char* var,
                                         int len)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Result* result = &sdata->result;
    if (result->next >= STMT_MAX_RESULTS)
        return 3;

    if (result->pos[result->next] >= 0)
        return 4;
    result->pos[result->next] = --pos;

    GDSQL_Log(LOG_INFO,
              ("%s: binding string result pos %d to [%d:%p]",
               DBNAME, pos, len, var));

    memset(&result->bind[result->next], 0, sizeof(MYSQL_BIND));
    result->bind[result->next].buffer_type = MYSQL_TYPE_STRING;
    result->bind[result->next].buffer = (char*) var;
    result->bind[result->next].buffer_length = len;
    result->bind[result->next].is_null = &result->null[result->next];
    result->bind[result->next].length = &result->len[result->next];
    result->bind[result->next].error = &result->error[result->next];
    ++result->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindr_date(gdsql_stmth* stmt,
                                       int pos,
                                       double* var)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Result* result = &sdata->result;
    if (result->next >= STMT_MAX_RESULTS)
        return 3;

    if (result->pos[result->next] >= 0)
        return 4;
    result->pos[result->next] = --pos;

    GDSQL_Log(LOG_INFO,
              ("%s: binding date result pos %d to %p",
               DBNAME, pos, var));

    memset(&result->bind[result->next], 0, sizeof(MYSQL_BIND));
    result->bind[result->next].buffer_type = MYSQL_TYPE_TIMESTAMP;
    result->bind[result->next].buffer = (char*) &result->colres[result->next].time.stamp;
    result->colres[result->next].time.result = var;
    result->bind[result->next].is_null = &result->null[result->next];
    result->bind[result->next].length = &result->len[result->next];
    result->bind[result->next].error = &result->error[result->next];
    ++result->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_mysql_stmt_bindr_boolean(gdsql_stmth* stmt,
                                          int pos,
                                          int* var)
{
    return gdsql_mysql_stmt_bindr_int(stmt, pos, var);
}

static int gdsql_mysql_stmt_step(gdsql_stmth* stmt)
{
    StmtData* sdata = (StmtData*) stmt->data;

    if (sdata == 0)
        return 1;

    Result* result = &sdata->result;

    if (stmt->state < STMT_STATE_PREPARED) {
        // Must prepare statement
        if (gdsql_mysql_stmt_prepare(stmt) != 0)
            return 2;
        
        stmt->state = STMT_STATE_PREPARED;
    }
    
    if (stmt->state < STMT_STATE_BOUNDP) {
        if (sdata->param.next > 0) {
            // Must call bind for params
        
            GDSQL_Log(LOG_INFO,
                      ("%s: binding params",
                       DBNAME));
            if (mysql_stmt_bind_param(sdata->ps,
                                      sdata->param.bind) != 0)
                return 3;
        }

        stmt->state = STMT_STATE_BOUNDP;
    }

    if (stmt->state < STMT_STATE_BOUNDR) {
        if (result->next > 0) {
            // Must call bind for results
        
            GDSQL_Log(LOG_INFO,
                      ("%s: binding results",
                       DBNAME));
            if (mysql_stmt_bind_result(sdata->ps,
                                       result->bind) != 0)
                return 4;
        }

        stmt->state = STMT_STATE_BOUNDR;
    }

    if (stmt->state < STMT_STATE_EXECUTED) {
        // Must call execute
        
        GDSQL_Log(LOG_INFO,
                  ("%s: executing [%s]",
                   DBNAME, stmt->query));
        if (mysql_stmt_execute(sdata->ps) != 0)
            return 5;

        stmt->state = STMT_STATE_EXECUTED;
    }
    
    if (stmt->state < STMT_STATE_EXHAUSTED) {
        // Not yet done, can call fetch
        GDSQL_Log(LOG_INFO,
                  ("%s: stepping statement [%s]",
                   DBNAME, stmt->query));
        int st = mysql_stmt_fetch(sdata->ps);
        if (st != 0) {
            GDSQL_Log(LOG_INFO,
                      ("%s: failed to step: %d (%d / %d) - %s",
                       DBNAME, st, MYSQL_NO_DATA, MYSQL_DATA_TRUNCATED,
                       mysql_stmt_error(sdata->ps)));
            stmt->state = STMT_STATE_EXHAUSTED;
            return 6;
        }

        int j = 0;
        for (j = 0; j < result->next; ++j) {
            if (result->bind[j].buffer_type != MYSQL_TYPE_TIMESTAMP)
                continue;
        
            GDSQL_Log(LOG_INFO,
                      ("%s: generating date result for col %d",
                       DBNAME, j));
            MYSQL_TIME* ts = (MYSQL_TIME*) &result->colres[j].time.stamp;
            double* dp = result->colres[j].time.result;
            *dp = gdsql_cal2jul(ts->year, ts->month, ts->day,
                                ts->hour, ts->minute, ts->second);
        }
    }
    
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
    return 0;
}

static int gdsql_mysql_stmt_is_column_null(gdsql_stmth* stmt,
                                           int pos)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 0;
    
    --pos;
    Result* result = &sdata->result;
    int j = 0;
    for (j = 0; j < result->next; ++j) {
        int p = result->pos[j];
        if (p != pos)
            continue;
        return result->null[j];
    }

    return 0;
}

static int gdsql_mysql_stmt_finalize(gdsql_stmth* stmt)
{
    StmtData* sdata = (StmtData*) stmt->data;
    int ret = 0;

    do {
        if (sdata == 0) {
            ret = 1;
            break;
        }

        do {
            if (sdata->ps == 0) {
                ret = 2;
                break;
            }

            GDSQL_Log(LOG_INFO,
                      ("%s: finalizing mysql3 statement [%s]",
                       DBNAME, stmt->query));
            if (mysql_stmt_close(sdata->ps) != 0) {
                ret = 3;
                break;
            }
            GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
        } while (0);

        free(sdata);
        stmt->data = 0;
    } while (0);

    return ret;
}
