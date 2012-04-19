#include <endian.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <libpq-fe.h>
#include <gdsql_log.h>
#include <gdsql_hidden.h>
#include <gdsql_date.h>
#include <gdsql_util.h>

#define DBNAME "Postgres"

#define SECONDS_PER_DAY 86400.0
#define MICROSECONDS_PER_SECOND 1000000.0
#define JULIAN_20000101 2451545.0

/*
 * Define some size-specific integer types.
 */
typedef char int8;
typedef short int16;
typedef int int32;
typedef long long int64;

typedef struct DbData {
    PGconn* db;
} DbData;


#define STMT_MAX_PARAMS        100
#define STMT_MAX_PARAM_LENGTH   50

typedef struct Param {
    char buf[STMT_MAX_PARAMS][STMT_MAX_PARAM_LENGTH+1];
    const char *val[STMT_MAX_PARAMS];
    int len[STMT_MAX_PARAMS];
    int bin[STMT_MAX_PARAMS];
    int next;
} Param;

typedef struct Cursor {
    int rows;
    int cols;
    int next;
    Row row;
} Cursor;

typedef struct StmtData {
    PGresult* result;
    Param param;
    Cursor cursor;
} StmtData;

static int gdsql_postgres_init(void);
static int gdsql_postgres_fini(void);

static int gdsql_postgres_db_alloc(void);
static int gdsql_postgres_db_free(void);

static int gdsql_postgres_db_open(gdsql_dbh* db);
static int gdsql_postgres_db_close(gdsql_dbh* db);

static int gdsql_postgres_stmt_create(gdsql_stmth* stmt);
static int gdsql_postgres_stmt_prepare(gdsql_stmth* stmt);

static int gdsql_postgres_stmt_bindp_null(gdsql_stmth* stmt,
                                          int pos);
static int gdsql_postgres_stmt_bindp_int(gdsql_stmth* stmt,
                                         int pos,
                                         int val);
static int gdsql_postgres_stmt_bindp_double(gdsql_stmth* stmt,
                                            int pos,
                                            double val);
static int gdsql_postgres_stmt_bindp_string(gdsql_stmth* stmt,
                                            int pos,
                                            const char* val,
                                            int len);
static int gdsql_postgres_stmt_bindp_date(gdsql_stmth* stmt,
                                          int pos,
                                          double val);
static int gdsql_postgres_stmt_bindp_boolean(gdsql_stmth* stmt,
                                             int pos,
                                             int val);

static int gdsql_postgres_stmt_bindr_int(gdsql_stmth* stmt,
                                         int pos,
                                         int* var);
static int gdsql_postgres_stmt_bindr_double(gdsql_stmth* stmt,
                                            int pos,
                                            double* var);
static int gdsql_postgres_stmt_bindr_string(gdsql_stmth* stmt,
                                            int pos,
                                            char* var,
                                            int len);
static int gdsql_postgres_stmt_bindr_date(gdsql_stmth* stmt,
                                          int pos,
                                          double* var);
static int gdsql_postgres_stmt_bindr_boolean(gdsql_stmth* stmt,
                                             int pos,
                                             int* var);

static int gdsql_postgres_stmt_step(gdsql_stmth* stmt);
static int gdsql_postgres_stmt_is_column_null(gdsql_stmth* stmt,
                                              int pos);
static int gdsql_postgres_stmt_finalize(gdsql_stmth* stmt);

/*
 * Functions to get specific types from the query results.
 */
static int8 get_int8(const char* buf);
static int16 get_int16(const char* buf);
static int32 get_int32(const char* buf);
static int64 get_int64(const char* buf);
static double get_double(const char* buf);
static double get_date(const char* buf);

/*
 * Functions to associate specific types with the query parameters.
 */
static int put_int8(int8 val,
                    char* buf);
static int put_int16(int16 val,
                     char* buf);
static int put_int32(int32 val,
                     char* buf);
static int put_int64(int64 val,
                     char* buf);
static int put_double(double val,
                      char* buf);
static int put_date(double val,
                    char* buf);

int gdsql_postgres_boot(void)
{
    static DbOps ops = {
        gdsql_postgres_init,
        gdsql_postgres_fini,
        gdsql_postgres_db_alloc,
        gdsql_postgres_db_free,
        gdsql_postgres_db_open,
        gdsql_postgres_db_close,
        gdsql_postgres_stmt_create,
        gdsql_postgres_stmt_prepare,
        gdsql_postgres_stmt_bindp_null,
        gdsql_postgres_stmt_bindp_int,
        gdsql_postgres_stmt_bindp_double,
        gdsql_postgres_stmt_bindp_string,
        gdsql_postgres_stmt_bindp_date,
        gdsql_postgres_stmt_bindp_boolean,
        gdsql_postgres_stmt_bindr_int,
        gdsql_postgres_stmt_bindr_double,
        gdsql_postgres_stmt_bindr_string,
        gdsql_postgres_stmt_bindr_date,
        gdsql_postgres_stmt_bindr_boolean,
        gdsql_postgres_stmt_step,
        gdsql_postgres_stmt_is_column_null,
        gdsql_postgres_stmt_finalize,
    };

    GDSQL_Log(LOG_INFO,
              ("%s: booting", DBNAME));
    set_dbops(GDSQL_DB_POSTGRES, &ops);
    return 0;
}


static int gdsql_postgres_init(void)
{
    return 0;
}

static int gdsql_postgres_fini(void)
{
    return 0;
}

static int gdsql_postgres_db_alloc(void)
{
    return 0;
}

static int gdsql_postgres_db_free(void)
{
    return 0;
}

static int gdsql_postgres_db_open(gdsql_dbh* db)
{
    db->data = 0;

    char sp[10];
    sprintf(sp, "%hd", db->port);
    GDSQL_Log(LOG_INFO,
              ("%s: opening connection to [%s:%s:%s:%s:%s]",
               DBNAME, db->host, sp, db->name, db->user, db->password));
    PGconn* sql_db = PQsetdbLogin(db->host,
                                  sp,
                                  0,
                                  0,
                                  db->name,
                                  db->user,
                                  db->password);
    if (sql_db == 0)
        return 1;

    ConnStatusType st = PQstatus(sql_db);
    if (st != CONNECTION_OK)
        return 2;

    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
    
    DbData* data = (DbData*) malloc(sizeof(DbData));
    data->db = sql_db;
    db->data = data;
    return 0;
}

static int gdsql_postgres_db_close(gdsql_dbh* db)
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
            PQfinish(ddata->db);
            GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
        } while (0);

        free(ddata);
        db->data = 0;
    } while (0);

    return ret;
}

static int gdsql_postgres_stmt_create(gdsql_stmth* stmt)
{
    if (stmt->gdsql_db == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: creating statement",
               DBNAME));
    StmtData* sdata = (StmtData*) malloc(sizeof(StmtData));
    sdata->result = 0;
    sdata->param.next = 0;
    sdata->cursor.rows = 0;
    sdata->cursor.cols = 0;
    sdata->cursor.next = 0;
    sdata->cursor.row.ncol = 0;
    stmt->data = sdata;
    return 0;
}

static int gdsql_postgres_stmt_prepare(gdsql_stmth* stmt)
{
    if (stmt->gdsql_db == 0)
        return 1;
    
    DbData* ddata = (DbData*) stmt->gdsql_db->data;
    if (ddata == 0)
        return 3;
    if (ddata->db == 0)
        return 4;
    
    GDSQL_Log(LOG_INFO,
              ("%s: preparing statement [%s]",
               DBNAME, stmt->query));
    PGresult* sql_ps = PQprepare(ddata->db,
                                 "", // unnamed statement
                                 stmt->query,
                                 0,  // no params specified yet
                                 0);
    GDSQL_Log(LOG_INFO,
              ("%s: ps = %p",
               DBNAME, sql_ps));
    if (sql_ps == 0)
        return 5;
    
    ExecStatusType st = PQresultStatus(sql_ps);
    PQclear(sql_ps);
    GDSQL_Log(LOG_INFO,
              ("%s: st = %d (%d / %d)",
               DBNAME, (int) st,
               (int) PGRES_COMMAND_OK, (int) PGRES_TUPLES_OK));
    if (st != PGRES_COMMAND_OK &&
        st != PGRES_TUPLES_OK)
        return 6;

    GDSQL_Log(LOG_INFO, ("%s: success!", DBNAME));
    return 0;
}

static int gdsql_postgres_stmt_bindp_null(gdsql_stmth* stmt,
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
    param->val[param->next] = 0;
    param->len[param->next] = 0;
    param->bin[param->next] = 1;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindp_int(gdsql_stmth* stmt,
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
    param->len[param->next] = put_int32(val, param->buf[param->next]);
    param->val[param->next] = param->buf[param->next];
    param->bin[param->next] = 1;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindp_double(gdsql_stmth* stmt,
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

    param->len[param->next] = put_double(val, param->buf[param->next]);
    param->val[param->next] = param->buf[param->next];
    param->bin[param->next] = 1;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindp_string(gdsql_stmth* stmt,
                                            int pos,
                                            const char* val,
                                            int len)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Param* param = &sdata->param;
    if (param->next >= STMT_MAX_PARAMS)
        return 2;

    if (len < 0)
        len = strlen(val);
    if (len >= STMT_MAX_PARAM_LENGTH)
        return 3;

    GDSQL_Log(LOG_INFO,
              ("%s: binding string param pos %d to [%d:%s]",
               DBNAME, pos, len, val));

    param->len[param->next] = len;
    memcpy(param->buf[param->next], val, len);
    param->val[param->next] = param->buf[param->next];
    param->bin[param->next] = 1;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindp_date(gdsql_stmth* stmt,
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
              ("%s: binding date param pos %d to %lf",
               DBNAME, pos, val));

    param->len[param->next] = put_date(val, param->buf[param->next]);
    param->val[param->next] = param->buf[param->next];
    param->bin[param->next] = 1;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindp_boolean(gdsql_stmth* stmt,
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
              ("%s: binding boolean param pos %d to %d",
               DBNAME, pos, val));
    int8 b = (int8) val;
    param->len[param->next] = put_int8(b, param->buf[param->next]);
    param->val[param->next] = param->buf[param->next];
    param->bin[param->next] = 1;
    ++param->next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindr_int(gdsql_stmth* stmt,
                                         int pos,
                                         int* var)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Row* row = &sdata->cursor.row;
    if (row->ncol >= STMT_MAX_COLS)
        return 3;

    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding int result pos %d to %p",
               DBNAME, pos, var));
    row->cols[row->ncol].pos = pos;
    row->cols[row->ncol].type = STMT_VAL_INT;
    row->cols[row->ncol].val.ival = var;
    row->cols[row->ncol].len = 0;
    ++row->ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindr_double(gdsql_stmth* stmt,
                                            int pos,
                                            double* var)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Row* row = &sdata->cursor.row;
    if (row->ncol >= STMT_MAX_COLS)
        return 3;

    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding double result pos %d to %p",
               DBNAME, pos, var));
    row->cols[row->ncol].pos = pos;
    row->cols[row->ncol].type = STMT_VAL_DOUBLE;
    row->cols[row->ncol].val.dval = var;
    row->cols[row->ncol].len = 0;
    ++row->ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindr_string(gdsql_stmth* stmt,
                                            int pos,
                                            char* var,
                                            int len)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Row* row = &sdata->cursor.row;
    if (row->ncol >= STMT_MAX_COLS)
        return 3;

    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding string result pos %d to [%d:%p]",
               DBNAME, pos, len, var));
    row->cols[row->ncol].pos = pos;
    row->cols[row->ncol].type = STMT_VAL_STRING;
    row->cols[row->ncol].val.sval = var;
    row->cols[row->ncol].len = len;
    ++row->ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindr_date(gdsql_stmth* stmt,
                                          int pos,
                                          double* var)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Row* row = &sdata->cursor.row;
    if (row->ncol >= STMT_MAX_COLS)
        return 3;

    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding date result pos %d to %p",
               DBNAME, pos, var));
    row->cols[row->ncol].pos = pos;
    row->cols[row->ncol].type = STMT_VAL_DATE;
    row->cols[row->ncol].val.dval = var;
    row->cols[row->ncol].len = 0;
    ++row->ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_bindr_boolean(gdsql_stmth* stmt,
                                             int pos,
                                             int* var)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    Row* row = &sdata->cursor.row;
    if (row->ncol >= STMT_MAX_COLS)
        return 3;

    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding boolean result pos %d to %p",
               DBNAME, pos, var));
    row->cols[row->ncol].pos = pos;
    row->cols[row->ncol].type = STMT_VAL_BOOLEAN;
    row->cols[row->ncol].val.ival = var;
    row->cols[row->ncol].len = 0;
    ++row->ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_postgres_stmt_step(gdsql_stmth* stmt)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    if (stmt->state < STMT_STATE_PREPARED) {
        // Must prepare statement
        if (gdsql_postgres_stmt_prepare(stmt) != 0)
            return 2;
        
        stmt->state = STMT_STATE_PREPARED;
    }
    
    if (stmt->state < STMT_STATE_BOUNDP) {
        stmt->state = STMT_STATE_BOUNDP;
    }

    if (stmt->state < STMT_STATE_BOUNDR) {
        stmt->state = STMT_STATE_BOUNDR;
    }
    
    Param* param = &sdata->param;

    if (stmt->state < STMT_STATE_EXECUTED) {
        // Must execute statement

        GDSQL_Log(LOG_INFO,
                  ("%s: executing statement",
                   DBNAME));

        if (stmt->gdsql_db == 0)
            return 2;
    
        DbData* ddata = (DbData*) stmt->gdsql_db->data;
        if (ddata == 0)
            return 3;
        if (ddata->db == 0)
            return 4;
    
        GDSQL_Log(LOG_INFO,
                  ("%s: nParams = %d",
                   DBNAME, param->next));
        sdata->result = PQexecPrepared(ddata->db,
                                       "",
                                       param->next,
                                       param->val,
                                       param->len,
                                       param->bin,
                                       1);
        GDSQL_Log(LOG_INFO,
                  ("%s: result = %p",
                   DBNAME, sdata->result));
        if (sdata->result == 0)
            return 5;
        
        ExecStatusType st = PQresultStatus(sdata->result);
        GDSQL_Log(LOG_INFO,
                  ("%s: st = %d (%d / %d)",
                   DBNAME, (int) st,
                   (int) PGRES_COMMAND_OK, (int) PGRES_TUPLES_OK));
        if (st != PGRES_COMMAND_OK &&
            st != PGRES_TUPLES_OK)
            return 6;

        sdata->cursor.rows = 0;
        sdata->cursor.cols = 0;
        sdata->cursor.next = 0;
        if (st == PGRES_TUPLES_OK) {
            sdata->cursor.rows = PQntuples(sdata->result);
            sdata->cursor.cols = PQnfields(sdata->result);
            GDSQL_Log(LOG_INFO,
                      ("%s: result size = %d x %d", 
                       DBNAME, sdata->cursor.rows, sdata->cursor.cols));
        }

        stmt->state = STMT_STATE_EXECUTED;
    }
    
    if (stmt->state < STMT_STATE_EXHAUSTED) {
        if (sdata->cursor.next >= sdata->cursor.rows) {
            // No more rows
            PQclear(sdata->result);
            sdata->result = 0;
            param->next = 0;
            sdata->cursor.rows = 0;
            sdata->cursor.cols = 0;
            sdata->cursor.next = 0;
            sdata->cursor.row.ncol = 0;
            stmt->state = STMT_STATE_EXHAUSTED;
            return 7;
        }

        // Not yet done, can call step
        GDSQL_Log(LOG_INFO,
              ("%s: stepping statement [%s]",
               DBNAME, stmt->query));

        int j = 0;
        Row* row = &sdata->cursor.row;
        for (j = 0; j < row->ncol; ++j) {
            int pos = row->cols[j].pos;
            row->cols[j].null = PQgetisnull(sdata->result,
                                            sdata->cursor.next,
                                            pos);
            const char* val = PQgetvalue(sdata->result,
                                         sdata->cursor.next,
                                         pos);
            switch (row->cols[j].type) {
            case STMT_VAL_INT:
                GDSQL_Log(LOG_INFO,
                          ("%s: column %d int%s",
                           DBNAME, pos,
                           row->cols[j].null ? " NULL" : ""));
                *(row->cols[j].val.ival) = row->cols[j].null ? 0 : get_int32(val);
                break;
            case STMT_VAL_DOUBLE:
                GDSQL_Log(LOG_INFO,
                          ("%s: column %d double%s",
                           DBNAME, pos,
                           row->cols[j].null ? " NULL" : ""));
                *(row->cols[j].val.dval) = row->cols[j].null ? 0.0 : get_double(val);
                break;
            case STMT_VAL_STRING:
                GDSQL_Log(LOG_INFO,
                          ("%s: column %d string%s",
                           DBNAME, pos,
                           row->cols[j].null ? " NULL" : ""));
                row->cols[j].val.sval[0] = '\0';
                if (! row->cols[j].null)
                    gdsql_copy_at_most(row->cols[j].val.sval,
                                       val,
                                       row->cols[j].len);
                break;
            case STMT_VAL_DATE:
                GDSQL_Log(LOG_INFO,
                          ("%s: column %d date%s",
                           DBNAME, pos,
                           row->cols[j].null ? " NULL" : ""));
                *(row->cols[j].val.dval) = row->cols[j].null ? 0.0 : get_date(val);
                break;
            case STMT_VAL_BOOLEAN:
                GDSQL_Log(LOG_INFO,
                          ("%s: column %d boolean%s",
                           DBNAME, pos,
                           row->cols[j].null ? " NULL" : ""));
                *(row->cols[j].val.ival) = row->cols[j].null ? 0 : get_int8(val);
                break;
            }
        }
        ++sdata->cursor.next;
    }
    
    GDSQL_Log(LOG_INFO, ("%s: success!", DBNAME));
    return 0;
}

static int gdsql_postgres_stmt_is_column_null(gdsql_stmth* stmt,
                                              int pos)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 0;
    
    --pos;
    Row* row = &sdata->cursor.row;
    int j = 0;
    for (j = 0; j < row->ncol; ++j) {
        int p = row->cols[j].pos;
        if (p != pos)
            continue;
        return row->cols[j].null;
    }

    return 0;
}

static int gdsql_postgres_stmt_finalize(gdsql_stmth* stmt)
{
    StmtData* sdata = (StmtData*) stmt->data;
    int ret = 0;

    do {
        if (sdata == 0) {
            ret = 1;
            break;
        }

        do {
            if (sdata->result == 0) {
                ret = 2;
                break;
            }

            GDSQL_Log(LOG_DEBUG,
                      ("%s: finalizing statement [%s]",
                       DBNAME, stmt->query));
            PQclear(sdata->result);
            GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
        } while (0);

        free(sdata);
        stmt->data = 0;
    } while (0);

    return ret;
}


static int8 get_int8(const char* buf)
{
    int8* ip = (int8*) buf;
    int8 iv = *ip;  // No conversion required for 8-bit values
    return iv;
}

static int16 get_int16(const char* buf)
{
    int16* ip = (int16*) buf;
    int16 iv = be16toh(*ip);
    return iv;
}

static int get_int32(const char* buf)
{
    int32* ip = (int32*) buf;
    int32 iv = be32toh(*ip);
    return iv;
}

static int64 get_int64(const char* buf)
{
    int64* ip = (int64*) buf;
    int64 iv = be64toh(*ip);
    return iv;
}

static double get_double(const char* buf)
{
    int64 iv = get_int64(buf);
    double* dp = (double*) &iv;
    return *dp;
}

static double get_date(const char* buf)
{
    // A date is a signed 64-bit integer representing microseconds
    // since the cutoff date, which is arbitrarily set to 1/jan/200.

    // Convert to seconds.
    int64 iv = get_int64(buf);
    int64 secs = (int64) (1.0 * iv / MICROSECONDS_PER_SECOND);

    // Convert to a "Julian era"; Julian days start at noon, so must
    // shift by half a day.
    secs += (int64) (SECONDS_PER_DAY * (JULIAN_20000101 - 0.5));

    // Build both parts of our representation for Julian dates: the
    // integer part encodes the date, the fractional part encodes the
    // time.
    int dd = (int) (1.0 * secs / SECONDS_PER_DAY);
    int ss = (int) (secs - SECONDS_PER_DAY * dd);
    double jul = dd + 1.0 * ss / SECONDS_PER_DAY;

    return jul;
}

static int put_int8(int8 val,
                    char* buf)
{
    int8* ip = (int8*) buf;
    *ip = val;      // No conversion required for 8-bit values
    return sizeof(int8);
}

static int put_int16(int16 val,
                     char* buf)
{
    int16* ip = (int16*) buf;
    *ip = htobe16(val);
    return sizeof(int16);
}

static int put_int32(int32 val,
                     char* buf)
{
    int32* ip = (int32*) buf;
    *ip = htobe32(val);
    return sizeof(int32);
}

static int put_int64(int64 val,
                     char* buf)
{
    int64* ip = (int64*) buf;
    *ip = htobe64(val);
    return sizeof(int64);
}

static int put_double(double val,
                      char* buf)
{
    int64* ip = (int64*) &val;
    return put_int64(*ip, buf);
}

static int put_date(double val,
                    char* buf)
{
    // Julian days start at noon, so must shift by half a day.
    double jul = val + 0.5;

    // Get date and time parts.
    int dd = (int) (jul);
    int tt = (int) (SECONDS_PER_DAY * (jul - dd) + 0.5);

    // Create a single value representing microseconds since the
    // cutoff date.
    int64 iv = (((dd - JULIAN_20000101) * SECONDS_PER_DAY + tt) *
                MICROSECONDS_PER_SECOND);

    return put_int64(iv, buf);
}
