#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <gdsql_date.h>
#include <gdsql_log.h>
#include <gdsql_hidden.h>
#include <gdsql_util.h>

#define DBNAME "SQLite"

typedef struct DbData {
    sqlite3* db;
} DbData;

#define STMT_MAX_PARAMS        100
#define STMT_MAX_PARAM_LENGTH   50

#define PARAM_TYPE_NULL     0
#define PARAM_TYPE_INT      1
#define PARAM_TYPE_DOUBLE   2
#define PARAM_TYPE_STRING   3
#define PARAM_TYPE_DATE     4
#define PARAM_TYPE_BOOLEAN  5

typedef struct PString {
    char buf[STMT_MAX_PARAM_LENGTH + 1];
    int len;
} PString;

typedef union PValue {
    int ival;
    double dval;
    PString sval;
} PValue;

typedef struct Param {
    PValue value[STMT_MAX_PARAMS];
    int pos[STMT_MAX_PARAMS];
    int type[STMT_MAX_PARAMS];
    int next;
} Param;

typedef struct StmtData {
    sqlite3_stmt* ps;
    Param param;
    Row row;
} StmtData;

static int gdsql_sqlite_init(void);
static int gdsql_sqlite_fini(void);

static int gdsql_sqlite_db_alloc(void);
static int gdsql_sqlite_db_free(void);

static int gdsql_sqlite_db_open(gdsql_dbh* db);
static int gdsql_sqlite_db_close(gdsql_dbh* db);

static int gdsql_sqlite_stmt_create(gdsql_stmth* stmt);
static int gdsql_sqlite_stmt_prepare(gdsql_stmth* stmt);

static int gdsql_sqlite_stmt_bindp_null(gdsql_stmth* stmt,
                                        int pos);
static int gdsql_sqlite_stmt_bindp_int(gdsql_stmth* stmt,
                                       int pos,
                                       int val);
static int gdsql_sqlite_stmt_bindp_double(gdsql_stmth* stmt,
                                          int pos,
                                          double val);
static int gdsql_sqlite_stmt_bindp_string(gdsql_stmth* stmt,
                                          int pos,
                                          const char* val,
                                          int len);
static int gdsql_sqlite_stmt_bindp_date(gdsql_stmth* stmt,
                                        int pos,
                                        double val);
static int gdsql_sqlite_stmt_bindp_boolean(gdsql_stmth* stmt,
                                           int pos,
                                           int val);

static int gdsql_sqlite_stmt_bindr_int(gdsql_stmth* stmt,
                                       int pos,
                                       int* var);
static int gdsql_sqlite_stmt_bindr_double(gdsql_stmth* stmt,
                                          int pos,
                                          double* var);
static int gdsql_sqlite_stmt_bindr_string(gdsql_stmth* stmt,
                                          int pos,
                                          char* var,
                                          int len);
static int gdsql_sqlite_stmt_bindr_date(gdsql_stmth* stmt,
                                        int pos,
                                        double* var);
static int gdsql_sqlite_stmt_bindr_boolean(gdsql_stmth* stmt,
                                           int pos,
                                           int* var);

static int gdsql_sqlite_stmt_step(gdsql_stmth* stmt);
static int gdsql_sqlite_stmt_is_column_null(gdsql_stmth* stmt,
                                            int pos);
static int gdsql_sqlite_stmt_finalize(gdsql_stmth* stmt);


int gdsql_sqlite_boot(void)
{
    static DbOps ops = {
        gdsql_sqlite_init,
        gdsql_sqlite_fini,
        gdsql_sqlite_db_alloc,
        gdsql_sqlite_db_free,
        gdsql_sqlite_db_open,
        gdsql_sqlite_db_close,
        gdsql_sqlite_stmt_create,
        gdsql_sqlite_stmt_prepare,
        gdsql_sqlite_stmt_bindp_null,
        gdsql_sqlite_stmt_bindp_int,
        gdsql_sqlite_stmt_bindp_double,
        gdsql_sqlite_stmt_bindp_string,
        gdsql_sqlite_stmt_bindp_date,
        gdsql_sqlite_stmt_bindp_boolean,
        gdsql_sqlite_stmt_bindr_int,
        gdsql_sqlite_stmt_bindr_double,
        gdsql_sqlite_stmt_bindr_string,
        gdsql_sqlite_stmt_bindr_date,
        gdsql_sqlite_stmt_bindr_boolean,
        gdsql_sqlite_stmt_step,
        gdsql_sqlite_stmt_is_column_null,
        gdsql_sqlite_stmt_finalize,
    };

    GDSQL_Log(LOG_INFO,
              ("%s: booting",
               DBNAME));
    set_dbops(GDSQL_DB_SQLITE, &ops);
    return 0;
}


static int gdsql_sqlite_init(void)
{
    int ret = 0;
    
    GDSQL_Log(LOG_INFO,
              ("%s: initializing sqlite3",
               DBNAME));
    if (sqlite3_initialize() != SQLITE_OK)
        ret = 1;

    return ret;
}

static int gdsql_sqlite_fini(void)
{
    int ret = 0;
    
    GDSQL_Log(LOG_INFO,
              ("%s: terminating sqlite3",
               DBNAME));
    if (sqlite3_shutdown() != SQLITE_OK)
        ret = 1;

    return ret;
}

static int gdsql_sqlite_db_alloc(void)
{
    return 0;
}

static int gdsql_sqlite_db_free(void)
{
    return 0;
}

static int gdsql_sqlite_db_open(gdsql_dbh* db)
{
    db->data = 0;
    
    GDSQL_Log(LOG_INFO,
              ("%s: opening file [%s]",
               DBNAME, db->name));
    sqlite3* sql_db;
    if (sqlite3_open(db->name, &sql_db) != SQLITE_OK)
        return 1;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    DbData* ddata = (DbData*) malloc(sizeof(DbData));
    ddata->db = sql_db;
    db->data = ddata;
    return 0;
}

static int gdsql_sqlite_db_close(gdsql_dbh* db)
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
                      ("%s: closing file [%s]",
                       DBNAME, db->name));
            if (sqlite3_close(ddata->db) != SQLITE_OK) {
                ret = 3;
                break;
            }
            GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));
        } while (0);

        free(ddata);
        db->data = 0;
    } while (0);

    return ret;
}

static int gdsql_sqlite_stmt_create(gdsql_stmth* stmt)
{
    if (stmt->gdsql_db == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: creating statement",
               DBNAME));
    StmtData* sdata = (StmtData*) malloc(sizeof(StmtData));
    memset(sdata, 0, sizeof(StmtData));
    stmt->data = sdata;
    return 0;
}

static int gdsql_sqlite_stmt_prepare(gdsql_stmth* stmt)
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
              ("%s: preparing statement [%s]",
               DBNAME, stmt->query));
    sqlite3_stmt *sql_ps = 0;
    if (sqlite3_prepare_v2(ddata->db,
                           stmt->query,
                           -1,
                           &sql_ps,
                           0) != SQLITE_OK)
        return 5;

    sdata->ps = sql_ps;
    GDSQL_Log(LOG_INFO, ("%s: success!", DBNAME));
    return 0;
}

static int gdsql_sqlite_stmt_bindp_null(gdsql_stmth* stmt,
                                        int pos)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: binding NULL param pos %d",
               DBNAME, pos));
    sdata->param.pos[sdata->param.next] = pos;
    sdata->param.type[sdata->param.next] = PARAM_TYPE_NULL;
    ++sdata->param.next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindp_int(gdsql_stmth* stmt,
                                       int pos,
                                       int val)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: binding int param pos %d to %d",
               DBNAME, pos, val));
    sdata->param.pos[sdata->param.next] = pos;
    sdata->param.type[sdata->param.next] = PARAM_TYPE_INT;
    sdata->param.value[sdata->param.next].ival = val;
    ++sdata->param.next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindp_double(gdsql_stmth* stmt,
                                          int pos,
                                          double val)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: binding double param pos %d to %lf",
               DBNAME, pos, val));
    sdata->param.pos[sdata->param.next] = pos;
    sdata->param.type[sdata->param.next] = PARAM_TYPE_DOUBLE;
    sdata->param.value[sdata->param.next].dval = val;
    ++sdata->param.next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindp_string(gdsql_stmth* stmt,
                                          int pos,
                                          const char* val,
                                          int len)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;

    if (len < 0)
        len = strlen(val);
    
    GDSQL_Log(LOG_INFO,
              ("%s: binding string param pos %d to [%d:%s]",
               DBNAME, pos, len, val));
    sdata->param.pos[sdata->param.next] = pos;
    sdata->param.type[sdata->param.next] = PARAM_TYPE_STRING;
    memcpy(sdata->param.value[sdata->param.next].sval.buf, val, len+1);
    sdata->param.value[sdata->param.next].sval.len = len;
    ++sdata->param.next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindp_date(gdsql_stmth* stmt,
                                        int pos,
                                        double val)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    
    GDSQL_Log(LOG_INFO,
              ("%s: binding date param pos %d to %lf",
               DBNAME, pos, val));
    sdata->param.pos[sdata->param.next] = pos;
    sdata->param.type[sdata->param.next] = PARAM_TYPE_DATE;
    sdata->param.value[sdata->param.next].dval = val;
    ++sdata->param.next;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindp_boolean(gdsql_stmth* stmt,
                                           int pos,
                                           int val)
{
    return gdsql_sqlite_stmt_bindp_int(stmt, pos, val);
}

static int gdsql_sqlite_stmt_bindr_int(gdsql_stmth* stmt,
                                       int pos,
                                       int* var)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    if (sdata->row.ncol >= STMT_MAX_COLS)
        return 3;

    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding int result pos %d to %p",
               DBNAME, pos, var));
    sdata->row.cols[sdata->row.ncol].pos = pos;
    sdata->row.cols[sdata->row.ncol].type = STMT_VAL_INT;
    sdata->row.cols[sdata->row.ncol].len = 0;
    sdata->row.cols[sdata->row.ncol].null = 0;
    sdata->row.cols[sdata->row.ncol].val.ival = var;
    ++sdata->row.ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindr_double(gdsql_stmth* stmt,
                                          int pos,
                                          double* var)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    if (sdata->row.ncol >= STMT_MAX_COLS)
        return 3;
    
    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding double result pos %d to %p",
               DBNAME, pos, var));
    sdata->row.cols[sdata->row.ncol].pos = pos;
    sdata->row.cols[sdata->row.ncol].type = STMT_VAL_DOUBLE;
    sdata->row.cols[sdata->row.ncol].len = 0;
    sdata->row.cols[sdata->row.ncol].null = 0;
    sdata->row.cols[sdata->row.ncol].val.dval = var;
    ++sdata->row.ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindr_string(gdsql_stmth* stmt,
                                          int pos,
                                          char* var,
                                          int len)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    if (sdata->row.ncol >= STMT_MAX_COLS)
        return 3;
    
    --pos;
    GDSQL_Log(LOG_INFO,
              ("%s: binding string result pos %d to [%d:%p]",
               DBNAME, pos, len, var));
    sdata->row.cols[sdata->row.ncol].pos = pos;
    sdata->row.cols[sdata->row.ncol].type = STMT_VAL_STRING;
    sdata->row.cols[sdata->row.ncol].len = len;
    sdata->row.cols[sdata->row.ncol].null = 0;
    sdata->row.cols[sdata->row.ncol].val.sval = var;
    ++sdata->row.ncol;
    GDSQL_Log(LOG_DEBUG, ("%s: success!", DBNAME));

    return 0;
}

static int gdsql_sqlite_stmt_bindr_date(gdsql_stmth* stmt,
                                        int pos,
                                        double* var)
{
    return gdsql_sqlite_stmt_bindr_double(stmt, pos, var);
}

static int gdsql_sqlite_stmt_bindr_boolean(gdsql_stmth* stmt,
                                           int pos,
                                           int* var)
{
    return gdsql_sqlite_stmt_bindr_int(stmt, pos, var);
}

static int gdsql_sqlite_stmt_step(gdsql_stmth* stmt)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 1;
    
    if (stmt->state < STMT_STATE_PREPARED) {
        // Must prepare statement
        if (gdsql_sqlite_stmt_prepare(stmt) != 0)
            return 2;
        
        stmt->state = STMT_STATE_PREPARED;
    }
    
    if (sdata->ps == 0)
        return 2;

    if (stmt->state < STMT_STATE_BOUNDP) {
        // Must bind parameters
        int j = 0;
        for (j = 0; j < sdata->param.next; ++j) {
            switch (sdata->param.type[j]) {
            case PARAM_TYPE_NULL:
                if (sqlite3_bind_null(sdata->ps,
                                      sdata->param.pos[j]) != SQLITE_OK)
                    return 3;
                break;
            case PARAM_TYPE_INT:
            case PARAM_TYPE_BOOLEAN:
                if (sqlite3_bind_int(sdata->ps,
                                     sdata->param.pos[j],
                                     sdata->param.value[j].ival) != SQLITE_OK)
                    return 3;
                break;
            case PARAM_TYPE_DOUBLE:
            case PARAM_TYPE_DATE:
                if (sqlite3_bind_double(sdata->ps,
                                        sdata->param.pos[j],
                                        sdata->param.value[j].dval) != SQLITE_OK)
                    return 3;
                break;
            case PARAM_TYPE_STRING:
                if (sqlite3_bind_text(sdata->ps,
                                      sdata->param.pos[j],
                                      sdata->param.value[j].sval.buf,
                                      sdata->param.value[j].sval.len,
                                      SQLITE_STATIC) != SQLITE_OK)

                    return 3;
                break;
            }
        }

        stmt->state = STMT_STATE_BOUNDP;
    }

    if (stmt->state < STMT_STATE_BOUNDR) {
        stmt->state = STMT_STATE_BOUNDR;
    }
    
    if (stmt->state < STMT_STATE_EXECUTED) {
        stmt->state = STMT_STATE_EXECUTED;
    }
    
    if (stmt->state < STMT_STATE_EXHAUSTED) {
        // Not yet done, can call step
        GDSQL_Log(LOG_INFO,
                  ("%s: stepping statement [%s]",
                   DBNAME, stmt->query));
        int st = sqlite3_step(sdata->ps);
        GDSQL_Log(LOG_INFO,
                  ("step returned %d (%d)",
                   st, SQLITE_ROW));
        if (st != SQLITE_ROW) {
            stmt->state = STMT_STATE_EXHAUSTED;
            return 3;
        }

        int j = 0;
        for (j = 0; j < sdata->row.ncol; ++j) {
            int pos = sdata->row.cols[j].pos;
            int ctype = sqlite3_column_type(sdata->ps, pos);

            sdata->row.cols[j].null = 0;
            if (ctype == SQLITE_NULL) {
                GDSQL_Log(LOG_INFO,
                          ("%s: column %d NULL",
                           DBNAME, pos));
                sdata->row.cols[j].null = 1;
            }
        
            switch (sdata->row.cols[j].type) {
            case STMT_VAL_INT:
                *(sdata->row.cols[j].val.ival) = 0;
                if (ctype == SQLITE_INTEGER) {
                    GDSQL_Log(LOG_INFO,
                              ("%s: column %d int NOT NULL",
                               DBNAME, pos));
                    *(sdata->row.cols[j].val.ival) = sqlite3_column_int(sdata->ps, pos);
                } else {
                    GDSQL_Log(LOG_INFO,
                              ("%s: column %d int INVALID TYPE",
                               DBNAME, pos));
                }
                break;
            case STMT_VAL_DOUBLE:
                *(sdata->row.cols[j].val.dval) = 0.0;
                if (ctype == SQLITE_FLOAT) {
                    GDSQL_Log(LOG_INFO,
                              ("%s: column %d double NOT NULL",
                               DBNAME, pos));
                    *(sdata->row.cols[j].val.dval) = sqlite3_column_double(sdata->ps, pos);
                } else {
                    GDSQL_Log(LOG_INFO,
                              ("%s: column %d double INVALID TYPE",
                               DBNAME, pos));
                }
                break;
            case STMT_VAL_STRING:
                sdata->row.cols[j].val.sval[0] = '\0';
                if (ctype == SQLITE_TEXT) {
                    GDSQL_Log(LOG_INFO,
                              ("%s: column %d string NOT NULL",
                               DBNAME, pos));
                    gdsql_copy_at_most(sdata->row.cols[j].val.sval,
                                       (char*) sqlite3_column_text(sdata->ps, pos),
                                       sdata->row.cols[j].len);
                } else {
                    GDSQL_Log(LOG_INFO,
                              ("%s: column %d string INVALID TYPE",
                               DBNAME, pos));
                }
                break;
            }
        }
    }

    GDSQL_Log(LOG_INFO, ("%s: success!", DBNAME));
    return 0;
}

static int gdsql_sqlite_stmt_is_column_null(gdsql_stmth* stmt,
                                            int pos)
{
    StmtData* sdata = (StmtData*) stmt->data;
    if (sdata == 0)
        return 0;
    
    --pos;
    int j = 0;
    for (j = 0; j < sdata->row.ncol; ++j) {
        int p = sdata->row.cols[j].pos;
        if (p != pos)
            continue;
        return sdata->row.cols[j].null;
    }

    return 0;
}

static int gdsql_sqlite_stmt_finalize(gdsql_stmth* stmt)
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

            GDSQL_Log(LOG_DEBUG,
                      ("%s: finalizing sqlite3 statement [%s]",
                       DBNAME, stmt->query));
            if (sqlite3_finalize(sdata->ps) != SQLITE_OK) {
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
