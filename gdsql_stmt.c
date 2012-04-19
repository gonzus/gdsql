#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <gdsql_log.h>
#include <gdsql_hidden.h>
#include <gdsql_util.h>
#include <gdsql_stmt.h>

gdsql_db gdsql_stmt_get_db(gdsql_stmt gdsql_stmt)
{
    gdsql_db gdsql_db = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0)
            break;

        gdsql_db = sh->gdsql_db;
    } while (0);

    return gdsql_db;
}

const char* gdsql_stmt_get_query(gdsql_stmt gdsql_stmt)
{
    const char* query = 0;

    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0)
            break;

        query = sh->query;
    } while (0);

    return query;
}

void gdsql_stmt_set_query(gdsql_stmt gdsql_stmt,
                          const char* fmt,
                          ...)
{
    do {
        if (fmt == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid stmt query"));
            break;
        }

        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0)
            break;

        gdsql_dbh* dh = sh->gdsql_db;
        if (dh == 0) {
            break;
        }

        va_list ap;
        va_start(ap, fmt);
        vsprintf(sh->query, fmt, ap);
        va_end(ap);

        const DbOps* ops = get_dbops(dh->type);
        if (ops != 0) {
            ops->stmt_create(sh);
            sh->state = STMT_STATE_DEFINED;
        }

    } while (0);
}

int gdsql_stmt_prepare(gdsql_stmt gdsql_stmt)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0) {
            ret = 1;
            break;
        }

        gdsql_dbh* dh = sh->gdsql_db;
        if (dh == 0) {
            ret = 2;
            break;
        }

        const DbOps* ops = get_dbops(dh->type);
        if (ops != 0) {
            ret = ops->stmt_prepare(sh);
            sh->state = STMT_STATE_PREPARED;
        }
    } while (0);

    return ret;
}

int gdsql_stmt_bindp_null(gdsql_stmt gdsql_stmt,
                          int pos)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindp_null(sh, pos);
    } while (0);

    return ret;
}

int gdsql_stmt_bindp_int(gdsql_stmt gdsql_stmt,
                         int pos,
                         int val)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindp_int(sh, pos, val);
    } while (0);

    return ret;
}

int gdsql_stmt_bindp_double(gdsql_stmt gdsql_stmt,
                            int pos,
                            double val)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindp_double(sh, pos, val);
    } while (0);

    return ret;
}

int gdsql_stmt_bindp_string(gdsql_stmt gdsql_stmt,
                            int pos,
                            const char* val,
                            int len)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindp_string(sh, pos, val, len);
    } while (0);
    
    return ret;
}

int gdsql_stmt_bindp_date(gdsql_stmt gdsql_stmt,
                          int pos,
                          double val)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindp_date(sh, pos, val);
    } while (0);

    return ret;
}

int gdsql_stmt_bindp_boolean(gdsql_stmt gdsql_stmt,
                             int pos,
                             int val)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindp_boolean(sh, pos, val);
    } while (0);

    return ret;
}

int gdsql_stmt_bindr_int(gdsql_stmt gdsql_stmt,
                         int pos,
                         int* var)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindr_int(sh, pos, var);
    } while (0);

    return ret;
}

int gdsql_stmt_bindr_double(gdsql_stmt gdsql_stmt,
                            int pos,
                            double* var)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindr_double(sh, pos, var);
    } while (0);

    return ret;
}

int gdsql_stmt_bindr_string(gdsql_stmt gdsql_stmt,
                            int pos,
                            char* var,
                            int len)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindr_string(sh, pos, var, len);
    } while (0);

    return ret;
}

int gdsql_stmt_bindr_date(gdsql_stmt gdsql_stmt,
                          int pos,
                          double* var)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindr_date(sh, pos, var);
    } while (0);

    return ret;
}

int gdsql_stmt_bindr_boolean(gdsql_stmt gdsql_stmt,
                             int pos,
                             int* var)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_bindr_boolean(sh, pos, var);
    } while (0);

    return ret;
}

int gdsql_stmt_step(gdsql_stmt gdsql_stmt)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_step(sh);
    } while (0);

    return ret;
}

int gdsql_stmt_is_column_null(gdsql_stmt gdsql_stmt,
                              int pos)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_is_column_null(sh, pos);
    } while (0);

    return ret;
}

int gdsql_stmt_finalize(gdsql_stmt gdsql_stmt)
{
    int ret = 0;
    
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0 || sh->gdsql_db == 0) {
            ret = 1;
            break;
        }

        const DbOps* ops = get_dbops(sh->gdsql_db->type);
        if (ops != 0)
            ret = ops->stmt_finalize(sh);
    } while (0);

    return ret;
}
