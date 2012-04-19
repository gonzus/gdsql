#include <stdio.h>
#include <stdlib.h>
#include <gdsql_log.h>
#include <gdsql_hidden.h>
#include <gdsql_util.h>
#include <gdsql.h>

gdsql gdsql_init(void)
{
    gdsqlh* xh = 0;

    do {
        xh = (gdsqlh*) malloc(sizeof(gdsqlh));
        if (xh == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Could not create gdsql object"));
            break;
        }
    
        xh->version = GDSQL_VERSION;
    } while (0);
    
    return xh;
}

void gdsql_fini(gdsql gdsql)
{
    do {
        if (gdsql == 0)
            break;
    
        gdsqlh* xh = (gdsqlh*) gdsql;
        if (xh->version != GDSQL_VERSION) {
            GDSQL_Log(LOG_WARNING,
                      ("Bad gdsql object"));
            return;
        }

        free(xh);
    } while(0);
}

gdsql_db gdsql_alloc_db(gdsql gdsql,
                        int type)
{
    gdsql_dbh* dh = 0;

    do {
        gdsqlh* xh = gdsql_check_gdsql(gdsql);
        if (xh == 0)
            break;
    
        if (type < 0 || type >= GDSQL_DB_COUNT) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid DB type"));
            break;
        }

        dh = (gdsql_dbh*) malloc(sizeof(gdsql_dbh));
        if (dh == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Could not create gdsql_db object"));
            break;
        }

        dh->gdsql = xh;
        dh->data = 0;
        dh->type = type;
        dh->host[0] = '\0';
        dh->port = 0;
        dh->name[0] = '\0';
        dh->user[0] = '\0';
        dh->password[0] = '\0';

        const DbOps* ops = get_dbops(dh->type);
        if (ops != 0)
            ops->init();
    } while (0);
    
    return dh;
}

void gdsql_free_db(gdsql_db gdsql_db)
{
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        const DbOps* ops = get_dbops(dh->type);
        if (ops != 0)
            ops->fini();

        free(dh);
    } while (0);
}

const char* gdsql_get_version(gdsql gdsql,
                              char* buf)
{
    buf[0] = '\0';
    do {
        if (gdsql == 0)
            break;
        
        gdsqlh* u = (gdsqlh*) gdsql;
        if (u->version != GDSQL_VERSION) {
            GDSQL_Log(LOG_WARNING,
                      ("Bad gdsql object"));
            break;
        }
        
        sprintf(buf, "%d", u->version);
    } while (0);
    
    return buf;
}

int gdsql_add_db(int dbtype)
{
    extern int gdsql_sqlite_boot(void);
    extern int gdsql_postgres_boot(void);
    extern int gdsql_mysql_boot(void);
    int ret = 0;

    do {
        if (dbtype < 0 || dbtype >= GDSQL_DB_COUNT) {
            GDSQL_Log(LOG_WARNING,
                      ("Cannot add invalid DB %d",
                       dbtype));
            ret = 1;
            break;
        }

        const DbOps* ops = 0;
        ops = get_dbops(dbtype);
        if (ops != 0)
            break;
    
        switch (dbtype) {
        case GDSQL_DB_SQLITE:
            gdsql_sqlite_boot();
            break;
        case GDSQL_DB_POSTGRES:
            gdsql_postgres_boot();
            break;
        case GDSQL_DB_MYSQL:
            gdsql_mysql_boot();
            break;
        }

        ops = get_dbops(dbtype);
        if (ops == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Could not add DB %d",
                       dbtype));
            ret = 2;
            break;
        }
    } while (0);

    return ret;
}

int gdsql_add_all_dbs(void)
{
    int j = 0;
    int n = 0;
    
    for (j = 0; j < GDSQL_DB_COUNT; ++j)
        if (gdsql_add_db(j) == 0)
            ++n;

    return n;
}
