#include <stdlib.h>
#include <unistd.h>
#include <gdsql_log.h>
#include <gdsql_util.h>

gdsqlh* gdsql_check_gdsql(gdsql gdsql)
{
    gdsqlh* uh = (gdsqlh*) gdsql;
    if (uh == 0 ||
        uh->version != GDSQL_VERSION) {
        GDSQL_Log(LOG_WARNING,
                  ("Bad gdsql object"));
        return 0;
    }

    return uh;
}

gdsql_dbh* gdsql_check_db(gdsql_db gdsql_db)
{
    gdsql_dbh* dh = (gdsql_dbh*) gdsql_db;
    if (dh == 0 ||
        dh->gdsql == 0 ||
        dh->gdsql->version != GDSQL_VERSION) {
        GDSQL_Log(LOG_WARNING,
                  ("Bad gdsql_db object"));
        return 0;
    }

    return dh;
}

gdsql_stmth* gdsql_check_stmt(gdsql_stmt gdsql_stmt)
{
    gdsql_stmth* ds = (gdsql_stmth*) gdsql_stmt;
    if (ds == 0 ||
        ds->gdsql_db == 0 ||
        ds->gdsql_db->gdsql == 0 ||
        ds->gdsql_db->gdsql->version != GDSQL_VERSION) {
        GDSQL_Log(LOG_WARNING,
                  ("Bad gdsql_stmt object"));
        return 0;
    }

    return ds;
}

int gdsql_copy_at_most(char* tgt,
                       const char* src,
                       int top)
{
    int s = 0;
    int t = 0;
    for (s = 0; src != 0 && src[s] != 0; ++s) {
        if ((t + 1) >= top)
            break;
        tgt[t++] = src[s];
    }
    tgt[t] = '\0';
    return t;
}

const char* gdsql_getenv(const char* name,
                         const char* defl)
{
    const char* s = getenv(name);
    if (s == 0)
        s = defl;
    return s;
}

unsigned int gdsql_getpid(void)
{
    unsigned int pid = (unsigned int) getpid();
    return pid;
}

int gdsql_file_base(const char* full)
{
    int i, s;

    s = -1;
    for (i = 0; full[i] != '\0'; ++i)
        if (full[i] == '\\' || full[i] == '/')
            s = i;

    return (s+1);
}
