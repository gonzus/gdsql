#include <gdsql.h>
#include <gdsql_hidden.h>

static const DbOps* dbops[GDSQL_DB_COUNT];

const DbOps* get_dbops(int dbtype)
{
    if (dbtype < 0 || dbtype >= GDSQL_DB_COUNT)
        return 0;

    return dbops[dbtype];
}

void set_dbops(int dbtype,
               const DbOps* ops)
{
    if (dbtype < 0 || dbtype >= GDSQL_DB_COUNT)
        return;

    dbops[dbtype] = ops;
}
