#ifndef GDSQL_H_
#define GDSQL_H_

#include <gdsql_types.h>

#define GDSQL_VERSION 1

#define GDSQL_DB_SQLITE   0
#define GDSQL_DB_POSTGRES 1
#define GDSQL_DB_MYSQL    2
#define GDSQL_DB_COUNT    3

gdsql gdsql_init(void);
void gdsql_fini(gdsql gdsql);

gdsql_db gdsql_alloc_db(gdsql gdsql,
                        int type);
void gdsql_free_db(gdsql_db db);

const char* gdsql_get_version(gdsql gdsql,
                              char* buf);

int gdsql_add_db(int dbtype);
int gdsql_add_all_dbs(void);

#include <gdsql_db.h>
#include <gdsql_stmt.h>
#include <gdsql_date.h>

#endif
