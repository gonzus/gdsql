#ifndef GDSQL_UTIL_H
#define GDSQL_UTIL_H

#include <gdsql.h>
#include <gdsql_db.h>
#include <gdsql_stmt.h>
#include <gdsql_hidden.h>
#include <gdsql_util.h>

gdsqlh* gdsql_check_gdsql(gdsql gdsql);
gdsql_dbh* gdsql_check_db(gdsql_db gdsql_db);
gdsql_stmth* gdsql_check_stmt(gdsql_stmt gdsql_stmt);

int gdsql_copy_at_most(char* tgt,
                       const char* src,
                       int top);

unsigned int gdsql_get_now(int* Y, int* M, int* D,
                           int* h, int* m, int* s,
                           int utc);

const char* gdsql_getenv(const char* name,
                         const char* defl);

unsigned int gdsql_getpid(void);

int gdsql_file_base(const char* full);

#endif
