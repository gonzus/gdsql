#ifndef GDSQL_DB_H_
#define GDSQL_DB_H_

#include <gdsql_types.h>

gdsql_stmt gdsql_db_alloc_stmt(gdsql_db gdsql_db);
void gdsql_db_free_stmt(gdsql_stmt stmt);

gdsql gdsql_db_get_gdsql(gdsql_db gdsql_db);

int gdsql_db_get_type(gdsql_db gdsql_db);

const char* gdsql_db_get_host(gdsql_db gdsql_db);
void gdsql_db_set_host(gdsql_db gdsql_db,
                       const char* host);

int gdsql_db_get_port(gdsql_db gdsql_db);
void gdsql_db_set_port(gdsql_db gdsql_db,
                       int port);

const char* gdsql_db_get_name(gdsql_db gdsql_db);
void gdsql_db_set_name(gdsql_db gdsql_db,
                       const char* name);

const char* gdsql_db_get_user(gdsql_db gdsql_db);
void gdsql_db_set_user(gdsql_db gdsql_db,
                       const char* user);

const char* gdsql_db_get_password(gdsql_db gdsql_db);
void gdsql_db_set_password(gdsql_db gdsql_db,
                           const char* password);

int gdsql_db_open(gdsql_db gdsql_db);
int gdsql_db_close(gdsql_db gdsql_db);

#endif
