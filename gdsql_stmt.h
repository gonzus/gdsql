#ifndef GDSQL_STMT_H_
#define GDSQL_STMT_H_

#include <gdsql_types.h>

gdsql_db gdsql_stmt_get_db(gdsql_stmt gdsql_stmt);

const char* gdsql_stmt_get_query(gdsql_stmt gdsql_stmt);
void gdsql_stmt_set_query(gdsql_stmt gdsql_stmt,
                          const char* fmt,
                          ...);

int gdsql_stmt_prepare(gdsql_stmt gdsql_stmt);

int gdsql_stmt_bindp_null(gdsql_stmt gdsql_stmt,
                          int pos);
int gdsql_stmt_bindp_int(gdsql_stmt gdsql_stmt,
                         int pos,
                         int val);
int gdsql_stmt_bindp_double(gdsql_stmt gdsql_stmt,
                            int pos,
                            double val);
int gdsql_stmt_bindp_string(gdsql_stmt gdsql_stmt,
                            int pos,
                            const char* val,
                            int len);
int gdsql_stmt_bindp_date(gdsql_stmt gdsql_stmt,
                          int pos,
                          double val);
int gdsql_stmt_bindp_boolean(gdsql_stmt gdsql_stmt,
                             int pos,
                             int val);

int gdsql_stmt_bindr_int(gdsql_stmt gdsql_stmt,
                         int pos,
                         int* var);
int gdsql_stmt_bindr_double(gdsql_stmt gdsql_stmt,
                            int pos,
                            double* var);
int gdsql_stmt_bindr_string(gdsql_stmt gdsql_stmt,
                            int pos,
                            char* var,
                            int len);
int gdsql_stmt_bindr_date(gdsql_stmt gdsql_stmt,
                          int pos,
                          double* var);
int gdsql_stmt_bindr_boolean(gdsql_stmt gdsql_stmt,
                             int pos,
                             int* var);

int gdsql_stmt_step(gdsql_stmt gdsql_stmt);
int gdsql_stmt_is_column_null(gdsql_stmt gdsql_stmt,
                              int pos);

int gdsql_stmt_finalize(gdsql_stmt gdsql_stmt);


#endif
