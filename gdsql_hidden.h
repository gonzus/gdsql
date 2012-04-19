#ifndef GDSQL_HIDDEN_H
#define GDSQL_HIDDEN_H

typedef struct gdsqlh {
    unsigned char version;
} gdsqlh;


typedef struct gdsql_dbh {
    gdsqlh* gdsql;
    void* data;
    int type;
    char host[50];
    unsigned short port;
    char name[50];
    char user[50];
    char password[50];
} gdsql_dbh;


#define STMT_STATE_CREATED   0
#define STMT_STATE_DEFINED   1
#define STMT_STATE_PREPARED  2
#define STMT_STATE_BOUNDP    3
#define STMT_STATE_BOUNDR    4
#define STMT_STATE_EXECUTED  5
#define STMT_STATE_EXHAUSTED 6

typedef struct gdsql_stmth {
    gdsql_dbh* gdsql_db;
    void* data;
    int state;
    char query[512];
} gdsql_stmth;


#define STMT_MAX_COLS    100

#define STMT_VAL_INVALID 0
#define STMT_VAL_INT     1
#define STMT_VAL_DOUBLE  2
#define STMT_VAL_STRING  3
#define STMT_VAL_DATE    4
#define STMT_VAL_BOOLEAN 5

typedef union Value {
    int* ival;
    double* dval;
    char* sval;
} Value;

typedef struct Col {
    unsigned short pos;
    unsigned short type;
    unsigned short len;
    unsigned short null;
    Value val;
} Col;

typedef struct Row {
    Col cols[STMT_MAX_COLS];
    int ncol;
} Row;

typedef int (sql_V)(void);
typedef int (sql_Dp)(gdsql_dbh* db);
typedef int (sql_Sp)(gdsql_stmth* stmt);
typedef int (sql_SpI)(gdsql_stmth* stmt,
                      int pos);
typedef int (sql_SpII)(gdsql_stmth* stmt,
                       int pos,
                       int val);
typedef int (sql_SpID)(gdsql_stmth* stmt,
                       int pos,
                       double val);
typedef int (sql_SpIXpI)(gdsql_stmth* stmt,
                         int pos,
                         const char* val,
                         int len);
typedef int (sql_SpIIp)(gdsql_stmth* stmt,
                        int pos,
                        int* var);
typedef int (sql_SpIDp)(gdsql_stmth* stmt,
                        int pos,
                        double* var);
typedef int (sql_SpICpI)(gdsql_stmth* stmt,
                         int pos,
                         char* var,
                         int len);

typedef struct DbOps {
    sql_V* init;
    sql_V* fini;
    
    sql_V* db_alloc;
    sql_V* db_free;
    sql_Dp* db_open;
    sql_Dp* db_close;
    
    sql_Sp* stmt_create;
    sql_Sp* stmt_prepare;
    
    sql_SpI* stmt_bindp_null;
    sql_SpII* stmt_bindp_int;
    sql_SpID* stmt_bindp_double;
    sql_SpIXpI* stmt_bindp_string;
    sql_SpID* stmt_bindp_date;
    sql_SpII* stmt_bindp_boolean;
    
    sql_SpIIp* stmt_bindr_int;
    sql_SpIDp* stmt_bindr_double;
    sql_SpICpI* stmt_bindr_string;
    sql_SpIDp* stmt_bindr_date;
    sql_SpIIp* stmt_bindr_boolean;
    
    sql_Sp* stmt_step;
    sql_SpI* stmt_is_column_null;
    sql_Sp* stmt_finalize;
} DbOps;

const DbOps* get_dbops(int dbtype);
void set_dbops(int dbtype,
               const DbOps* ops);

#endif
