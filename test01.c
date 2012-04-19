#include <stdio.h>
#include <string.h>
#include <gdsql.h>

static int test_sqlite(gdsql gdsql);
static int test_postgres(gdsql gdsql);
static int test_mysql(gdsql gdsql);

static int show_results(gdsql_db db,
                        const char* query);

int main(int argc, char* argv[])
{
    gdsql gdsql = 0;

    do {
        char tmp[20];

        gdsql = gdsql_init();
        if (gdsql == 0)
            break;
        fprintf(stderr,
                "Initialized gdsql, %p - v%s\n",
                gdsql, gdsql_get_version(gdsql, tmp));

#if 0
        gdsql_add_db(TEST_DB_TYPE);
#else
        int cnt = gdsql_add_all_dbs();
        fprintf(stderr,
                "Registered %d databases\n",
                cnt);
#endif

        test_sqlite(gdsql);
        test_postgres(gdsql);
        test_mysql(gdsql);
    } while (0);

    gdsql_fini(gdsql);
    fprintf(stderr,
            "Terminated gdsql\n");

    return 0;
}

static int test_sqlite(gdsql gdsql)
{
    int n = 0;
    gdsql_db db = 0;

    do {
        db = gdsql_alloc_db(gdsql, GDSQL_DB_SQLITE);
        if (db == 0)
            break;
        fprintf(stderr,
                "Created SQLite DB object (type %d)\n",
                gdsql_db_get_type(db));

        gdsql_db_set_name(db, "gonzo.dat");
        fprintf(stderr,
                "Set SQLite DB object parameters: [%s]\n",
                gdsql_db_get_name(db));

        if (gdsql_db_open(db) != 0)
            break;
        fprintf(stderr,
                "Opened DB connection\n");

        const char* query = 0;
#if 1
        query = ("SELECT id,name,julianday(birth),height,single "
                 "FROM people "
                 "WHERE julianday(birth) BETWEEN ? AND ? "
                 "ORDER BY id");
#elif 0
        query = ("SELECT id,name,julianday(birth),height,single "
                 "FROM people "
                 "ORDER BY id");
#endif

        printf("Results for SQLite DB:\n");
        n = show_results(db, query);
        printf("\n");
    } while (0);
    
    gdsql_db_close(db);
    fprintf(stderr,
            "Closed DB connection\n");

    gdsql_free_db(db);
    fprintf(stderr,
            "Freed DB\n");

    return n;
}

static int test_postgres(gdsql gdsql)
{
    int n = 0;
    gdsql_db db = 0;

    do {
        db = gdsql_alloc_db(gdsql, GDSQL_DB_POSTGRES);
        if (db == 0)
            break;
        fprintf(stderr,
                "Created Postgres DB object (type %d)\n",
                gdsql_db_get_type(db));

        gdsql_db_set_host(db, "localhost");
        gdsql_db_set_port(db, 5432);
        gdsql_db_set_name(db, "gonzo");
        gdsql_db_set_user(db, "postgres");
        gdsql_db_set_password(db, "password");
        fprintf(stderr,
                "Set Postgres DB object parameters: [%s:%d:%s|%s:%s]\n",
                gdsql_db_get_host(db),
                gdsql_db_get_port(db),
                gdsql_db_get_name(db),
                gdsql_db_get_user(db),
                gdsql_db_get_password(db));

        if (gdsql_db_open(db) != 0)
            break;
        fprintf(stderr,
                "Opened DB connection\n");

        const char* query = 0;
#if 1
        query = ("SELECT * "
                 "FROM people "
                 "WHERE birth BETWEEN $1 AND $2 "
                 "ORDER BY id");
#elif 0
        query = ("SELECT * "
                 "FROM people "
                 "ORDER BY id");
#endif

        printf("Results for Postgres DB:\n");
        n = show_results(db, query);
        printf("\n");
    } while (0);
    
    gdsql_db_close(db);
    fprintf(stderr,
            "Closed DB connection\n");

    gdsql_free_db(db);
    fprintf(stderr,
            "Freed DB\n");

    return n;
}

static int test_mysql(gdsql gdsql)
{
    int n = 0;
    gdsql_db db = 0;

    do {
        db = gdsql_alloc_db(gdsql, GDSQL_DB_MYSQL);
        if (db == 0)
            break;
        fprintf(stderr,
                "Created MySQL DB object (type %d)\n",
                gdsql_db_get_type(db));

        gdsql_db_set_host(db, "127.0.0.1");
        gdsql_db_set_port(db, 3306);
        gdsql_db_set_name(db, "gonzo");
        gdsql_db_set_user(db, "root");
        gdsql_db_set_password(db, "password");
        fprintf(stderr,
                "Set MySQL DB object parameters: [%s:%d:%s|%s:%s]\n",
                gdsql_db_get_host(db),
                gdsql_db_get_port(db),
                gdsql_db_get_name(db),
                gdsql_db_get_user(db),
                gdsql_db_get_password(db));

        if (gdsql_db_open(db) != 0)
            break;
        fprintf(stderr,
                "Opened DB connection\n");

        const char* query = 0;
#if 1
        query = ("SELECT * "
                 "FROM people "
                 "WHERE birth BETWEEN ? AND ? "
                 "ORDER BY id");
#elif 0
        query = ("SELECT * "
                 "FROM people "
                 "ORDER BY id");
#endif

        printf("Results for MySQL DB:\n");
        n = show_results(db, query);
        printf("\n");
    } while (0);
    
    gdsql_db_close(db);
    fprintf(stderr,
            "Closed DB connection\n");

    gdsql_free_db(db);
    fprintf(stderr,
            "Freed DB\n");

    return n;
}

static int show_results(gdsql_db db,
                        const char* query)
{
    int n = 0;
    gdsql_stmt stmt = 0;
    
    do {
        stmt = gdsql_db_alloc_stmt(db);
        if (stmt == 0)
            break;
        fprintf(stderr,
                "Allocated statement\n");
        
        gdsql_stmt_set_query(stmt, query);
        fprintf(stderr,
                "Set statement query to [%s]\n",
                gdsql_stmt_get_query(stmt));

#if 0
        // Not required!!!!
        if (gdsql_stmt_prepare(stmt) != 0)
            break;
        fprintf(stderr,
                "Statement was prepared\n");
#endif

#if 1
        double d1 = gdsql_cal2jul(1970,  1,  1, 0, 0, 0);
        double d2 = gdsql_cal2jul(2004, 12, 31, 0, 0, 0);
            
        if (gdsql_stmt_bindp_date(stmt, 1, d1) != 0)
            break;
        if (gdsql_stmt_bindp_date(stmt, 2, d2) != 0)
            break;
        fprintf(stderr,
                "Parameters were bound\n");
#endif
        
        int id;
        char name[100];
        double birth;
        double height;
        int single;

        if (gdsql_stmt_bindr_int    (stmt, 1, &id) != 0)
            break;
        if (gdsql_stmt_bindr_string (stmt, 2, name, 100) != 0)
            break;
        if (gdsql_stmt_bindr_date   (stmt, 3, &birth) != 0)
            break;
        if (gdsql_stmt_bindr_double (stmt, 4, &height) != 0)
            break;
        if (gdsql_stmt_bindr_boolean(stmt, 5, &single) != 0)
            break;
        fprintf(stderr,
                "Results were bound\n");

        while (1) {
            id = -1;
            name[0] = '\0';
            birth = 0;
            height = 0.0;
            single = 0;

            int ret = gdsql_stmt_step(stmt);
            if (ret != 0) {
                fprintf(stderr,
                        "Finished stepping\n");
                break;
            }

            if (gdsql_stmt_is_column_null(stmt, 2))
                strcpy(name, "NULL");

            int Y, M, D;
            int h, m, s;
            Y = M = D = h = m = s = 0;
            if (! gdsql_stmt_is_column_null(stmt, 3))
                gdsql_jul2cal(birth, &Y, &M, &D, &h, &m, &s);
                
            int ns = gdsql_stmt_is_column_null(stmt, 5);

            printf("Row %d: %d|%s|%04d-%02d-%02d %02d:%02d:%02d|%lf|%s\n",
                   ++n,
                   id,
                   name,
                   Y, M, D, h, m, s,
                   height,
                   ns ? "NULL" : (single ? "TRUE" : "FALSE"));
        }
    } while (0);

    gdsql_stmt_finalize(stmt);
    fprintf(stderr,
            "Statement was finalized\n");

    gdsql_db_free_stmt(stmt);
    fprintf(stderr,
            "Freed statement\n");

    return n;
}
