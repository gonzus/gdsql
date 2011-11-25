GDSQL - The "Good SQL" library
==============================

What is does / will do
----------------------

1. Define a uniform C API to access a relational database using prepared statements.
2. Support multiple databases.
3. Simplify your life.

What is does not do / will never do
-----------------------------------

1. Provide an object-relational bridge.
2. Hide different SQL syntaxes.
3. Eat up your memory and/or CPU.

What does it look like
----------------------

This is an example of accessing a Postgres database. We read rows from
table `people`, which has at least these columns:

* `id`: the integer primary key
* `name`: a string with the person's name.
* `birth`: a datetime with the person's birthday.

    gdsql gdsql = 0;
    gdsql_db db = 0;
    gdsql_stmt stmt = 0;

    gdsql = gdsql_init();

    db = gdsql_db_alloc(gdsql, TEST_DB_TYPE);
    gdsql_db_set_host(db, "localhost");
    gdsql_db_set_port(db, 5432);
    gdsql_db_set_name(db, "db");
    gdsql_db_set_user(db, "user");
    gdsql_db_set_password(db, "password");
    gdsql_db_open(db);

    stmt = gdsql_stmt_alloc(gdsql);
    gdsql_stmt_set_query(stmt,
                         "SELECT id,name FROM people WHERE birth BETWEEN $1 AND $2 ORDER BY id;");
    gdsql_stmt_prepare(stmt, db)

    double d1 = gdsql_cal2jul(1970,  1,  1, 0, 0, 0);
    double d2 = gdsql_cal2jul(2004, 12, 31, 0, 0, 0);
            
    gdsql_stmt_bindp_date(stmt, 1, d1);
    gdsql_stmt_bindp_date(stmt, 2, d2);
        
    int id;
    char name[100];
    double birth;
    double height;
    int single;

    gdsql_stmt_bindr_int    (stmt, 1, &id);
    gdsql_stmt_bindr_string (stmt, 2, name, 100);

    int n = 0;
    while (1) {
        id = -1;
        name[0] = '\0';

        int ret = gdsql_stmt_step(stmt);
        if (ret != 0)
            break;

            if (gdsql_stmt_is_column_null(stmt, 2))
                strcpy(name, "NULL");

            printf("Row %d: %d|%s\n",
                   ++n, id, name);
    }

    gdsql_stmt_finalize(stmt);
    gdsql_stmt_free(stmt);
    gdsql_db_close(db);
    gdsql_db_free(db);
    gdsql_fini(gdsql);
