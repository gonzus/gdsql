GDSQL -- The "Good SQL" library
===============================


What is does / will do
----------------------

1. Define a uniform C API to access a relational database using
prepared statements.
2. Support multiple databases, at the same time from a single program.
3. Simplify your life.


What is does not do / will never do
-----------------------------------

1. Provide an object-relational bridge.
2. Hide different SQL syntaxes.
3. Eat up your memory and/or CPU.


What does it look like
----------------------

This is an example of accessing a PostgreSQL database. We read rows
from table `people`, which has at least these columns:

* `id`: the integer primary key
* `name`: a string with the person's name.
* `birth`: a datetime with the person's birthday.

Here is the code, without any error checking. This exact same code
should work with other databases (except for any differences in the
SQL syntax).

    gdsql gdsql = 0;
    gdsql_db db = 0;
    gdsql_stmt stmt = 0;

    gdsql = gdsql_init();

    db = gdsql_alloc_db(gdsql, GDSQL_DB_POSTGRES);
    gdsql_db_set_host(db, "localhost");
    gdsql_db_set_port(db, 5432);
    gdsql_db_set_name(db, "db");
    gdsql_db_set_user(db, "user");
    gdsql_db_set_password(db, "password");
    gdsql_db_open(db);

    stmt = gdsql_db_alloc_stmt(db);
    gdsql_stmt_set_query(stmt,
                         "SELECT id,name FROM people WHERE birth BETWEEN $1 AND $2 ORDER BY id;");

    double d1 = gdsql_cal2jul(1970,  1,  1,  0,  0,  0);
    double d2 = gdsql_cal2jul(2004, 12, 31, 23, 59, 59);
            
    gdsql_stmt_bindp_date(stmt, 1, d1);
    gdsql_stmt_bindp_date(stmt, 2, d2);
        
    int id;
    char name[100];

    gdsql_stmt_bindr_int   (stmt, 1, &id);
    gdsql_stmt_bindr_string(stmt, 2, name, 100);

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
    gdsql_db_free_stmt(stmt);
    gdsql_db_close(db);
    gdsql_free_db(db);
    gdsql_fini(gdsql);


What databases are supported
----------------------------

There already is support for [SQLite][1], [PostgreSQL][2] and
[MySQL][3].

I believe the library will be ready for a v1.0 release when it also
provides support for [Oracle][4], [Sybase][5], [DB2][6] and [SQL
Server][7].


What the name means
-------------------

The name `gdsql` means "Good SQL". The fact that `GD` are also the
initials of the original author is purely coincidental...


[1]: http://www.sqlite.org/                 "SQLite"
[2]: http://www.postgresql.org/             "PostgreSQL"
[3]: http://www.mysql.com/                  "MySQL"
[4]: http://www.oracle.com/                 "Oracle"
[5]: http://www.sybase.com/                 "Sybase"
[6]: http://www.ibm.com/software/data/db2/  "DB2"
[7]: http://www.microsoft.com/sqlserver/    "SQL Server"
