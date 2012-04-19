#include <stdlib.h>
#include <string.h>
#include <gdsql_log.h>
#include <gdsql_hidden.h>
#include <gdsql_util.h>
#include <gdsql_db.h>

gdsql_stmt gdsql_db_alloc_stmt(gdsql_db gdsql_db)
{
    gdsql_stmth* sh = 0;

    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;
    
        sh = (gdsql_stmth*) malloc(sizeof(gdsql_stmth));
        if (sh == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Could not create gdsql_stmt object"));
            break;
        }

        sh->gdsql_db = dh;
        sh->data = 0;
        sh->state = STMT_STATE_CREATED;
        sh->query[0] = '\0';
    } while (0);
    
    return sh;
}

void gdsql_db_free_stmt(gdsql_stmt gdsql_stmt)
{
    do {
        gdsql_stmth* sh = gdsql_check_stmt(gdsql_stmt);
        if (sh == 0)
            break;

        free(sh);
    } while (0);
}

gdsql gdsql_db_get_gdsql(gdsql_db gdsql_db)
{
    gdsql gdsql = 0;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;
    
        gdsql = dh->gdsql;
    } while (0);

    return gdsql;
}

int gdsql_db_get_type(gdsql_db gdsql_db)
{
    int type = -1;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        type = dh->type;
    } while (0);

    return type;
}

const char* gdsql_db_get_host(gdsql_db gdsql_db)
{
    const char* host = 0;

    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        host = dh->host;
    } while (0);

    return host;
}

void gdsql_db_set_host(gdsql_db gdsql_db,
                       const char* host)
{
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        if (host == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid DB host"));
            break;
        }

        strcpy(dh->host, host);
    } while (0);
}

int gdsql_db_get_port(gdsql_db gdsql_db)
{
    int port = -1;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        port = dh->port;
    } while (0);

    return port;
}

void gdsql_db_set_port(gdsql_db gdsql_db,
                       int port)
{
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        if (port < 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid DB port"));
            break;
        }

        dh->port = port;
    } while (0);
}

const char* gdsql_db_get_name(gdsql_db gdsql_db)
{
    const char* name = 0;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        name = dh->name;
    } while (0);

    return name;
}

void gdsql_db_set_name(gdsql_db gdsql_db,
                       const char* name)
{
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        if (name == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid DB name"));
            break;
        }

        strcpy(dh->name, name);
    } while (0);
}

const char* gdsql_db_get_user(gdsql_db gdsql_db)
{
    const char* user = 0;

    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        user = dh->user;
    } while (0);

    return user;
}

void gdsql_db_set_user(gdsql_db gdsql_db,
                       const char* user)
{
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        if (user == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid DB user"));
            break;
        }

        strcpy(dh->user, user);
    } while (0);
}

const char* gdsql_db_get_password(gdsql_db gdsql_db)
{
    const char* password = 0;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        password = dh->password;
    } while (0);

    return password;
}

void gdsql_db_set_password(gdsql_db gdsql_db,
                           const char* password)
{
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        if (password == 0) {
            GDSQL_Log(LOG_WARNING,
                      ("Invalid DB password"));
            break;
        }

        strcpy(dh->password, password);
    } while (0);
}

int gdsql_db_open(gdsql_db gdsql_db)
{
    int ret = 0;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        const DbOps* ops = get_dbops(dh->type);
        if (ops != 0)
            ret = ops->db_open(dh);
    } while (0);

    return ret;
}

int gdsql_db_close(gdsql_db gdsql_db)
{
    int ret = 0;
    
    do {
        gdsql_dbh* dh = gdsql_check_db(gdsql_db);
        if (dh == 0)
            break;

        const DbOps* ops = get_dbops(dh->type);
        if (ops != 0)
            ret = ops->db_close(dh);
    } while (0);

    return ret;
}
