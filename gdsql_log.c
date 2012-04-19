#include <errno.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <gdsql_util.h>
#include <gdsql_date.h>

#include <gdsql_log.h>

#define LOG_LEVEL_ENV "GDSQL_LOG_LEVEL"
#define LOG_NAME      "gdsql"
#define LOG_EXT       "log"

static int init(void);
static FILE* get_stream(void);
static const char* error_text(int errnum);
static const char* file_name(const char* full, char* buf);
static int stack_trace(int level, int skip);


/*
 * Esta variable indica el nivel de severidad que se usara al correr
 * el programa. Se puede modificar sin recompilar. La variable se
 * leera desde la variable de ambiente GDSQL_LOG_LEVEL.
 */
static int log_level = -1;
static char log_name[256];


int gdsql_log_before(int level)
{
    return level;
}

int gdsql_log_after(int level)
{
    if (level == LOG_FATAL)
        exit(level);

    return level;
}


int gdsql_log_where(int level,
                    const char* filename,
                    int lineno)
{
    FILE* f;
    int Y, M, D, h, m, s;
    char buf[256];

    if (level == LOG_ALWAYS)
        return level;

    f = get_stream();
    if (f == 0)
        return level;

    gdsql_get_now(&Y, &M, &D, &h, &m, &s, 0);
    fprintf(f, "%04d%02d%02d %02d%02d%02d %s:%d ",
            Y, M, D, h, m, s, file_name(filename, buf), lineno);
    return level;
}

int gdsql_log_head(int level)
{
    FILE* f;

    f = get_stream();
    if (f == 0)
        return level;

    if (level == LOG_FATAL)
        fprintf(f, "Fatal -- ");
    else if (level == LOG_ERROR)
        fprintf(f, "Error -- ");
    else if (level == LOG_WARNING)
        fprintf(f, "Warning -- ");

    return level;
}

int gdsql_log_tail(int level)
{
    FILE* f;

    f = get_stream();
    if (f == 0)
        return level;

    if (level == LOG_FATAL || level == LOG_ERROR) {
        int errnum = errno;
        const char* errtxt = error_text(errnum);
        if (errtxt == 0)
            errtxt = "UNKNOWN";

        fprintf(f, " (%d: %s)\n", errnum, errtxt);
        if (level == LOG_FATAL)
            stack_trace(level, 2);
    } else if (level != LOG_ALWAYS)
        fprintf(f, "\n");

    fflush(f);
    return level;
}

int gdsql_log_write(const char* fmt,
                    ...)
{
    va_list vl;
    FILE* f;

    f = get_stream();
    if (f == 0)
        return 0;

    va_start(vl, fmt);
    vfprintf(f, fmt, vl);
    va_end(vl);

    return 0;
}


int gdsql_log_nothing(void)
{
    return 0;
}


int gdsql_get_log_level(void)
{
    if (log_level == -1)
        log_level = init();

    return log_level;
}

int gdsql_set_log_level(int level)
{
    int current = gdsql_get_log_level();
    log_level = level;
    return current;
}

void gdsql_log_shutdown(void)
{
    FILE* fp = get_stream();
    if (fp != 0 &&
        fp != stderr)
        fclose(fp);

    log_name[0] = '\0';
}


static int get_level(const char* buf)
{
    static const char* clevel[LOG_LAST] = {
        "LOG_DEBUG",
        "LOG_INFO",
        "LOG_WARNING",
        "LOG_ERROR",
        "LOG_FATAL"
    };
    int i;
    int l;

    // String vacio? Retornamos -1.
    if (buf == 0 || buf[0] == '\0')
        return -1;

    // String numerico? Retornamos su valor, siempre que este en el
    // rango [0,LOG_LAST).
    l = 0;
    for (i = 0; buf[i] != 0; ++i) {
        if (! isdigit((int) buf[i]))
            break;
        l = l * 10 + buf[i] - '0';
    }
    if (buf[i] == 0 &&
        l >= 0 &&
        l < LOG_LAST)
        return l;

    // Buscamos el string en la lista de posibles valores.
    for (l = 0; l < LOG_LAST; ++l) {
        if (strcmp(buf, clevel[l]) == 0) {
            return l;
            break;
        }
    }

    // Mala cosa, retornamos -1.
    return -1;
}

static int level = -1;

//
// Setea el nivel dinamico de severidad en uno de los siguientes valores:
//
// 1. Archivo de configuracion, llave "log_level"
// 2. Variable de ambiente GDSQL_LOG_LEVEL
// 3. Default
//
// Los valores posibles para el nivel son:
//
//   LOG_DEBUG
//   LOG_INFO
//   LOG_WARNING
//   LOG_ERROR
//   LOG_FATAL
//
static int init(void)
{
    const char* env;

    // Si ya se definio un nivel, lo retornamos.
    if (level >= 0)
        return level;

    // Si la variable de ambiente esta definida, usamos ese valor.
    if (level < 0) {
        env = gdsql_getenv(LOG_LEVEL_ENV, 0);
        level = get_level(env);
    }

    // Usamos el valor default.
    if (level < 0)
        level = LOG_DEFAULT;

    return level;
}

static FILE* get_stream(void)
{
    static FILE* stream = 0;
    static int configured = 0;
    static int level = 0;

    if (! configured) {
        ++level;
        if (level <= 1) {
            stream = stderr;
            do {
                unsigned int pid;
                FILE* s;

                pid = gdsql_getpid();
                sprintf(log_name, "%s-%u.%s",
                        LOG_NAME, pid, LOG_EXT);
                s = fopen(log_name, "a");
                if (s != 0)
                    stream = s;
            } while (0);
            configured = 1;
        }
        --level;
    }
    
    return stream;
}

static const char* error_text(int errnum)
{
    return strerror(errnum);
}

static const char* file_name(const char* full, char* buf)
{
    int s = gdsql_file_base(full);
    strcpy(buf, full + s);
    return buf;
}

static int stack_trace(int level, int skip)
{
    return level;
}
