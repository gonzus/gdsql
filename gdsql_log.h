#ifndef GDSQL_LOG_H
#define GDSQL_LOG_H

/*
 * This is a multi-level logging facility.  Its implementation is
 * rather convoluted, but it has two purposes in mind:
 *
 * 1. Being able to log with different levels or severities.
 *
 * 2. Being able to specify at compile time the starting severity on
 *    which we are interested; in this case, all lesser levels are
 *    completely compiled out of the code, and have no impact on
 *    performance (-DLOG_LEVEL=LOG_WARNING).
 *
 * 3. Being able to specify at run time (via an environment variable)
 *    the starting severity on which we are interested; in this case,
 *    all lesser levels are not shown, although they have an impact on
 *    performance (export LOG_LEVEL=LOG_INFO).
 *
 * The different levels are:
 *
 *   LOG_DEBUG    Very abbundant debugging messages.
 *   LOG_INFO     Informational messages.
 *   LOG_WARNING  Warning messages, from the business logic POV.
 *   LOG_ERROR    Error messages that are recoverable.
 *   LOG_FATAL    Error messages that cause program termination.
 *   LOG_ALWAYS   Messages that are ALWAYS displayed.
 */

/*
 * Error levels.
 */
#define  LOG_DEBUG      0
#define  LOG_INFO       1
#define  LOG_WARNING    2
#define  LOG_ERROR      3
#define  LOG_FATAL      4
#define  LOG_LAST       5

#define  LOG_ALWAYS    90

/*
 * Default error level.
 */
#define  LOG_DEFAULT   LOG_INFO

/*
 * If no -DLOG_LEVEL was specified at compile time, use a default
 * value.
 */
#ifndef LOG_LEVEL
#define LOG_LEVEL        LOG_DEFAULT
#endif


/*
 * Vudu magic so that when compiling with a certain log level, any
 * calls with a lower level will truly disappear from the code.
 */
#define GDSQL_LOG_GO(l, r)    (gdsql_get_log_level() > l ? gdsql_log_nothing() : \
                               (gdsql_log_before(l),                    \
                                gdsql_log_where(l, __FILE__, __LINE__), \
                                gdsql_log_head(l),                      \
                                gdsql_log_write r,                      \
                                gdsql_log_tail(l),                      \
                                gdsql_log_after(l)))


#if LOG_LEVEL <= LOG_DEBUG
#define _GDSQL_LOG_DEBUG(r)      GDSQL_LOG_GO(LOG_DEBUG, r)
#else
#define _GDSQL_LOG_DEBUG(r)      gdsql_log_nothing()
#endif

#if LOG_LEVEL <= LOG_INFO
#define _GDSQL_LOG_INFO(r)       GDSQL_LOG_GO(LOG_INFO, r)
#else
#define _GDSQL_LOG_INFO(r)       gdsql_log_nothing()
#endif

#if LOG_LEVEL <= LOG_WARNING
#define _GDSQL_LOG_WARNING(r)    GDSQL_LOG_GO(LOG_WARNING, r)
#else
#define _GDSQL_LOG_WARNING(r)    gdsql_log_nothing()
#endif

#if LOG_LEVEL <= LOG_ERROR
#define _GDSQL_LOG_ERROR(r)      GDSQL_LOG_GO(LOG_ERROR, r)
#else
#define _GDSQL_LOG_ERROR(r)      gdsql_log_nothing()
#endif

#if LOG_LEVEL <= LOG_FATAL
#define _GDSQL_LOG_FATAL(r)      GDSQL_LOG_GO(LOG_FATAL, r)
#else
#define _GDSQL_LOG_FATAL(r)      gdsql_log_nothing()
#endif


#if 1  /* always! */
#define _GDSQL_LOG_ALWAYS(r)     GDSQL_LOG_GO(LOG_ALWAYS, r)
#else
#define _GDSQL_LOG_ALWAYS(r)     gdsql_log_nothing()
#endif


#define  GDSQL_Log(l,r)          _GDSQL_##l(r)



/*
 * Actions to execute for each level before and after reporting the
 * event.
 */
int gdsql_log_before(int level);
int gdsql_log_after(int level);

/*
 * Build the log message.  Must return a value because these functions
 * are used within a comma list.
 */
int gdsql_log_where(int level,
                    const char* filename,
                    int lineno);
int gdsql_log_head(int level);
int gdsql_log_tail(int level);

/*
 * Print log message.
 */
int gdsql_log_write(const char* fmt, ...);

/*
 * Get run-time log level.
 */
int gdsql_get_log_level(void);

/*
 * Set run-time log level. Return previous level.
 */
int gdsql_set_log_level(int level);

/*
 * A do-nothing function, so that the compiler won't complain with:
 *
 * if (blah)
 *   0;
 */
int gdsql_log_nothing(void);

/*
 * A function to force shutdown of the loggin system.
 */
void gdsql_log_shutdown(void);

#endif
