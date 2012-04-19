##
# Define interesting objects just once

CC_OBJS = \
	gdsql.o \
	gdsql_db.o \
	gdsql_stmt.o \
	gdsql_util.o \
	gdsql_date.o \
	gdsql_log.o \
	gdsql_hidden.o \
	\
	gdsql_sqlite.o \
	gdsql_postgres.o \
	gdsql_mysql.o \

GDSQL_LIB = \
	libgdsql.a \


##
# Configure compiler and linker

MYSQL_DIR = "/cygdrive/c/Archivos de programa/MySQL/MySQL Server 5.5"

# CFLAGS += -DDEBUG
CFLAGS += -g
CFLAGS += -Wall
CFLAGS += -I/usr/local/include
CFLAGS += -I.

LDFLAGS += -L/usr/local/lib
LDFLAGS += -L.
LDFLAGS += -lgdsql
LDFLAGS += -lsqlite3
LDFLAGS += -lpq
LDFLAGS += -lmysqlclient -lz


##
# Rules to build the gdsql library

$(GDSQL_LIB): $(CC_OBJS)
	ar rf $@ $^


##
# Generic rules

first: all

all: $(GDSQL_LIB)

clean:
	rm -f $(GDSQL_LIB)
	rm -f *.o *~ *.exe *.exe.stackdump *.log


##
# Rules for tests

test01: test01.o $(GDSQL_LIB)
	cc -o $@ $@.o $(LDFLAGS)
