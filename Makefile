/************************************************************
 * Najira Getachew
 * CS525 - Advanced Database Organization
 * Storage Manager Implementation - Assignment 3
 ************************************************************/
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g

OBJS = dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o rm_serializer.o record_mgr.o

all: test_expr test_assign3_1

test_expr: test_expr.c dberror.o expr.o
	$(CC) $(CFLAGS) -o test_expr test_expr.c dberror.o expr.o

test_assign3_1: test_assign3_1.c $(OBJS)
	$(CC) $(CFLAGS) -o test_assign3_1 test_assign3_1.c $(OBJS)

dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c dberror.c

storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	$(CC) $(CFLAGS) -c storage_mgr.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h storage_mgr.h dberror.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

expr.o: expr.c expr.h dberror.h tables.h
	$(CC) $(CFLAGS) -c expr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	$(CC) $(CFLAGS) -c rm_serializer.c

record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h dberror.h rm_serializer.h
	$(CC) $(CFLAGS) -c record_mgr.c

clean:
	rm -f $(OBJS) test_expr test_assign3_1 *.exe *.table

.PHONY: all clean
