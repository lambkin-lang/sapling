# Makefile for the Sapling B+ tree library
#
# Targets:
#   make          — build the static library (libsapling.a)
#   make test     — compile and run the test suite
#   make clean    — remove build artifacts

CC      = gcc
CFLAGS  = -Wall -Wextra -Werror -O2 -std=c99
AR      = ar
ARFLAGS = rcs

LIB     = libsapling.a
OBJ     = sapling.o
TEST_BIN = test_sapling

.PHONY: all test clean

all: $(LIB)

$(LIB): $(OBJ)
	$(AR) $(ARFLAGS) $@ $^

sapling.o: sapling.c sapling.h
	$(CC) $(CFLAGS) -c sapling.c -o sapling.o

test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): test_sapling.c sapling.c sapling.h
	$(CC) $(CFLAGS) test_sapling.c sapling.c -o $(TEST_BIN)

clean:
	rm -f $(OBJ) $(LIB) $(TEST_BIN)
