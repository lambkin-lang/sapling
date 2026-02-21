# Makefile for the Sapling B+ tree library
#
# Targets:
#   make              — build the static library (libsapling.a)
#   make test         — compile and run the test suite
#   make debug        — build with debug symbols, no optimisation
#   make asan         — build and test with AddressSanitizer
#   make tsan         — build and test with ThreadSanitizer (implies THREADED=1)
#   make bench        — build benchmark harness
#   make bench-run    — run sorted-load benchmark harness
#   make bench-ci     — benchmark regression guardrail against baseline
#   make wasm-lib     — build wasm32-wasi static library (requires WASI_SYSROOT)
#   make wasm-check   — build wasm32-wasi smoke module (requires WASI_SYSROOT)
#   make clean        — remove build artifacts
#
# Variables:
#   PAGE_SIZE=N       — override SAPLING_PAGE_SIZE (default 4096)
#   THREADED=1        — enable thread-safe build (-DSAPLING_THREADED -lpthread)
#   BENCH_COUNT=N     — entries per benchmark round (default 100000)
#   BENCH_ROUNDS=N    — benchmark rounds (default 3)
#   BENCH_BASELINE=F  — baseline file for bench-ci (default benchmarks/baseline.env)
#   BENCH_ALLOWED_REGRESSION_PCT=N — override baseline regression budget
#   WASI_SYSROOT=DIR  — path to wasi sysroot (required by wasm targets)

CC       ?= gcc
CFLAGS   := -Wall -Wextra -Werror -std=c99
AR        = ar
ARFLAGS   = rcs
LDFLAGS  :=

PAGE_SIZE ?= 4096
CFLAGS   += -DSAPLING_PAGE_SIZE=$(PAGE_SIZE)

ifdef THREADED
CFLAGS   += -DSAPLING_THREADED
LDFLAGS  += -lpthread
endif

LIB      = libsapling.a
OBJ      = sapling.o
TEST_BIN = test_sapling
BENCH_BIN = bench_sapling
BENCH_COUNT ?= 100000
BENCH_ROUNDS ?= 3
BENCH_BASELINE ?= benchmarks/baseline.env
WASI_CC   ?= clang
WASI_AR   ?= ar
WASI_TARGET ?= wasm32-wasi
WASI_SYSROOT ?=
WASI_CFLAGS ?= -Wall -Wextra -Werror -std=c99 -O2 -DSAPLING_PAGE_SIZE=$(PAGE_SIZE) --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT)
WASI_LDFLAGS ?= --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT)
WASM_LIB  = libsapling_wasm.a
WASM_OBJ  = sapling_wasm.o
WASM_SMOKE = wasm_smoke.wasm

.PHONY: all test debug asan tsan bench bench-run bench-ci wasm-lib wasm-check clean

all: CFLAGS += -O2
all: $(LIB)

debug: CFLAGS += -O0 -g -DDEBUG
debug: $(LIB)

$(LIB): $(OBJ)
	$(AR) $(ARFLAGS) $@ $^

sapling.o: sapling.c sapling.h
	$(CC) $(CFLAGS) -c sapling.c -o sapling.o

test: CFLAGS += -O2 -g
test: $(TEST_BIN)
	./$(TEST_BIN)

asan: CFLAGS += -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
asan: LDFLAGS += -fsanitize=address,undefined
asan: clean
	$(CC) $(CFLAGS) test_sapling.c sapling.c -o $(TEST_BIN) $(LDFLAGS)
	./$(TEST_BIN)

tsan: CFLAGS += -O1 -g -fsanitize=thread -DSAPLING_THREADED
tsan: LDFLAGS += -fsanitize=thread -lpthread
tsan: clean
	$(CC) $(CFLAGS) test_sapling.c sapling.c -o $(TEST_BIN) $(LDFLAGS)
	./$(TEST_BIN)

bench: CFLAGS += -O3 -g
bench: $(BENCH_BIN)

bench-run: CFLAGS += -O3 -g
bench-run: $(BENCH_BIN)
	./$(BENCH_BIN) --count $(BENCH_COUNT) --rounds $(BENCH_ROUNDS)

bench-ci:
	BENCH_COUNT="$(BENCH_COUNT)" \
	BENCH_ROUNDS="$(BENCH_ROUNDS)" \
	BENCH_ALLOWED_REGRESSION_PCT="$(BENCH_ALLOWED_REGRESSION_PCT)" \
	/bin/bash scripts/bench_ci.sh "$(BENCH_BASELINE)"

wasm-lib: $(WASM_LIB)

wasm-check: $(WASM_SMOKE)

$(WASM_OBJ): sapling.c sapling.h
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	$(WASI_CC) $(WASI_CFLAGS) -c sapling.c -o $(WASM_OBJ)

$(WASM_LIB): $(WASM_OBJ)
	$(WASI_AR) $(ARFLAGS) $@ $^

$(WASM_SMOKE): wasm_smoke.c sapling.c sapling.h
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	$(WASI_CC) $(WASI_CFLAGS) wasm_smoke.c sapling.c -o $(WASM_SMOKE) $(WASI_LDFLAGS)

$(TEST_BIN): test_sapling.c sapling.c sapling.h
	$(CC) $(CFLAGS) test_sapling.c sapling.c -o $(TEST_BIN) $(LDFLAGS)

$(BENCH_BIN): bench_sapling.c sapling.c sapling.h
	$(CC) $(CFLAGS) bench_sapling.c sapling.c -o $(BENCH_BIN) $(LDFLAGS)

clean:
	rm -f $(OBJ) $(LIB) $(TEST_BIN) $(BENCH_BIN) $(WASM_OBJ) $(WASM_LIB) $(WASM_SMOKE)
