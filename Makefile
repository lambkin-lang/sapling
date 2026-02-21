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
#   make format       — apply clang-format to C sources
#   make format-check — verify formatting
#   make tidy         — run clang-tidy checks
#   make cppcheck     — run cppcheck checks (skips if unavailable)
#   make lint         — run format-check + tidy + cppcheck
#   make schema-check — validate schemas/dbi_manifest.csv
#   make stress-harness — run deterministic fault harness scaffold
#   make phase0-check — run phase-0 foundation checks
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
#   CLANG_FORMAT=BIN  — clang-format binary override
#   CLANG_TIDY=BIN    — clang-tidy binary override
#   CPPCHECK=BIN      — cppcheck binary override

CC       ?= gcc
CFLAGS   := -Wall -Wextra -Werror -std=c99
INCLUDES := -Iinclude -Isrc -I.
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
STRESS_BIN = fault_harness
BENCH_COUNT ?= 100000
BENCH_ROUNDS ?= 3
BENCH_BASELINE ?= benchmarks/baseline.env
DBI_MANIFEST ?= schemas/dbi_manifest.csv
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy
CPPCHECK ?= cppcheck
WASI_CC   ?= clang
WASI_AR   ?= ar
WASI_TARGET ?= wasm32-wasi
WASI_SYSROOT ?=
WASI_CFLAGS ?= -Wall -Wextra -Werror -std=c99 -O2 -DSAPLING_PAGE_SIZE=$(PAGE_SIZE) --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT) $(INCLUDES)
WASI_LDFLAGS ?= --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT)
WASM_LIB  = libsapling_wasm.a
WASM_OBJ  = sapling_wasm.o
WASM_SMOKE = wasm_smoke.wasm
SAPLING_SRC = src/sapling/sapling.c
SAPLING_HDR = include/sapling/sapling.h
FAULT_SRC = src/common/fault_inject.c
FAULT_HDR = src/common/fault_inject.h
FORMAT_FILES = sapling.c sapling.h $(FAULT_SRC) $(FAULT_HDR) tests/stress/fault_harness.c
PHASE0_TIDY_FILES = $(FAULT_SRC) tests/stress/fault_harness.c
PHASE0_CPPCHECK_FILES = src/common tests/stress/fault_harness.c

.PHONY: all test debug asan tsan bench bench-run bench-ci wasm-lib wasm-check format format-check tidy cppcheck lint schema-check stress-harness phase0-check clean

all: CFLAGS += -O2
all: $(LIB)

debug: CFLAGS += -O0 -g -DDEBUG
debug: $(LIB)

$(LIB): $(OBJ)
	$(AR) $(ARFLAGS) $@ $^

sapling.o: $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $(SAPLING_SRC) -o sapling.o

test: CFLAGS += -O2 -g
test: $(TEST_BIN)
	./$(TEST_BIN)

asan: CFLAGS += -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
asan: LDFLAGS += -fsanitize=address,undefined
asan: clean
	$(CC) $(CFLAGS) $(INCLUDES) test_sapling.c $(SAPLING_SRC) -o $(TEST_BIN) $(LDFLAGS)
	./$(TEST_BIN)

tsan: CFLAGS += -O1 -g -fsanitize=thread -DSAPLING_THREADED
tsan: LDFLAGS += -fsanitize=thread -lpthread
tsan: clean
	$(CC) $(CFLAGS) $(INCLUDES) test_sapling.c $(SAPLING_SRC) -o $(TEST_BIN) $(LDFLAGS)
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

$(WASM_OBJ): $(SAPLING_SRC) $(SAPLING_HDR)
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	$(WASI_CC) $(WASI_CFLAGS) -c $(SAPLING_SRC) -o $(WASM_OBJ)

$(WASM_LIB): $(WASM_OBJ)
	$(WASI_AR) $(ARFLAGS) $@ $^

$(WASM_SMOKE): wasm_smoke.c $(SAPLING_SRC) $(SAPLING_HDR)
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	$(WASI_CC) $(WASI_CFLAGS) wasm_smoke.c $(SAPLING_SRC) -o $(WASM_SMOKE) $(WASI_LDFLAGS)

$(TEST_BIN): test_sapling.c $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) test_sapling.c $(SAPLING_SRC) -o $(TEST_BIN) $(LDFLAGS)

$(BENCH_BIN): bench_sapling.c $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) bench_sapling.c $(SAPLING_SRC) -o $(BENCH_BIN) $(LDFLAGS)

format:
	@command -v $(CLANG_FORMAT) >/dev/null 2>&1 || { echo "clang-format not found: $(CLANG_FORMAT)"; exit 2; }
	$(CLANG_FORMAT) -i $(FORMAT_FILES)

format-check:
	@command -v $(CLANG_FORMAT) >/dev/null 2>&1 || { echo "clang-format not found: $(CLANG_FORMAT)"; exit 2; }
	$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

tidy:
	@command -v $(CLANG_TIDY) >/dev/null 2>&1 || { echo "clang-tidy not found: $(CLANG_TIDY)"; exit 2; }
	$(CLANG_TIDY) $(PHASE0_TIDY_FILES) -- $(CFLAGS) $(INCLUDES)

cppcheck:
	@if command -v $(CPPCHECK) >/dev/null 2>&1; then \
		$(CPPCHECK) --enable=warning,performance,portability --error-exitcode=1 --std=c99 \
			-Iinclude $(PHASE0_CPPCHECK_FILES); \
	else \
		echo "cppcheck not found ($(CPPCHECK)); skipping"; \
	fi

lint: format-check tidy cppcheck

schema-check:
	python3 tools/check_dbi_manifest.py $(DBI_MANIFEST)

$(STRESS_BIN): tests/stress/fault_harness.c $(FAULT_SRC) $(FAULT_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) tests/stress/fault_harness.c $(FAULT_SRC) -o $(STRESS_BIN) $(LDFLAGS)

stress-harness: $(STRESS_BIN)
	./$(STRESS_BIN)

phase0-check: lint schema-check stress-harness

clean:
	rm -f $(OBJ) $(LIB) $(TEST_BIN) $(BENCH_BIN) $(STRESS_BIN) $(WASM_OBJ) $(WASM_LIB) $(WASM_SMOKE)
