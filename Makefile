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
#   make wit-schema-check — validate WIT schema package
#   make wit-schema-generate — generate DBI manifest + C metadata from WIT
#   make wit-schema-cc-check — compile generated C metadata
#   make runner-wire-test — run v0 runner wire-format tests
#   make schema-check — validate schemas/dbi_manifest.csv
#   make stress-harness — run deterministic fault harness scaffold
#   make phase0-check — run phase-0 foundation checks
#   make phasea-check — run phase-0 checks + runner wire tests
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
#   WASM_TOOLS=BIN    — wasm-tools binary override

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
RUNNER_WIRE_TEST_BIN = runner_wire_test
BENCH_COUNT ?= 100000
BENCH_ROUNDS ?= 3
BENCH_BASELINE ?= benchmarks/baseline.env
DBI_MANIFEST ?= schemas/dbi_manifest.csv
WIT_SCHEMA_DIR ?= schemas/wit
WIT_SCHEMA ?= $(WIT_SCHEMA_DIR)/runtime-schema.wit
WIT_CODEGEN ?= tools/wit_schema_codegen.py
WIT_GEN_DIR ?= generated
WIT_GEN_HDR ?= $(WIT_GEN_DIR)/wit_schema_dbis.h
WIT_GEN_SRC ?= $(WIT_GEN_DIR)/wit_schema_dbis.c
WIT_GEN_OBJ ?= $(WIT_GEN_DIR)/wit_schema_dbis.o
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy
CPPCHECK ?= cppcheck
WASM_TOOLS ?= wasm-tools
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
RUNNER_WIRE_SRC = src/runner/wire_v0.c
RUNNER_WIRE_HDR = src/runner/wire_v0.h
RUNNER_WIRE_TEST_SRC = tests/unit/runner_wire_test.c
FORMAT_FILES = sapling.c sapling.h $(FAULT_SRC) $(FAULT_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(RUNNER_WIRE_TEST_SRC) tests/stress/fault_harness.c
PHASE0_TIDY_FILES = $(FAULT_SRC) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_TEST_SRC) tests/stress/fault_harness.c
PHASE0_CPPCHECK_FILES = src/common src/runner tests/unit/runner_wire_test.c tests/stress/fault_harness.c

.PHONY: all test debug asan tsan bench bench-run bench-ci wasm-lib wasm-check format format-check tidy cppcheck lint wit-schema-check wit-schema-generate wit-schema-cc-check runner-wire-test schema-check stress-harness phase0-check phasea-check clean

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

$(RUNNER_WIRE_TEST_BIN): $(RUNNER_WIRE_TEST_SRC) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_WIRE_TEST_SRC) $(RUNNER_WIRE_SRC) -o $(RUNNER_WIRE_TEST_BIN) $(LDFLAGS)

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

wit-schema-check:
	@command -v $(WASM_TOOLS) >/dev/null 2>&1 || { echo "wasm-tools not found: $(WASM_TOOLS)"; exit 2; }
	$(WASM_TOOLS) component wit $(WIT_SCHEMA_DIR) --json >/dev/null

wit-schema-generate: $(WIT_SCHEMA) $(WIT_CODEGEN)
	python3 $(WIT_CODEGEN) --wit $(WIT_SCHEMA) --manifest $(DBI_MANIFEST) --header $(WIT_GEN_HDR) --source $(WIT_GEN_SRC)

wit-schema-cc-check: wit-schema-generate
	$(CC) $(CFLAGS) $(INCLUDES) -c $(WIT_GEN_SRC) -o $(WIT_GEN_OBJ)
	rm -f $(WIT_GEN_OBJ)

schema-check: wit-schema-check wit-schema-cc-check
	python3 tools/check_dbi_manifest.py $(DBI_MANIFEST)

runner-wire-test: CFLAGS += -O2 -g
runner-wire-test: $(RUNNER_WIRE_TEST_BIN)
	./$(RUNNER_WIRE_TEST_BIN)

$(STRESS_BIN): tests/stress/fault_harness.c $(FAULT_SRC) $(FAULT_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) tests/stress/fault_harness.c $(FAULT_SRC) -o $(STRESS_BIN) $(LDFLAGS)

stress-harness: $(STRESS_BIN)
	./$(STRESS_BIN)

phase0-check: lint schema-check stress-harness

phasea-check: phase0-check runner-wire-test

clean:
	rm -f $(OBJ) $(LIB) $(TEST_BIN) $(BENCH_BIN) $(STRESS_BIN) $(RUNNER_WIRE_TEST_BIN) $(WASM_OBJ) $(WASM_LIB) $(WASM_SMOKE) $(WIT_GEN_OBJ)
