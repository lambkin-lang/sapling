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
#   make runner-lifecycle-test — run runner lifecycle/schema-guard tests
#   make runner-txctx-test — run phase-B host tx context tests
#   make runner-txstack-test — run phase-B nested tx stack tests
#   make runner-attempt-test — run phase-B retry attempt tests
#   make runner-integration-test — run phase-B retry+nested integration test
#   make runner-mailbox-test — run phase-C mailbox lease tests
#   make runner-outbox-test — run phase-C outbox append/drain tests
#   make runner-timer-test — run phase-C timer ingestion/drain tests
#   make runner-scheduler-test — run phase-C timer scheduling helper tests
#   make runner-intent-sink-test — run composed outbox+timer intent sink tests
#   make wasi-runtime-test — run concrete wasi runtime wrapper tests
#   make wasi-shim-test — run runner<->wasi shim integration tests
#   make schema-check — validate schemas/dbi_manifest.csv
#   make stress-harness — run deterministic fault harness scaffold
#   make phase0-check — run phase-0 foundation checks
#   make phasea-check — run phase-0 checks + phase-A runner tests
#   make phaseb-check — run phase-A checks + phase-B tx context tests
#   make phasec-check — run phase-B checks + phase-C mailbox tests
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
RUNNER_LIFECYCLE_TEST_BIN = runner_lifecycle_test
RUNNER_TXCTX_TEST_BIN = runner_txctx_test
RUNNER_TXSTACK_TEST_BIN = runner_txstack_test
RUNNER_ATTEMPT_TEST_BIN = runner_attempt_test
RUNNER_INTEGRATION_TEST_BIN = runner_atomic_integration_test
RUNNER_MAILBOX_TEST_BIN = runner_mailbox_test
RUNNER_OUTBOX_TEST_BIN = runner_outbox_test
RUNNER_TIMER_TEST_BIN = runner_timer_test
RUNNER_SCHEDULER_TEST_BIN = runner_scheduler_test
RUNNER_INTENT_SINK_TEST_BIN = runner_intent_sink_test
WASI_RUNTIME_TEST_BIN = wasi_runtime_test
WASI_SHIM_TEST_BIN = wasi_shim_test
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
RUNNER_LIFECYCLE_SRC = src/runner/runner_v0.c
RUNNER_LIFECYCLE_HDR = src/runner/runner_v0.h
RUNNER_LIFECYCLE_TEST_SRC = tests/unit/runner_lifecycle_test.c
RUNNER_TXCTX_SRC = src/runner/txctx_v0.c
RUNNER_TXCTX_HDR = src/runner/txctx_v0.h
RUNNER_TXCTX_TEST_SRC = tests/unit/runner_txctx_test.c
RUNNER_TXSTACK_SRC = src/runner/txstack_v0.c
RUNNER_TXSTACK_HDR = src/runner/txstack_v0.h
RUNNER_TXSTACK_TEST_SRC = tests/unit/runner_txstack_test.c
RUNNER_ATTEMPT_SRC = src/runner/attempt_v0.c
RUNNER_ATTEMPT_HDR = src/runner/attempt_v0.h
RUNNER_ATTEMPT_TEST_SRC = tests/unit/runner_attempt_test.c
RUNNER_INTEGRATION_TEST_SRC = tests/integration/runner_atomic_integration_test.c
RUNNER_MAILBOX_SRC = src/runner/mailbox_v0.c
RUNNER_MAILBOX_HDR = src/runner/mailbox_v0.h
RUNNER_MAILBOX_TEST_SRC = tests/unit/runner_mailbox_test.c
RUNNER_OUTBOX_SRC = src/runner/outbox_v0.c
RUNNER_OUTBOX_HDR = src/runner/outbox_v0.h
RUNNER_OUTBOX_TEST_SRC = tests/unit/runner_outbox_test.c
RUNNER_TIMER_SRC = src/runner/timer_v0.c
RUNNER_TIMER_HDR = src/runner/timer_v0.h
RUNNER_TIMER_TEST_SRC = tests/unit/runner_timer_test.c
RUNNER_SCHEDULER_SRC = src/runner/scheduler_v0.c
RUNNER_SCHEDULER_HDR = src/runner/scheduler_v0.h
RUNNER_SCHEDULER_TEST_SRC = tests/unit/runner_scheduler_test.c
RUNNER_INTENT_SINK_SRC = src/runner/intent_sink_v0.c
RUNNER_INTENT_SINK_HDR = src/runner/intent_sink_v0.h
RUNNER_INTENT_SINK_TEST_SRC = tests/unit/runner_intent_sink_test.c
WASI_SHIM_SRC = src/wasi/shim_v0.c
WASI_SHIM_HDR = src/wasi/shim_v0.h
WASI_RUNTIME_SRC = src/wasi/runtime_v0.c
WASI_RUNTIME_HDR = src/wasi/runtime_v0.h
WASI_RUNTIME_TEST_SRC = tests/unit/wasi_runtime_test.c
WASI_SHIM_TEST_SRC = tests/unit/wasi_shim_test.c
FORMAT_FILES = sapling.c sapling.h $(FAULT_SRC) $(FAULT_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_OUTBOX_SRC) $(RUNNER_OUTBOX_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_INTENT_SINK_SRC) $(RUNNER_INTENT_SINK_HDR) $(WASI_RUNTIME_SRC) $(WASI_RUNTIME_HDR) $(WASI_SHIM_SRC) $(WASI_SHIM_HDR) $(RUNNER_WIRE_TEST_SRC) $(RUNNER_LIFECYCLE_TEST_SRC) $(RUNNER_TXCTX_TEST_SRC) $(RUNNER_TXSTACK_TEST_SRC) $(RUNNER_ATTEMPT_TEST_SRC) $(RUNNER_MAILBOX_TEST_SRC) $(RUNNER_OUTBOX_TEST_SRC) $(RUNNER_TIMER_TEST_SRC) $(RUNNER_SCHEDULER_TEST_SRC) $(RUNNER_INTENT_SINK_TEST_SRC) $(RUNNER_INTEGRATION_TEST_SRC) $(WASI_RUNTIME_TEST_SRC) $(WASI_SHIM_TEST_SRC) tests/stress/fault_harness.c
PHASE0_TIDY_FILES = $(FAULT_SRC) $(RUNNER_WIRE_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_OUTBOX_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_INTENT_SINK_SRC) $(WASI_RUNTIME_SRC) $(WASI_SHIM_SRC) $(RUNNER_WIRE_TEST_SRC) $(RUNNER_LIFECYCLE_TEST_SRC) $(RUNNER_TXCTX_TEST_SRC) $(RUNNER_TXSTACK_TEST_SRC) $(RUNNER_ATTEMPT_TEST_SRC) $(RUNNER_MAILBOX_TEST_SRC) $(RUNNER_OUTBOX_TEST_SRC) $(RUNNER_TIMER_TEST_SRC) $(RUNNER_SCHEDULER_TEST_SRC) $(RUNNER_INTENT_SINK_TEST_SRC) $(RUNNER_INTEGRATION_TEST_SRC) $(WASI_RUNTIME_TEST_SRC) $(WASI_SHIM_TEST_SRC) tests/stress/fault_harness.c
PHASE0_CPPCHECK_FILES = src/common src/runner src/wasi tests/unit/runner_wire_test.c tests/unit/runner_lifecycle_test.c tests/unit/runner_txctx_test.c tests/unit/runner_txstack_test.c tests/unit/runner_attempt_test.c tests/unit/runner_mailbox_test.c tests/unit/runner_outbox_test.c tests/unit/runner_timer_test.c tests/unit/runner_scheduler_test.c tests/unit/runner_intent_sink_test.c tests/integration/runner_atomic_integration_test.c tests/unit/wasi_runtime_test.c tests/unit/wasi_shim_test.c tests/stress/fault_harness.c

.PHONY: all test debug asan tsan bench bench-run bench-ci wasm-lib wasm-check format format-check tidy cppcheck lint wit-schema-check wit-schema-generate wit-schema-cc-check runner-wire-test runner-lifecycle-test runner-txctx-test runner-txstack-test runner-attempt-test runner-integration-test test-integration runner-mailbox-test runner-outbox-test runner-timer-test runner-scheduler-test runner-intent-sink-test wasi-runtime-test wasi-shim-test schema-check stress-harness phase0-check phasea-check phaseb-check phasec-check clean

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

$(RUNNER_LIFECYCLE_TEST_BIN): $(RUNNER_LIFECYCLE_TEST_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_LIFECYCLE_TEST_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(RUNNER_LIFECYCLE_TEST_BIN) $(LDFLAGS)

$(RUNNER_TXCTX_TEST_BIN): $(RUNNER_TXCTX_TEST_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_TXCTX_TEST_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) -o $(RUNNER_TXCTX_TEST_BIN) $(LDFLAGS)

$(RUNNER_TXSTACK_TEST_BIN): $(RUNNER_TXSTACK_TEST_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_TXSTACK_TEST_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) -o $(RUNNER_TXSTACK_TEST_BIN) $(LDFLAGS)

$(RUNNER_ATTEMPT_TEST_BIN): $(RUNNER_ATTEMPT_TEST_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_ATTEMPT_TEST_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) -o $(RUNNER_ATTEMPT_TEST_BIN) $(LDFLAGS)

$(RUNNER_INTEGRATION_TEST_BIN): $(RUNNER_INTEGRATION_TEST_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_INTEGRATION_TEST_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) -o $(RUNNER_INTEGRATION_TEST_BIN) $(LDFLAGS)

$(RUNNER_MAILBOX_TEST_BIN): $(RUNNER_MAILBOX_TEST_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_MAILBOX_TEST_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(RUNNER_MAILBOX_TEST_BIN) $(LDFLAGS)

$(RUNNER_OUTBOX_TEST_BIN): $(RUNNER_OUTBOX_TEST_SRC) $(RUNNER_OUTBOX_SRC) $(RUNNER_OUTBOX_HDR) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_OUTBOX_TEST_SRC) $(RUNNER_OUTBOX_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(RUNNER_OUTBOX_TEST_BIN) $(LDFLAGS)

$(RUNNER_TIMER_TEST_BIN): $(RUNNER_TIMER_TEST_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_TIMER_TEST_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(RUNNER_TIMER_TEST_BIN) $(LDFLAGS)

$(RUNNER_SCHEDULER_TEST_BIN): $(RUNNER_SCHEDULER_TEST_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_SCHEDULER_TEST_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(RUNNER_SCHEDULER_TEST_BIN) $(LDFLAGS)

$(RUNNER_INTENT_SINK_TEST_BIN): $(RUNNER_INTENT_SINK_TEST_SRC) $(RUNNER_INTENT_SINK_SRC) $(RUNNER_INTENT_SINK_HDR) $(RUNNER_OUTBOX_SRC) $(RUNNER_OUTBOX_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(SAPLING_HDR) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(RUNNER_INTENT_SINK_TEST_SRC) $(RUNNER_INTENT_SINK_SRC) $(RUNNER_OUTBOX_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(RUNNER_INTENT_SINK_TEST_BIN) $(LDFLAGS)

$(WASI_RUNTIME_TEST_BIN): $(WASI_RUNTIME_TEST_SRC) $(WASI_RUNTIME_SRC) $(WASI_RUNTIME_HDR) $(RUNNER_WIRE_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(WASI_RUNTIME_TEST_SRC) $(WASI_RUNTIME_SRC) -o $(WASI_RUNTIME_TEST_BIN) $(LDFLAGS)

$(WASI_SHIM_TEST_BIN): $(WASI_SHIM_TEST_SRC) $(WASI_SHIM_SRC) $(WASI_SHIM_HDR) $(WASI_RUNTIME_SRC) $(WASI_RUNTIME_HDR) $(RUNNER_INTENT_SINK_SRC) $(RUNNER_INTENT_SINK_HDR) $(RUNNER_OUTBOX_SRC) $(RUNNER_OUTBOX_HDR) $(RUNNER_TIMER_SRC) $(RUNNER_TIMER_HDR) $(RUNNER_ATTEMPT_SRC) $(RUNNER_ATTEMPT_HDR) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXSTACK_HDR) $(RUNNER_TXCTX_SRC) $(RUNNER_TXCTX_HDR) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_LIFECYCLE_HDR) $(RUNNER_MAILBOX_SRC) $(RUNNER_MAILBOX_HDR) $(RUNNER_SCHEDULER_SRC) $(RUNNER_SCHEDULER_HDR) $(RUNNER_WIRE_SRC) $(RUNNER_WIRE_HDR) $(SAPLING_SRC) $(WIT_GEN_SRC) $(WIT_GEN_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) $(WASI_SHIM_TEST_SRC) $(WASI_SHIM_SRC) $(WASI_RUNTIME_SRC) $(RUNNER_INTENT_SINK_SRC) $(RUNNER_OUTBOX_SRC) $(RUNNER_TIMER_SRC) $(RUNNER_ATTEMPT_SRC) $(RUNNER_TXSTACK_SRC) $(RUNNER_TXCTX_SRC) $(RUNNER_LIFECYCLE_SRC) $(RUNNER_MAILBOX_SRC) $(RUNNER_SCHEDULER_SRC) $(RUNNER_WIRE_SRC) $(SAPLING_SRC) $(WIT_GEN_SRC) -o $(WASI_SHIM_TEST_BIN) $(LDFLAGS)

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

runner-lifecycle-test: CFLAGS += -O2 -g
runner-lifecycle-test: wit-schema-generate $(RUNNER_LIFECYCLE_TEST_BIN)
	./$(RUNNER_LIFECYCLE_TEST_BIN)

runner-txctx-test: CFLAGS += -O2 -g
runner-txctx-test: $(RUNNER_TXCTX_TEST_BIN)
	./$(RUNNER_TXCTX_TEST_BIN)

runner-txstack-test: CFLAGS += -O2 -g
runner-txstack-test: $(RUNNER_TXSTACK_TEST_BIN)
	./$(RUNNER_TXSTACK_TEST_BIN)

runner-attempt-test: CFLAGS += -O2 -g
runner-attempt-test: $(RUNNER_ATTEMPT_TEST_BIN)
	./$(RUNNER_ATTEMPT_TEST_BIN)

runner-integration-test: CFLAGS += -O2 -g
runner-integration-test: $(RUNNER_INTEGRATION_TEST_BIN)
	./$(RUNNER_INTEGRATION_TEST_BIN)

test-integration: runner-integration-test

runner-mailbox-test: CFLAGS += -O2 -g
runner-mailbox-test: wit-schema-generate $(RUNNER_MAILBOX_TEST_BIN)
	./$(RUNNER_MAILBOX_TEST_BIN)

runner-outbox-test: CFLAGS += -O2 -g
runner-outbox-test: wit-schema-generate $(RUNNER_OUTBOX_TEST_BIN)
	./$(RUNNER_OUTBOX_TEST_BIN)

runner-timer-test: CFLAGS += -O2 -g
runner-timer-test: wit-schema-generate $(RUNNER_TIMER_TEST_BIN)
	./$(RUNNER_TIMER_TEST_BIN)

runner-scheduler-test: CFLAGS += -O2 -g
runner-scheduler-test: wit-schema-generate $(RUNNER_SCHEDULER_TEST_BIN)
	./$(RUNNER_SCHEDULER_TEST_BIN)

runner-intent-sink-test: CFLAGS += -O2 -g
runner-intent-sink-test: wit-schema-generate $(RUNNER_INTENT_SINK_TEST_BIN)
	./$(RUNNER_INTENT_SINK_TEST_BIN)

wasi-runtime-test: CFLAGS += -O2 -g
wasi-runtime-test: $(WASI_RUNTIME_TEST_BIN)
	./$(WASI_RUNTIME_TEST_BIN)

wasi-shim-test: CFLAGS += -O2 -g
wasi-shim-test: wit-schema-generate $(WASI_SHIM_TEST_BIN)
	./$(WASI_SHIM_TEST_BIN)

$(STRESS_BIN): tests/stress/fault_harness.c $(FAULT_SRC) $(FAULT_HDR)
	$(CC) $(CFLAGS) $(INCLUDES) tests/stress/fault_harness.c $(FAULT_SRC) -o $(STRESS_BIN) $(LDFLAGS)

stress-harness: $(STRESS_BIN)
	./$(STRESS_BIN)

phase0-check: lint schema-check stress-harness

phasea-check: phase0-check runner-wire-test runner-lifecycle-test wasi-runtime-test wasi-shim-test

phaseb-check: phasea-check runner-txctx-test runner-txstack-test runner-attempt-test runner-integration-test

phasec-check: phaseb-check runner-mailbox-test runner-outbox-test runner-timer-test runner-scheduler-test runner-intent-sink-test

clean:
	rm -f $(OBJ) $(LIB) $(TEST_BIN) $(BENCH_BIN) $(STRESS_BIN) $(RUNNER_WIRE_TEST_BIN) $(RUNNER_LIFECYCLE_TEST_BIN) $(RUNNER_TXCTX_TEST_BIN) $(RUNNER_TXSTACK_TEST_BIN) $(RUNNER_ATTEMPT_TEST_BIN) $(RUNNER_INTEGRATION_TEST_BIN) $(RUNNER_MAILBOX_TEST_BIN) $(RUNNER_OUTBOX_TEST_BIN) $(RUNNER_TIMER_TEST_BIN) $(RUNNER_SCHEDULER_TEST_BIN) $(RUNNER_INTENT_SINK_TEST_BIN) $(WASI_RUNTIME_TEST_BIN) $(WASI_SHIM_TEST_BIN) $(WASM_OBJ) $(WASM_LIB) $(WASM_SMOKE) $(WIT_GEN_OBJ)
