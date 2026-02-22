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
#   make runner-lifecycle-threaded-tsan-test — run threaded lifecycle test under TSan
#   make runner-txctx-test — run phase-B host tx context tests
#   make runner-txstack-test — run phase-B nested tx stack tests
#   make runner-attempt-test — run phase-B retry attempt tests
#   make runner-attempt-handler-test — run generic attempt-backed handler adapter tests
#   make runner-integration-test — run phase-B retry+nested integration test
#   make runner-recovery-test — run runner checkpoint/restore recovery integration test
#   make runner-mailbox-test — run phase-C mailbox lease tests
#   make runner-dead-letter-test — run phase-C dead-letter move/replay/drain tests
#   make runner-outbox-test — run phase-C outbox append/drain tests
#   make runner-timer-test — run phase-C timer ingestion/drain tests
#   make runner-scheduler-test — run phase-C timer scheduling helper tests
#   make runner-intent-sink-test — run composed outbox+timer intent sink tests
#   make runner-native-example — run non-WASI attempt-handler integration example
#   make runner-threaded-pipeline-example — run threaded 4-worker order pipeline example
#   make runner-multiwriter-stress-build — build threaded runner-style multi-writer stress harness
#   make runner-multiwriter-stress — run threaded runner-style multi-writer stress harness (investigative repro)
#   make runner-phasee-bench — build phase-E runner coupling study benchmark
#   make runner-phasee-bench-run — run phase-E runner coupling study benchmark
#   make runner-release-checklist — run phase-F runner release checklist automation
#   make wasi-runtime-test — run concrete wasi runtime wrapper tests
#   make wasi-shim-test — run runner<->wasi shim integration tests
#   make schema-check — validate schemas/dbi_manifest.csv
#   make runner-dbi-status-check — validate runner DBI status drift
#   make stress-harness — run deterministic fault harness scaffold
#   make phase0-check — run phase-0 foundation checks
#   make phasea-check — run phase-0 checks + phase-A runner tests
#   make phaseb-check — run phase-A checks + phase-B tx/attempt tests
#   make phasec-check — run phase-B checks + phase-C mailbox/dead-letter/outbox/timer tests
#   make clean        — remove build artifacts
#
# Variables:
#   PAGE_SIZE=N       — override SAPLING_PAGE_SIZE (default 4096)
#   THREADED=1        — enable thread-safe build (-DSAPLING_THREADED -lpthread)
#   BENCH_COUNT=N     — entries per benchmark round (default 100000)
#   BENCH_ROUNDS=N    — benchmark rounds (default 3)
#   BENCH_BASELINE=F  — baseline file for bench-ci (default benchmarks/baseline.env)
#   BENCH_ALLOWED_REGRESSION_PCT=N — override baseline regression budget
#   RUNNER_PHASEE_BENCH_COUNT=N — message count for Phase E study benchmark
#   RUNNER_PHASEE_BENCH_ROUNDS=N — rounds for Phase E study benchmark
#   RUNNER_PHASEE_BENCH_BATCH=N — batch size for baseline poll scenario
#   RUNNER_RELEASE_BENCH_COUNT=N — count for release checklist benchmark step
#   RUNNER_RELEASE_BENCH_ROUNDS=N — rounds for release checklist benchmark step
#   RUNNER_RELEASE_BENCH_BATCH=N — batch size for release checklist benchmark step
#   RUNNER_MULTIWRITER_STRESS_ROUNDS=N — rounds for threaded multiwriter stress
#   RUNNER_MULTIWRITER_STRESS_ORDERS=N — orders per round for multiwriter stress
#   RUNNER_MULTIWRITER_STRESS_TIMEOUT_MS=N — timeout per round for stress harness
#   WASI_SYSROOT=DIR  — path to wasi sysroot (required by wasm targets)
#   CLANG_FORMAT=BIN  — clang-format binary override
#   CLANG_TIDY=BIN    — clang-tidy binary override
#   CPPCHECK=BIN      — cppcheck binary override
#   WASM_TOOLS=BIN    — wasm-tools binary override

CC       ?= /opt/homebrew/opt/llvm@21/bin/clang
CFLAGS   := -Wall -Wextra -Werror -std=c11
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
OBJ      = src/sapling/sapling.o
TEST_BIN = test_sapling
BENCH_BIN = bench_sapling
STRESS_BIN = fault_harness
RUNNER_WIRE_TEST_BIN = runner_wire_test
RUNNER_LIFECYCLE_TEST_BIN = runner_lifecycle_test
RUNNER_LIFECYCLE_TSAN_TEST_BIN = runner_lifecycle_test_tsan
RUNNER_TXCTX_TEST_BIN = runner_txctx_test
RUNNER_TXSTACK_TEST_BIN = runner_txstack_test
RUNNER_ATTEMPT_TEST_BIN = runner_attempt_test
RUNNER_ATTEMPT_HANDLER_TEST_BIN = runner_attempt_handler_test
RUNNER_INTEGRATION_TEST_BIN = runner_atomic_integration_test
RUNNER_RECOVERY_TEST_BIN = runner_recovery_integration_test
RUNNER_MAILBOX_TEST_BIN = runner_mailbox_test
RUNNER_DEAD_LETTER_TEST_BIN = runner_dead_letter_test
RUNNER_OUTBOX_TEST_BIN = runner_outbox_test
RUNNER_TIMER_TEST_BIN = runner_timer_test
RUNNER_SCHEDULER_TEST_BIN = runner_scheduler_test
RUNNER_INTENT_SINK_TEST_BIN = runner_intent_sink_test
RUNNER_NATIVE_EXAMPLE_BIN = runner_native_example
RUNNER_THREADED_PIPELINE_EXAMPLE_BIN = runner_threaded_pipeline_example
RUNNER_MULTIWRITER_STRESS_BIN = runner_multiwriter_stress
RUNNER_PHASEE_BENCH_BIN = bench_runner_phasee
WASI_RUNTIME_TEST_BIN = wasi_runtime_test
WASI_SHIM_TEST_BIN = wasi_shim_test
BENCH_COUNT ?= 100000
BENCH_ROUNDS ?= 3
RUNNER_PHASEE_BENCH_COUNT ?= 5000
RUNNER_PHASEE_BENCH_ROUNDS ?= 5
RUNNER_PHASEE_BENCH_BATCH ?= 64
RUNNER_RELEASE_BENCH_COUNT ?= 5000
RUNNER_RELEASE_BENCH_ROUNDS ?= 5
RUNNER_RELEASE_BENCH_BATCH ?= 64
RUNNER_MULTIWRITER_STRESS_ROUNDS ?= 8
RUNNER_MULTIWRITER_STRESS_ORDERS ?= 64
RUNNER_MULTIWRITER_STRESS_TIMEOUT_MS ?= 5000
BENCH_BASELINE ?= benchmarks/baseline.env
DBI_MANIFEST ?= schemas/dbi_manifest.csv
WIT_SCHEMA_DIR ?= schemas/wit
WIT_SCHEMA ?= $(WIT_SCHEMA_DIR)/runtime-schema.wit
WIT_CODEGEN ?= tools/wit_schema_codegen.py
WIT_GEN_DIR ?= generated
WIT_GEN_HDR ?= $(WIT_GEN_DIR)/wit_schema_dbis.h
WIT_GEN_SRC ?= $(WIT_GEN_DIR)/wit_schema_dbis.c
WIT_GEN_OBJ ?= $(WIT_GEN_DIR)/wit_schema_dbis.o
CLANG_FORMAT ?= /opt/homebrew/opt/llvm@21/bin/clang-format
CLANG_TIDY ?= /opt/homebrew/opt/llvm@21/bin/clang-tidy
CPPCHECK ?= cppcheck
WASM_TOOLS ?= wasm-tools
WASI_CC   ?= /opt/homebrew/opt/llvm@21/bin/clang
WASI_AR   ?= ar
WASI_TARGET ?= wasm32-wasi
WASI_SYSROOT ?=
WASI_CFLAGS ?= -Wall -Wextra -Werror -std=c11 -O2 -DSAPLING_PAGE_SIZE=$(PAGE_SIZE) --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT) $(INCLUDES)
WASI_LDFLAGS ?= --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT)
WASM_LIB  = libsapling_wasm.a
WASM_OBJ  = sapling_wasm.o
WASM_SMOKE = wasm_smoke.wasm
# Dynamic Source Discovery
C_SOURCES := $(shell find src tests benchmarks examples -type f -name '*.c')
C_HEADERS := $(shell find src tests benchmarks examples -type f -name '*.h')

FORMAT_FILES := $(C_SOURCES) $(C_HEADERS)
PHASE0_TIDY_FILES := $(filter-out generated/%, $(C_SOURCES))
PHASE0_CPPCHECK_FILES := src tests examples benchmarks

SAPLING_SRC := src/sapling/sapling.c
SAPLING_HDR := include/sapling/sapling.h

WIT_GEN_SRC ?= $(WIT_GEN_DIR)/wit_schema_dbis.c
WIT_GEN_HDR ?= $(WIT_GEN_DIR)/wit_schema_dbis.h

CORE_OBJS := src/sapling/sapling.o
COMMON_OBJS := $(patsubst %.c,%.o,$(filter src/common/%, $(C_SOURCES)))
RUNNER_OBJS := $(patsubst %.c,%.o,$(filter src/runner/%, $(C_SOURCES)))
WASI_OBJS := $(patsubst %.c,%.o,$(filter src/wasi/%, $(C_SOURCES)))
WIT_GEN_OBJ := $(WIT_GEN_DIR)/wit_schema_dbis.o

ALL_LIB_OBJS := $(CORE_OBJS) $(COMMON_OBJS) $(RUNNER_OBJS) $(WASI_OBJS) $(WIT_GEN_OBJ)

.PHONY: all test debug asan tsan bench bench-run bench-ci wasm-lib wasm-check format format-check tidy cppcheck lint wit-schema-check wit-schema-generate wit-schema-cc-check runner-wire-test runner-lifecycle-test runner-lifecycle-threaded-tsan-test runner-txctx-test runner-txstack-test runner-attempt-test runner-attempt-handler-test runner-integration-test runner-recovery-test test-integration runner-mailbox-test runner-dead-letter-test runner-outbox-test runner-timer-test runner-scheduler-test runner-intent-sink-test runner-native-example runner-threaded-pipeline-example runner-multiwriter-stress-build runner-multiwriter-stress runner-phasee-bench runner-phasee-bench-run runner-release-checklist wasi-runtime-test wasi-shim-test schema-check runner-dbi-status-check stress-harness phase0-check phasea-check phaseb-check phasec-check clean

all: CFLAGS += -O2
all: $(LIB)

debug: CFLAGS += -O0 -g -DDEBUG
debug: $(LIB)

$(LIB): $(OBJ)
	$(AR) $(ARFLAGS) $@ $^


test: CFLAGS += -O2 -g
test: $(TEST_BIN)
	./$(TEST_BIN)

asan: CFLAGS += -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
asan: LDFLAGS += -fsanitize=address,undefined
asan: clean
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_sapling.c $(SAPLING_SRC) -o $(TEST_BIN) $(LDFLAGS)
	./$(TEST_BIN)

tsan: CFLAGS += -O1 -g -fsanitize=thread -DSAPLING_THREADED
tsan: LDFLAGS += -fsanitize=thread -lpthread
tsan: clean
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_sapling.c $(SAPLING_SRC) -o $(TEST_BIN) $(LDFLAGS)
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

$(WASM_SMOKE): tests/unit/wasm_smoke.c $(SAPLING_SRC) $(SAPLING_HDR)
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	$(WASI_CC) $(WASI_CFLAGS) tests/unit/wasm_smoke.c $(SAPLING_SRC) -o $(WASM_SMOKE) $(WASI_LDFLAGS)


# Generic object compilation
%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(TEST_BIN): tests/unit/test_sapling.o $(CORE_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_sapling.o $(CORE_OBJS) -o $(TEST_BIN) $(LDFLAGS)

$(BENCH_BIN): benchmarks/bench_sapling.o $(CORE_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) benchmarks/bench_sapling.o $(CORE_OBJS) -o $(BENCH_BIN) $(LDFLAGS)

$(RUNNER_WIRE_TEST_BIN): tests/unit/runner_wire_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_wire_test.o $(ALL_LIB_OBJS) -o $(RUNNER_WIRE_TEST_BIN) $(LDFLAGS)

$(RUNNER_LIFECYCLE_TEST_BIN): tests/unit/runner_lifecycle_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_lifecycle_test.o $(ALL_LIB_OBJS) -o $(RUNNER_LIFECYCLE_TEST_BIN) $(LDFLAGS)

$(RUNNER_TXCTX_TEST_BIN): tests/unit/runner_txctx_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_txctx_test.o $(ALL_LIB_OBJS) -o $(RUNNER_TXCTX_TEST_BIN) $(LDFLAGS)

$(RUNNER_TXSTACK_TEST_BIN): tests/unit/runner_txstack_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_txstack_test.o $(ALL_LIB_OBJS) -o $(RUNNER_TXSTACK_TEST_BIN) $(LDFLAGS)

$(RUNNER_ATTEMPT_TEST_BIN): tests/unit/runner_attempt_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_attempt_test.o $(ALL_LIB_OBJS) -o $(RUNNER_ATTEMPT_TEST_BIN) $(LDFLAGS)

$(RUNNER_ATTEMPT_HANDLER_TEST_BIN): tests/unit/runner_attempt_handler_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_attempt_handler_test.o $(ALL_LIB_OBJS) -o $(RUNNER_ATTEMPT_HANDLER_TEST_BIN) $(LDFLAGS)

$(RUNNER_INTEGRATION_TEST_BIN): tests/integration/runner_atomic_integration_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/integration/runner_atomic_integration_test.o $(ALL_LIB_OBJS) -o $(RUNNER_INTEGRATION_TEST_BIN) $(LDFLAGS)

$(RUNNER_RECOVERY_TEST_BIN): tests/integration/runner_recovery_integration_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/integration/runner_recovery_integration_test.o $(ALL_LIB_OBJS) -o $(RUNNER_RECOVERY_TEST_BIN) $(LDFLAGS)

$(RUNNER_MAILBOX_TEST_BIN): tests/unit/runner_mailbox_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_mailbox_test.o $(ALL_LIB_OBJS) -o $(RUNNER_MAILBOX_TEST_BIN) $(LDFLAGS)

$(RUNNER_DEAD_LETTER_TEST_BIN): tests/unit/runner_dead_letter_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_dead_letter_test.o $(ALL_LIB_OBJS) -o $(RUNNER_DEAD_LETTER_TEST_BIN) $(LDFLAGS)

$(RUNNER_OUTBOX_TEST_BIN): tests/unit/runner_outbox_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_outbox_test.o $(ALL_LIB_OBJS) -o $(RUNNER_OUTBOX_TEST_BIN) $(LDFLAGS)

$(RUNNER_TIMER_TEST_BIN): tests/unit/runner_timer_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_timer_test.o $(ALL_LIB_OBJS) -o $(RUNNER_TIMER_TEST_BIN) $(LDFLAGS)

$(RUNNER_SCHEDULER_TEST_BIN): tests/unit/runner_scheduler_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_scheduler_test.o $(ALL_LIB_OBJS) -o $(RUNNER_SCHEDULER_TEST_BIN) $(LDFLAGS)

$(RUNNER_INTENT_SINK_TEST_BIN): tests/unit/runner_intent_sink_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/runner_intent_sink_test.o $(ALL_LIB_OBJS) -o $(RUNNER_INTENT_SINK_TEST_BIN) $(LDFLAGS)

$(RUNNER_NATIVE_EXAMPLE_BIN): examples/native/runner_native_example.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) examples/native/runner_native_example.o $(ALL_LIB_OBJS) -o $(RUNNER_NATIVE_EXAMPLE_BIN) $(LDFLAGS)

$(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN): examples/native/runner_threaded_pipeline_example.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) examples/native/runner_threaded_pipeline_example.o $(ALL_LIB_OBJS) -o $(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN) $(LDFLAGS)

$(RUNNER_MULTIWRITER_STRESS_BIN): tests/stress/runner_multiwriter_stress.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/stress/runner_multiwriter_stress.o $(ALL_LIB_OBJS) -o $(RUNNER_MULTIWRITER_STRESS_BIN) $(LDFLAGS)

$(RUNNER_PHASEE_BENCH_BIN): benchmarks/bench_runner_phasee.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) benchmarks/bench_runner_phasee.o $(ALL_LIB_OBJS) -o $(RUNNER_PHASEE_BENCH_BIN) $(LDFLAGS)

$(WASI_RUNTIME_TEST_BIN): tests/unit/wasi_runtime_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/wasi_runtime_test.o $(ALL_LIB_OBJS) -o $(WASI_RUNTIME_TEST_BIN) $(LDFLAGS)

$(WASI_SHIM_TEST_BIN): tests/unit/wasi_shim_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/wasi_shim_test.o $(ALL_LIB_OBJS) -o $(WASI_SHIM_TEST_BIN) $(LDFLAGS)

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
		$(CPPCHECK) --enable=warning,performance,portability --error-exitcode=1 --std=c11 \
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

schema-check: wit-schema-check wit-schema-cc-check
	python3 tools/check_dbi_manifest.py $(DBI_MANIFEST)
	$(MAKE) runner-dbi-status-check

runner-dbi-status-check: wit-schema-generate
	python3 tools/check_runner_dbi_status.py $(DBI_MANIFEST) $(WIT_GEN_HDR) .

runner-wire-test: CFLAGS += -O2 -g
runner-wire-test: $(RUNNER_WIRE_TEST_BIN)
	./$(RUNNER_WIRE_TEST_BIN)

runner-lifecycle-test: CFLAGS += -O2 -g
runner-lifecycle-test: wit-schema-generate $(RUNNER_LIFECYCLE_TEST_BIN)
	./$(RUNNER_LIFECYCLE_TEST_BIN)

$(RUNNER_LIFECYCLE_TSAN_TEST_BIN): tests/unit/runner_lifecycle_test.c $(C_SOURCES) $(WIT_GEN_SRC)
	$(CC) -Wall -Wextra -Werror -std=c11 -DSAPLING_PAGE_SIZE=$(PAGE_SIZE) -DSAPLING_THREADED -O1 -fsanitize=thread $(INCLUDES) tests/unit/runner_lifecycle_test.c $(filter src/runner/%, $(C_SOURCES)) $(CORE_OBJS:.o=.c) $(COMMON_OBJS:.o=.c) $(WASI_OBJS:.o=.c) $(WIT_GEN_SRC) -o $(RUNNER_LIFECYCLE_TSAN_TEST_BIN) -fsanitize=thread -lpthread

runner-lifecycle-threaded-tsan-test: wit-schema-generate $(RUNNER_LIFECYCLE_TSAN_TEST_BIN)
	./$(RUNNER_LIFECYCLE_TSAN_TEST_BIN)
	rm -f $(RUNNER_LIFECYCLE_TSAN_TEST_BIN)

runner-txctx-test: CFLAGS += -O2 -g
runner-txctx-test: $(RUNNER_TXCTX_TEST_BIN)
	./$(RUNNER_TXCTX_TEST_BIN)

runner-txstack-test: CFLAGS += -O2 -g
runner-txstack-test: $(RUNNER_TXSTACK_TEST_BIN)
	./$(RUNNER_TXSTACK_TEST_BIN)

runner-attempt-test: CFLAGS += -O2 -g
runner-attempt-test: $(RUNNER_ATTEMPT_TEST_BIN)
	./$(RUNNER_ATTEMPT_TEST_BIN)

runner-attempt-handler-test: CFLAGS += -O2 -g
runner-attempt-handler-test: wit-schema-generate $(RUNNER_ATTEMPT_HANDLER_TEST_BIN)
	./$(RUNNER_ATTEMPT_HANDLER_TEST_BIN)

runner-integration-test: CFLAGS += -O2 -g
runner-integration-test: $(RUNNER_INTEGRATION_TEST_BIN)
	./$(RUNNER_INTEGRATION_TEST_BIN)

runner-recovery-test: CFLAGS += -O2 -g
runner-recovery-test: wit-schema-generate $(RUNNER_RECOVERY_TEST_BIN)
	./$(RUNNER_RECOVERY_TEST_BIN)

test-integration: runner-integration-test

runner-mailbox-test: CFLAGS += -O2 -g
runner-mailbox-test: wit-schema-generate $(RUNNER_MAILBOX_TEST_BIN)
	./$(RUNNER_MAILBOX_TEST_BIN)

runner-dead-letter-test: CFLAGS += -O2 -g
runner-dead-letter-test: wit-schema-generate $(RUNNER_DEAD_LETTER_TEST_BIN)
	./$(RUNNER_DEAD_LETTER_TEST_BIN)

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

runner-native-example: CFLAGS += -O2 -g
runner-native-example: wit-schema-generate $(RUNNER_NATIVE_EXAMPLE_BIN)
	./$(RUNNER_NATIVE_EXAMPLE_BIN)

runner-threaded-pipeline-example: CFLAGS += -O2 -g -DSAPLING_THREADED
runner-threaded-pipeline-example: LDFLAGS += -lpthread
runner-threaded-pipeline-example: $(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN)
	./$(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN)

runner-multiwriter-stress-build: CFLAGS += -O2 -g -DSAPLING_THREADED
runner-multiwriter-stress-build: LDFLAGS += -lpthread
runner-multiwriter-stress-build: wit-schema-generate $(RUNNER_MULTIWRITER_STRESS_BIN)

runner-multiwriter-stress: runner-multiwriter-stress-build
	RUNNER_MULTIWRITER_STRESS_ROUNDS=$(RUNNER_MULTIWRITER_STRESS_ROUNDS) \
	RUNNER_MULTIWRITER_STRESS_ORDERS=$(RUNNER_MULTIWRITER_STRESS_ORDERS) \
	RUNNER_MULTIWRITER_STRESS_TIMEOUT_MS=$(RUNNER_MULTIWRITER_STRESS_TIMEOUT_MS) \
	./$(RUNNER_MULTIWRITER_STRESS_BIN)

runner-phasee-bench: CFLAGS += -O3 -g
runner-phasee-bench: wit-schema-generate $(RUNNER_PHASEE_BENCH_BIN)

runner-phasee-bench-run: CFLAGS += -O3 -g
runner-phasee-bench-run: wit-schema-generate $(RUNNER_PHASEE_BENCH_BIN)
	./$(RUNNER_PHASEE_BENCH_BIN) --count $(RUNNER_PHASEE_BENCH_COUNT) --rounds $(RUNNER_PHASEE_BENCH_ROUNDS) --batch $(RUNNER_PHASEE_BENCH_BATCH)

runner-release-checklist:
	$(MAKE) phasec-check
	$(MAKE) runner-phasee-bench-run \
		RUNNER_PHASEE_BENCH_COUNT=$(RUNNER_RELEASE_BENCH_COUNT) \
		RUNNER_PHASEE_BENCH_ROUNDS=$(RUNNER_RELEASE_BENCH_ROUNDS) \
		RUNNER_PHASEE_BENCH_BATCH=$(RUNNER_RELEASE_BENCH_BATCH)

wasi-runtime-test: CFLAGS += -O2 -g
wasi-runtime-test: $(WASI_RUNTIME_TEST_BIN)
	./$(WASI_RUNTIME_TEST_BIN)

wasi-shim-test: CFLAGS += -O2 -g
wasi-shim-test: wit-schema-generate $(WASI_SHIM_TEST_BIN)
	./$(WASI_SHIM_TEST_BIN)

$(STRESS_BIN): tests/stress/fault_harness.o src/common/fault_inject.o $(CORE_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) tests/stress/fault_harness.o src/common/fault_inject.o $(CORE_OBJS) -o $(STRESS_BIN) $(LDFLAGS)

stress-harness: $(STRESS_BIN)
	./$(STRESS_BIN)

phase0-check: lint schema-check stress-harness

phasea-check: phase0-check runner-wire-test runner-lifecycle-test runner-lifecycle-threaded-tsan-test wasi-runtime-test wasi-shim-test

phaseb-check: phasea-check runner-txctx-test runner-txstack-test runner-attempt-test runner-attempt-handler-test runner-integration-test

phasec-check: phaseb-check runner-mailbox-test runner-dead-letter-test runner-outbox-test runner-timer-test runner-scheduler-test runner-intent-sink-test runner-native-example runner-threaded-pipeline-example runner-multiwriter-stress-build runner-recovery-test

clean:
	find . -type f \( -name '*.o' -o -name '*.a' -o -name '*.wasm' \) -delete
	rm -f $(TEST_BIN) $(BENCH_BIN) $(STRESS_BIN) $(RUNNER_WIRE_TEST_BIN) $(RUNNER_LIFECYCLE_TEST_BIN) $(RUNNER_LIFECYCLE_TSAN_TEST_BIN) $(RUNNER_TXCTX_TEST_BIN) $(RUNNER_TXSTACK_TEST_BIN) $(RUNNER_ATTEMPT_TEST_BIN) $(RUNNER_ATTEMPT_HANDLER_TEST_BIN) $(RUNNER_INTEGRATION_TEST_BIN) $(RUNNER_RECOVERY_TEST_BIN) $(RUNNER_MAILBOX_TEST_BIN) $(RUNNER_DEAD_LETTER_TEST_BIN) $(RUNNER_OUTBOX_TEST_BIN) $(RUNNER_TIMER_TEST_BIN) $(RUNNER_SCHEDULER_TEST_BIN) $(RUNNER_INTENT_SINK_TEST_BIN) $(RUNNER_NATIVE_EXAMPLE_BIN) $(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN) $(RUNNER_MULTIWRITER_STRESS_BIN) $(RUNNER_PHASEE_BENCH_BIN) $(WASI_RUNTIME_TEST_BIN) $(WASI_SHIM_TEST_BIN)
