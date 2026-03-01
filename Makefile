# Makefile for the Sapling B+ tree library

# Variables:
#   PAGE_SIZE=N       — override SAPLING_PAGE_SIZE (default 4096)
#   THREADED=1        — enable thread-safe build (-DSAPLING_THREADED -lpthread)
#   BENCH_COUNT=N     — entries per benchmark round (default 100000)
#   BENCH_ROUNDS=N    — benchmark rounds (default 3)
#   SEQ_FUZZ_RUNS=N   — seq-fuzz run budget (default 20000)
#   SEQ_FUZZ_MAX_LEN=N — seq-fuzz max input length (default 4096)
#   SEQ_FUZZ_CORPUS=DIR — writable seq-fuzz corpus directory (default .fuzz-corpus/seq)
#   SEQ_FUZZ_SEED_CORPUS=DIR — optional read-only seed corpus (default tests/fuzz/corpus/seq)
#   TEXT_FUZZ_RUNS=N   — text-fuzz run budget (default 20000)
#   TEXT_FUZZ_MAX_LEN=N — text-fuzz max input length (default 4096)
#   TEXT_FUZZ_CORPUS=DIR — writable text-fuzz corpus directory (default .fuzz-corpus/text)
#   TEXT_FUZZ_SEED_CORPUS=DIR — optional read-only seed corpus (default tests/fuzz/corpus/text)
#   BENCH_BASELINE=F  — baseline file for bench-ci (default benchmarks/baseline.env)
#   BENCH_ALLOWED_REGRESSION_PCT=N — override baseline regression budget
#   BENCH_BASELINE_PROFILE=NAME — force baseline profile (default auto-detect)
#   BENCH_EMIT_BASELINE_UPDATE=1 — print baseline update lines instead of enforcing
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
#   CPPCHECK_FILES="..." — source roots/files analyzed by cppcheck (default src)
#   LINT_WARNING_FLAGS="..." — extra warning flags for lint warning pass
#   WASM_TOOLS=BIN    — wasm-tools binary override

CC       ?= clang
CXX      ?= clang++
CFLAGS   := -Wall -Wextra -Werror -std=c11
# Expose POSIX declarations (clock_gettime/nanosleep) under strict C11.
CFLAGS   += -D_POSIX_C_SOURCE=200809L
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

BUILD_DIR := build
OBJ_DIR   := $(BUILD_DIR)/obj
BIN_DIR   := $(BUILD_DIR)/bin
LIB_DIR   := $(BUILD_DIR)/lib

LIB      = $(LIB_DIR)/libsapling.a
TEST_BIN := $(BIN_DIR)/test_sapling
TEST_SEQ_BIN := $(BIN_DIR)/test_seq
TEST_TEXT_BIN := $(BIN_DIR)/test_text
TEST_TEXT_LITERAL_BIN := $(BIN_DIR)/test_text_literal
TEST_TEXT_TREE_REG_BIN := $(BIN_DIR)/test_text_tree_registry
TEST_BEPT_BIN := $(BIN_DIR)/test_bept
TEST_HAMT_BIN := $(BIN_DIR)/test_hamt
TEST_ARENA_BIN := $(BIN_DIR)/test_arena
TEST_THATCH_BIN := $(BIN_DIR)/test_thatch
TEST_THATCH_JSON_BIN := $(BIN_DIR)/test_thatch_json
BENCH_BIN = $(BIN_DIR)/bench_sapling
BENCH_SEQ_BIN = $(BIN_DIR)/bench_seq
BENCH_TEXT_BIN = $(BIN_DIR)/bench_text
BENCH_BEPT_BIN = $(BIN_DIR)/bench_bept
STRESS_BIN = $(BIN_DIR)/fault_harness
SEQ_FUZZ_BIN = $(BIN_DIR)/fuzz_seq
TEXT_FUZZ_BIN = $(BIN_DIR)/fuzz_text

RUNNER_LIFECYCLE_TSAN_TEST_BIN = $(BIN_DIR)/runner_lifecycle_test_tsan



# ... existing definitions ...

WASM_RUNNER_TEST_BIN = $(BIN_DIR)/wasm_runner_test
WASI_DEDUPE_TEST_BIN = $(BIN_DIR)/wasi_dedupe_test
WASI_RUNTIME_TEST_BIN = $(BIN_DIR)/wasi_runtime_test
WASI_SHIM_TEST_BIN = $(BIN_DIR)/wasi_shim_test
BENCH_COUNT ?= 100000
BENCH_ROUNDS ?= 3
SEQ_FUZZ_RUNS ?= 20000
SEQ_FUZZ_MAX_LEN ?= 4096
SEQ_FUZZ_CORPUS ?= .fuzz-corpus/seq
SEQ_FUZZ_SEED_CORPUS ?= tests/fuzz/corpus/seq
SEQ_FUZZ_INPUT_CORPUS := $(if $(and $(wildcard $(SEQ_FUZZ_SEED_CORPUS)),$(filter-out $(SEQ_FUZZ_CORPUS),$(SEQ_FUZZ_SEED_CORPUS))),$(SEQ_FUZZ_SEED_CORPUS),)
TEXT_FUZZ_RUNS ?= 20000
TEXT_FUZZ_MAX_LEN ?= 4096
TEXT_FUZZ_CORPUS ?= .fuzz-corpus/text
TEXT_FUZZ_SEED_CORPUS ?= tests/fuzz/corpus/text
TEXT_FUZZ_INPUT_CORPUS := $(if $(and $(wildcard $(TEXT_FUZZ_SEED_CORPUS)),$(filter-out $(TEXT_FUZZ_CORPUS),$(TEXT_FUZZ_SEED_CORPUS))),$(TEXT_FUZZ_SEED_CORPUS),)
LLVM_SYMBOLIZER ?= $(shell command -v llvm-symbolizer 2>/dev/null || true)
SEQ_FUZZ_CXXLIB_DIR ?= $(if $(wildcard /opt/homebrew/opt/llvm@21/lib/c++),/opt/homebrew/opt/llvm@21/lib/c++,)
comma := ,
SEQ_FUZZ_CXXLIB_FLAGS := $(if $(wildcard $(SEQ_FUZZ_CXXLIB_DIR)),-L$(SEQ_FUZZ_CXXLIB_DIR) -Wl$(comma)-rpath$(comma)$(SEQ_FUZZ_CXXLIB_DIR) -lc++abi,)
SEQ_FUZZ_SYMBOLIZER_ENV := $(if $(LLVM_SYMBOLIZER),ASAN_SYMBOLIZER_PATH=$(LLVM_SYMBOLIZER),)
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
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy
CPPCHECK ?= cppcheck
CPPCHECK_FILES ?= src
CLANG_TIDY_CHECKS ?= clang-analyzer-*,portability-*,-clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling
CLANG_TIDY_HEADER_FILTER ?= ^(src|include)/
LINT_WARNING_FLAGS ?= -Wformat=2 -Wmissing-prototypes -Wold-style-definition -Wpointer-arith -Wstrict-prototypes -Wundef -Wvla
LINT_CFLAGS ?= $(CFLAGS) $(LINT_WARNING_FLAGS)
WASM_TOOLS ?= wasm-tools
WASI_CC   ?= clang
WASI_AR   ?= ar
WASI_TARGET ?= wasm32-wasi
WASI_SYSROOT ?=
WASI_CFLAGS ?= -Wall -Wextra -Werror -std=c11 -O2 -DSAPLING_PAGE_SIZE=$(PAGE_SIZE) --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT) $(INCLUDES)
WASI_LDFLAGS ?= --target=$(WASI_TARGET) --sysroot=$(WASI_SYSROOT)
WASM_LIB  = $(LIB_DIR)/libsapling_wasm.a
WASM_OBJ  = $(OBJ_DIR)/sapling_wasm.o
WASM_SMOKE = $(BIN_DIR)/wasm_smoke.wasm
WASM_GUEST_EXAMPLE = $(BIN_DIR)/wasm_guest_example.wasm
# Runner tests
RUNNER_TEST_NAMES := \
	attempt_handler \
	attempt \
	dead_letter \
	dedupe \
	intent_sink \
	lease \
	lifecycle \
	mailbox \
	outbox \
	scheduler \
	timer \
	txctx \
	txstack \
	wire

RUNNER_TEST_BINS := $(patsubst %,$(BIN_DIR)/runner_%_test,$(RUNNER_TEST_NAMES))
RUNNER_TEST_TARGETS := $(foreach test,$(RUNNER_TEST_NAMES),runner-$(subst _,-,$(test))-test)

# Dynamic Source Discovery
C_SOURCES := $(shell find src tests benchmarks examples -type f -name '*.c')
C_HEADERS := $(shell find src tests benchmarks examples -type f -name '*.h')

FORMAT_FILES := $(C_SOURCES) $(C_HEADERS)
PHASE0_TIDY_FILES := $(filter-out generated/%, $(C_SOURCES))
LINT_WARNING_SOURCES := $(filter src/%, $(C_SOURCES))
LINT_TIDY_SOURCES := $(filter src/%, $(PHASE0_TIDY_FILES))

SAPLING_SRC := src/sapling/sapling.c src/sapling/arena.c src/sapling/txn.c src/sapling/bept.c src/sapling/hamt.c src/sapling/err.c
SAPLING_HDR := include/sapling/sapling.h include/sapling/arena.h include/sapling/txn.h include/sapling/bept.h include/sapling/hamt.h include/sapling/err.h
SEQ_SRC := src/sapling/seq.c
SEQ_HDR := include/sapling/seq.h
TEXT_SRC := src/sapling/text.c
TEXT_HDR := include/sapling/text.h
TEXT_LITERAL_SRC := src/sapling/text_literal.c
TEXT_LITERAL_HDR := include/sapling/text_literal.h
TEXT_TREE_REG_SRC := src/sapling/text_tree_registry.c
TEXT_TREE_REG_HDR := include/sapling/text_tree_registry.h
THATCH_SRC := src/sapling/thatch.c
THATCH_HDR := include/sapling/thatch.h
THATCH_JSON_SRC := src/sapling/thatch_json.c
THATCH_JSON_HDR := include/sapling/thatch_json.h

WIT_GEN_SRC ?= $(WIT_GEN_DIR)/wit_schema_dbis.c
WIT_GEN_HDR ?= $(WIT_GEN_DIR)/wit_schema_dbis.h

CORE_OBJS := $(OBJ_DIR)/src/sapling/sapling.o $(OBJ_DIR)/src/sapling/arena.o $(OBJ_DIR)/src/sapling/txn.o $(OBJ_DIR)/src/sapling/bept.o $(OBJ_DIR)/src/sapling/hamt.o
SEQ_OBJ := $(OBJ_DIR)/src/sapling/seq.o
TEXT_OBJ := $(OBJ_DIR)/src/sapling/text.o
COMMON_OBJS := $(patsubst %.c,$(OBJ_DIR)/%.o,$(filter src/common/%, $(C_SOURCES)))
RUNNER_OBJS := $(patsubst %.c,$(OBJ_DIR)/%.o,$(filter src/runner/%, $(C_SOURCES)))
WASI_OBJS := $(patsubst %.c,$(OBJ_DIR)/%.o,$(filter src/wasi/%, $(C_SOURCES)))
WIT_GEN_OBJ := $(OBJ_DIR)/$(WIT_GEN_DIR)/wit_schema_dbis.o
THREADED_OBJ_DIR := $(BUILD_DIR)/obj_threaded
THREADED_CORE_OBJS := $(THREADED_OBJ_DIR)/src/sapling/sapling.o $(THREADED_OBJ_DIR)/src/sapling/arena.o $(THREADED_OBJ_DIR)/src/sapling/txn.o $(THREADED_OBJ_DIR)/src/sapling/bept.o $(THREADED_OBJ_DIR)/src/sapling/hamt.o
THREADED_COMMON_OBJS := $(patsubst %.c,$(THREADED_OBJ_DIR)/%.o,$(filter src/common/%, $(C_SOURCES)))
THREADED_RUNNER_OBJS := $(patsubst %.c,$(THREADED_OBJ_DIR)/%.o,$(filter src/runner/%, $(C_SOURCES)))
THREADED_WASI_OBJS := $(patsubst %.c,$(THREADED_OBJ_DIR)/%.o,$(filter src/wasi/%, $(C_SOURCES)))
THREADED_WIT_GEN_OBJ := $(THREADED_OBJ_DIR)/$(WIT_GEN_DIR)/wit_schema_dbis.o

ALL_LIB_OBJS := $(CORE_OBJS) $(COMMON_OBJS) $(RUNNER_OBJS) $(WASI_OBJS) $(WIT_GEN_OBJ)
THREADED_ALL_LIB_OBJS := $(THREADED_CORE_OBJS) $(THREADED_COMMON_OBJS) $(THREADED_RUNNER_OBJS) $(THREADED_WASI_OBJS) $(THREADED_WIT_GEN_OBJ)
OBJ := $(CORE_OBJS)

.PHONY: all test text-test text-literal-test text-tree-registry-test seq-test test-arena thatch-test thatch-json-test hamt-test debug asan asan-seq tsan bench bench-run seq-bench seq-bench-run text-bench text-bench-run bench-ci seq-fuzz text-fuzz wasm-lib wasm-check format format-check style-check lint-warnings tidy cppcheck cppcheck-strict lint lint-strict wit-schema-check wit-schema-generate wit-schema-cc-check $(RUNNER_TEST_TARGETS) runner-lifecycle-threaded-tsan-test runner-integration-test test-integration runner-native-example runner-host-api-example runner-threaded-pipeline-example runner-multiwriter-stress-build runner-multiwriter-stress runner-phasee-bench runner-phasee-bench-run runner-release-checklist wasi-runtime-test wasi-shim-test wasi-dedupe-test wasm-runner-test schema-check runner-dbi-status-check stress-harness phase0-check phasea-check phaseb-check phasec-check clean

all: CFLAGS += -O2
all: $(LIB)

debug: CFLAGS += -O0 -g -DDEBUG
debug: $(LIB)

$(LIB): $(OBJ)
	@mkdir -p $(dir $@)
	$(AR) $(ARFLAGS) $@ $^


test: CFLAGS += -O2 -g
test: $(TEST_BIN) $(TEST_SEQ_BIN) $(TEST_TEXT_BIN) $(TEST_TEXT_LITERAL_BIN) $(TEST_TEXT_TREE_REG_BIN) $(TEST_BEPT_BIN) $(TEST_HAMT_BIN) $(TEST_ARENA_BIN) $(TEST_THATCH_BIN) $(TEST_THATCH_JSON_BIN) $(WASI_SHIM_TEST_BIN) $(WASI_RUNTIME_TEST_BIN) $(WASI_DEDUPE_TEST_BIN) $(RUNNER_TEST_BINS)
	./$(TEST_BIN)
	./$(TEST_SEQ_BIN)
	./$(TEST_TEXT_BIN)
	./$(TEST_TEXT_LITERAL_BIN)
	./$(TEST_TEXT_TREE_REG_BIN)
	./$(TEST_BEPT_BIN)
	./$(TEST_HAMT_BIN)
	./$(TEST_ARENA_BIN)
	./$(TEST_THATCH_BIN)
	./$(TEST_THATCH_JSON_BIN)
	./$(WASI_SHIM_TEST_BIN)
	./$(WASI_RUNTIME_TEST_BIN)
	./$(WASI_DEDUPE_TEST_BIN)
	@for bin in $(RUNNER_TEST_BINS); do echo "Running $$bin"; ./$$bin || exit 1; done

text-test: CFLAGS += -O2 -g
text-test: $(TEST_TEXT_BIN)
	./$(TEST_TEXT_BIN)

asan: CFLAGS += -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
asan: LDFLAGS += -fsanitize=address,undefined
asan: clean
	@mkdir -p $(dir $(TEST_BIN)) $(dir $(TEST_SEQ_BIN))
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_sapling.c $(SAPLING_SRC) -o $(TEST_BIN) $(LDFLAGS)
	./$(TEST_BIN)
	$(CC) $(CFLAGS) -DSAPLING_SEQ_TESTING $(INCLUDES) tests/unit/test_seq.c $(SEQ_SRC) $(SAPLING_SRC) -o $(TEST_SEQ_BIN) $(LDFLAGS)
	./$(TEST_SEQ_BIN)

asan-seq: CFLAGS += -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -DSAPLING_SEQ_TESTING
asan-seq: LDFLAGS += -fsanitize=address,undefined
asan-seq: clean
	@mkdir -p $(dir $(TEST_SEQ_BIN))
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_seq.c $(SEQ_SRC) $(SAPLING_SRC) -o $(TEST_SEQ_BIN) $(LDFLAGS)
	./$(TEST_SEQ_BIN)

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

seq-bench: CFLAGS += -O3 -g
seq-bench: $(BENCH_SEQ_BIN)

seq-bench-run: CFLAGS += -O3 -g
seq-bench-run: $(BENCH_SEQ_BIN)
	./$(BENCH_SEQ_BIN) --count $(BENCH_COUNT) --rounds $(BENCH_ROUNDS)

text-bench: CFLAGS += -O3 -g
text-bench: $(BENCH_TEXT_BIN)

text-bench-run: CFLAGS += -O3 -g
text-bench-run: $(BENCH_TEXT_BIN)
	./$(BENCH_TEXT_BIN) --count $(BENCH_COUNT) --rounds $(BENCH_ROUNDS)

bept-bench: CFLAGS += -O3 -g
bept-bench: $(BENCH_BEPT_BIN)

bept-bench-run: CFLAGS += -O3 -g
bept-bench-run: $(BENCH_BEPT_BIN)
	./$(BENCH_BEPT_BIN) --count $(BENCH_COUNT) --rounds $(BENCH_ROUNDS)

seq-fuzz: CFLAGS += -O1 -g -fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer -DSAPLING_SEQ_TESTING
seq-fuzz: LDFLAGS += -fsanitize=fuzzer,address,undefined
seq-fuzz:
	@mkdir -p $(dir $(SEQ_FUZZ_BIN)) $(OBJ_DIR)/tests/fuzz $(OBJ_DIR)/src/sapling $(SEQ_FUZZ_CORPUS)
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/sapling.c -o $(OBJ_DIR)/src/sapling/sapling_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/txn.c -o $(OBJ_DIR)/src/sapling/txn_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/arena.c -o $(OBJ_DIR)/src/sapling/arena_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/bept.c -o $(OBJ_DIR)/src/sapling/bept_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c tests/fuzz/fuzz_seq.c -o $(OBJ_DIR)/tests/fuzz/fuzz_seq.o
	$(CC) $(CFLAGS) $(INCLUDES) -c $(SEQ_SRC) -o $(OBJ_DIR)/src/sapling/seq_fuzz.o
	$(CXX) $(OBJ_DIR)/tests/fuzz/fuzz_seq.o $(OBJ_DIR)/src/sapling/seq_fuzz.o $(OBJ_DIR)/src/sapling/sapling_fuzz.o $(OBJ_DIR)/src/sapling/txn_fuzz.o $(OBJ_DIR)/src/sapling/arena_fuzz.o $(OBJ_DIR)/src/sapling/bept_fuzz.o -o $(SEQ_FUZZ_BIN) $(LDFLAGS) $(SEQ_FUZZ_CXXLIB_FLAGS)
	$(SEQ_FUZZ_SYMBOLIZER_ENV) ./$(SEQ_FUZZ_BIN) -runs=$(SEQ_FUZZ_RUNS) -max_len=$(SEQ_FUZZ_MAX_LEN) $(SEQ_FUZZ_CORPUS) $(SEQ_FUZZ_INPUT_CORPUS)

text-fuzz: CFLAGS += -O1 -g -fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer -DSAPLING_SEQ_TESTING
text-fuzz: LDFLAGS += -fsanitize=fuzzer,address,undefined
text-fuzz:
	@mkdir -p $(dir $(TEXT_FUZZ_BIN)) $(OBJ_DIR)/tests/fuzz $(OBJ_DIR)/src/sapling $(TEXT_FUZZ_CORPUS)
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/sapling.c -o $(OBJ_DIR)/src/sapling/sapling_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/txn.c -o $(OBJ_DIR)/src/sapling/txn_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/arena.c -o $(OBJ_DIR)/src/sapling/arena_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c src/sapling/bept.c -o $(OBJ_DIR)/src/sapling/bept_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c tests/fuzz/fuzz_text.c -o $(OBJ_DIR)/tests/fuzz/fuzz_text.o
	$(CC) $(CFLAGS) $(INCLUDES) -c $(TEXT_SRC) -o $(OBJ_DIR)/src/sapling/text_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c $(TEXT_LITERAL_SRC) -o $(OBJ_DIR)/src/sapling/text_literal_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c $(TEXT_TREE_REG_SRC) -o $(OBJ_DIR)/src/sapling/text_tree_registry_fuzz.o
	$(CC) $(CFLAGS) $(INCLUDES) -c $(SEQ_SRC) -o $(OBJ_DIR)/src/sapling/seq_text_fuzz.o
	$(CXX) $(OBJ_DIR)/tests/fuzz/fuzz_text.o $(OBJ_DIR)/src/sapling/text_fuzz.o $(OBJ_DIR)/src/sapling/text_literal_fuzz.o $(OBJ_DIR)/src/sapling/text_tree_registry_fuzz.o $(OBJ_DIR)/src/sapling/seq_text_fuzz.o $(OBJ_DIR)/src/sapling/sapling_fuzz.o $(OBJ_DIR)/src/sapling/txn_fuzz.o $(OBJ_DIR)/src/sapling/arena_fuzz.o $(OBJ_DIR)/src/sapling/bept_fuzz.o -o $(TEXT_FUZZ_BIN) $(LDFLAGS) $(SEQ_FUZZ_CXXLIB_FLAGS)
	$(SEQ_FUZZ_SYMBOLIZER_ENV) ./$(TEXT_FUZZ_BIN) -runs=$(TEXT_FUZZ_RUNS) -max_len=$(TEXT_FUZZ_MAX_LEN) $(TEXT_FUZZ_CORPUS) $(TEXT_FUZZ_INPUT_CORPUS)

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
	@mkdir -p $(dir $@)
	$(WASI_CC) $(WASI_CFLAGS) -c $(SAPLING_SRC) -o $(WASM_OBJ)

$(WASM_LIB): $(WASM_OBJ)
	@mkdir -p $(dir $@)
	$(WASI_AR) $(ARFLAGS) $@ $^

$(WASM_SMOKE): tests/unit/wasm_smoke.c $(SAPLING_SRC) $(SAPLING_HDR)
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	@mkdir -p $(dir $@)
	$(WASI_CC) $(WASI_CFLAGS) tests/unit/wasm_smoke.c $(SAPLING_SRC) -o $(WASM_SMOKE) $(WASI_LDFLAGS)

$(WASM_GUEST_EXAMPLE): tests/unit/wasm_guest_example.c
	@if [ -z "$(WASI_SYSROOT)" ]; then \
		echo "WASI_SYSROOT is required (example: /opt/wasi-sdk/share/wasi-sysroot)"; \
		exit 1; \
	fi
	@mkdir -p $(dir $@)
	$(WASI_CC) $(WASI_CFLAGS) tests/unit/wasm_guest_example.c -o $(WASM_GUEST_EXAMPLE) $(WASI_LDFLAGS)


# Generic object compilation
$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Threaded object compilation in a separate object tree to avoid
# cross-mode artifact reuse between threaded and non-threaded targets.
$(THREADED_OBJ_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -DSAPLING_THREADED $(INCLUDES) -c $< -o $@

$(TEST_BIN): tests/unit/test_sapling.c $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_sapling.c $(SAPLING_SRC) -o $@ $(LDFLAGS)

test-arena: $(TEST_ARENA_BIN)
	./$(TEST_ARENA_BIN)

$(TEST_ARENA_BIN): tests/unit/test_arena.c src/sapling/arena.c include/sapling/arena.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_arena.c src/sapling/arena.c -o $@ $(LDFLAGS)

thatch-test: CFLAGS += -O2 -g
thatch-test: $(TEST_THATCH_BIN)
	./$(TEST_THATCH_BIN)

$(TEST_THATCH_BIN): tests/unit/test_thatch.c $(THATCH_SRC) $(THATCH_HDR) $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_thatch.c $(THATCH_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

thatch-json-test: CFLAGS += -O2 -g
thatch-json-test: $(TEST_THATCH_JSON_BIN)
	./$(TEST_THATCH_JSON_BIN)

$(TEST_THATCH_JSON_BIN): tests/unit/test_thatch_json.c $(THATCH_JSON_SRC) $(THATCH_JSON_HDR) $(THATCH_SRC) $(THATCH_HDR) $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_thatch_json.c $(THATCH_JSON_SRC) $(THATCH_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS) -lm

$(TEST_TEXT_BIN): tests/unit/test_text.c $(TEXT_SRC) $(TEXT_LITERAL_SRC) $(TEXT_TREE_REG_SRC) $(SEQ_SRC) $(TEXT_HDR) $(TEXT_LITERAL_HDR) $(TEXT_TREE_REG_HDR) $(SEQ_HDR) $(SAPLING_SRC)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_text.c $(TEXT_SRC) $(TEXT_LITERAL_SRC) $(TEXT_TREE_REG_SRC) $(SEQ_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

text-literal-test: CFLAGS += -O2 -g
text-literal-test: $(TEST_TEXT_LITERAL_BIN)
	./$(TEST_TEXT_LITERAL_BIN)

$(TEST_TEXT_LITERAL_BIN): tests/unit/test_text_literal.c $(TEXT_LITERAL_SRC) $(TEXT_TREE_REG_SRC) $(TEXT_SRC) $(SEQ_SRC) $(TEXT_LITERAL_HDR) $(TEXT_TREE_REG_HDR) $(TEXT_HDR) $(SEQ_HDR) $(SAPLING_SRC)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_text_literal.c $(TEXT_LITERAL_SRC) $(TEXT_TREE_REG_SRC) $(TEXT_SRC) $(SEQ_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

text-tree-registry-test: CFLAGS += -O2 -g
text-tree-registry-test: $(TEST_TEXT_TREE_REG_BIN)
	./$(TEST_TEXT_TREE_REG_BIN)

$(TEST_TEXT_TREE_REG_BIN): tests/unit/test_text_tree_registry.c $(TEXT_TREE_REG_SRC) $(TEXT_LITERAL_SRC) $(TEXT_SRC) $(SEQ_SRC) $(TEXT_TREE_REG_HDR) $(TEXT_LITERAL_HDR) $(TEXT_HDR) $(SEQ_HDR) $(SAPLING_SRC)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_text_tree_registry.c $(TEXT_TREE_REG_SRC) $(TEXT_LITERAL_SRC) $(TEXT_SRC) $(SEQ_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

$(TEST_SEQ_BIN): tests/unit/test_seq.c $(SEQ_SRC) $(SEQ_HDR) $(SAPLING_SRC)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -DSAPLING_SEQ_TESTING $(INCLUDES) tests/unit/test_seq.c $(SEQ_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

$(TEST_BEPT_BIN): tests/unit/test_bept.c $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/unit/test_bept.c $(SAPLING_SRC) -o $@ $(LDFLAGS)

hamt-test: CFLAGS += -O2 -g
hamt-test: $(TEST_HAMT_BIN)
	./$(TEST_HAMT_BIN)

$(TEST_HAMT_BIN): tests/unit/test_hamt.c $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -DSAPLING_HAMT_TESTING $(INCLUDES) tests/unit/test_hamt.c $(SAPLING_SRC) -o $@ $(LDFLAGS)

seq-test: CFLAGS += -O2 -g
seq-test: $(TEST_SEQ_BIN)
	./$(TEST_SEQ_BIN)

$(BENCH_BIN): benchmarks/bench_sapling.c $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) benchmarks/bench_sapling.c $(SAPLING_SRC) -o $@ $(LDFLAGS)

$(BENCH_SEQ_BIN): benchmarks/bench_seq.c $(SEQ_SRC) $(SAPLING_SRC) $(SEQ_HDR) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) benchmarks/bench_seq.c $(SEQ_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

$(BENCH_TEXT_BIN): benchmarks/bench_text.c $(TEXT_SRC) $(TEXT_LITERAL_SRC) $(TEXT_TREE_REG_SRC) $(SEQ_SRC) $(SAPLING_SRC) $(TEXT_HDR) $(TEXT_LITERAL_HDR) $(TEXT_TREE_REG_HDR) $(SEQ_HDR) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) benchmarks/bench_text.c $(TEXT_SRC) $(TEXT_LITERAL_SRC) $(TEXT_TREE_REG_SRC) $(SEQ_SRC) $(SAPLING_SRC) -o $@ $(LDFLAGS)

$(BENCH_BEPT_BIN): benchmarks/bench_bept.c $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) benchmarks/bench_bept.c $(SAPLING_SRC) -o $@ $(LDFLAGS)

$(STRESS_BIN): tests/stress/fault_harness.c src/common/fault_inject.c $(SAPLING_SRC) $(SAPLING_HDR)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) tests/stress/fault_harness.c src/common/fault_inject.c $(SAPLING_SRC) -o $@ $(LDFLAGS)

# Consolidated Runner Test Build Rules
# 1. Build the unit test binaries (pattern rule)
$(RUNNER_TEST_BINS): $(BIN_DIR)/runner_%_test: $(OBJ_DIR)/tests/unit/runner_%_test.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

# 2. Build the integration test binaries (pattern rule)
$(BIN_DIR)/runner_%_integration_test: $(OBJ_DIR)/tests/integration/runner_%_integration_test.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

# 3. Phony targets to run tests.
# Convert underscores in source test names to hyphenated target names
# so phase checks can use natural "runner-foo-bar-test" targets.
define RUNNER_TEST_RULE
runner-$(subst _,-,$(1))-test: CFLAGS += -O2 -g
runner-$(subst _,-,$(1))-test: wit-schema-generate $(BIN_DIR)/runner_$(1)_test
	./$(BIN_DIR)/runner_$(1)_test
endef
$(foreach test,$(RUNNER_TEST_NAMES),$(eval $(call RUNNER_TEST_RULE,$(test))))

# Special case for recovery which is an integration test
RUNNER_RECOVERY_TEST_BIN = $(BIN_DIR)/runner_recovery_integration_test
runner-recovery-test: CFLAGS += -O2 -g
runner-recovery-test: wit-schema-generate $(RUNNER_RECOVERY_TEST_BIN)
	./$(RUNNER_RECOVERY_TEST_BIN)

$(RUNNER_RECOVERY_TEST_BIN): $(OBJ_DIR)/tests/integration/runner_recovery_integration_test.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/tests/integration/runner_recovery_integration_test.o $(ALL_LIB_OBJS) -o $(RUNNER_RECOVERY_TEST_BIN) $(LDFLAGS)

# Special case for ttl-sweep which has different naming
RUNNER_TTL_SWEEP_TEST_BIN = $(BIN_DIR)/runner_ttl_sweep_test
runner-ttl-sweep-test: CFLAGS += -O2 -g
runner-ttl-sweep-test: wit-schema-generate $(RUNNER_TTL_SWEEP_TEST_BIN)
	./$(RUNNER_TTL_SWEEP_TEST_BIN)

$(RUNNER_TTL_SWEEP_TEST_BIN): $(OBJ_DIR)/tests/unit/test_runner_ttl_sweep.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/tests/unit/test_runner_ttl_sweep.o $(ALL_LIB_OBJS) -o $(RUNNER_TTL_SWEEP_TEST_BIN) $(LDFLAGS)

RUNNER_NATIVE_EXAMPLE_BIN = $(BIN_DIR)/runner_native_example
RUNNER_HOST_API_EXAMPLE_BIN = $(BIN_DIR)/runner_host_api_example
RUNNER_THREADED_PIPELINE_EXAMPLE_BIN = $(BIN_DIR)/runner_threaded_pipeline_example
RUNNER_MULTIWRITER_STRESS_BIN = $(BIN_DIR)/runner_multiwriter_stress
RUNNER_PHASEE_BENCH_BIN = $(BIN_DIR)/bench_runner_phasee
RUNNER_INTEGRATION_TEST_BIN = $(BIN_DIR)/runner_atomic_integration_test

$(RUNNER_NATIVE_EXAMPLE_BIN): $(OBJ_DIR)/examples/native/runner_native_example.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/examples/native/runner_native_example.o $(ALL_LIB_OBJS) -o $(RUNNER_NATIVE_EXAMPLE_BIN) $(LDFLAGS)

$(RUNNER_HOST_API_EXAMPLE_BIN): $(OBJ_DIR)/examples/native/runner_host_api_example.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/examples/native/runner_host_api_example.o $(ALL_LIB_OBJS) -o $(RUNNER_HOST_API_EXAMPLE_BIN) $(LDFLAGS)

$(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN): $(THREADED_OBJ_DIR)/examples/native/runner_threaded_pipeline_example.o $(THREADED_ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(THREADED_OBJ_DIR)/examples/native/runner_threaded_pipeline_example.o $(THREADED_ALL_LIB_OBJS) -o $(RUNNER_THREADED_PIPELINE_EXAMPLE_BIN) $(LDFLAGS)

$(RUNNER_MULTIWRITER_STRESS_BIN): $(THREADED_OBJ_DIR)/tests/stress/runner_multiwriter_stress.o $(THREADED_ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(THREADED_OBJ_DIR)/tests/stress/runner_multiwriter_stress.o $(THREADED_ALL_LIB_OBJS) -o $(RUNNER_MULTIWRITER_STRESS_BIN) $(LDFLAGS)

$(RUNNER_PHASEE_BENCH_BIN): $(OBJ_DIR)/benchmarks/bench_runner_phasee.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/benchmarks/bench_runner_phasee.o $(ALL_LIB_OBJS) -o $(RUNNER_PHASEE_BENCH_BIN) $(LDFLAGS)

$(WASI_RUNTIME_TEST_BIN): $(OBJ_DIR)/tests/unit/wasi_runtime_test.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/tests/unit/wasi_runtime_test.o $(ALL_LIB_OBJS) -o $(WASI_RUNTIME_TEST_BIN) $(LDFLAGS)

$(WASI_SHIM_TEST_BIN): $(OBJ_DIR)/tests/unit/wasi_shim_test.o $(ALL_LIB_OBJS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJ_DIR)/tests/unit/wasi_shim_test.o $(ALL_LIB_OBJS) -o $(WASI_SHIM_TEST_BIN) $(LDFLAGS)

format:
	@command -v $(CLANG_FORMAT) >/dev/null 2>&1 || { echo "clang-format not found: $(CLANG_FORMAT)"; exit 2; }
	$(CLANG_FORMAT) -i $(FORMAT_FILES)

format-check:
	@command -v $(CLANG_FORMAT) >/dev/null 2>&1 || { echo "clang-format not found: $(CLANG_FORMAT)"; exit 2; }
	$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

style-check: format-check

lint-warnings:
	$(CC) $(LINT_CFLAGS) $(INCLUDES) -fsyntax-only $(LINT_WARNING_SOURCES)

tidy:
	@command -v $(CLANG_TIDY) >/dev/null 2>&1 || { echo "clang-tidy not found: $(CLANG_TIDY)"; exit 2; }
	$(CLANG_TIDY) -quiet -checks=$(CLANG_TIDY_CHECKS) -header-filter='$(CLANG_TIDY_HEADER_FILTER)' $(LINT_TIDY_SOURCES) -- $(CFLAGS) $(INCLUDES)

cppcheck:
	@if command -v $(CPPCHECK) >/dev/null 2>&1; then \
		$(CPPCHECK) --enable=warning,performance,portability --std=c11 \
			-Iinclude $(CPPCHECK_FILES); \
	else \
		echo "cppcheck not found ($(CPPCHECK)); skipping"; \
	fi

cppcheck-strict:
	@if command -v $(CPPCHECK) >/dev/null 2>&1; then \
		$(CPPCHECK) --enable=warning,performance,portability --error-exitcode=1 --std=c11 \
			-Iinclude $(CPPCHECK_FILES); \
	else \
		echo "cppcheck not found ($(CPPCHECK)); skipping"; \
	fi

lint: lint-warnings tidy cppcheck

lint-strict: lint-warnings tidy cppcheck-strict

wit-schema-check:
	@command -v $(WASM_TOOLS) >/dev/null 2>&1 || { echo "wasm-tools not found: $(WASM_TOOLS)"; exit 2; }
	$(WASM_TOOLS) component wit $(WIT_SCHEMA_DIR) --json >/dev/null

wit-schema-generate: $(WIT_SCHEMA) $(WIT_CODEGEN)
	python3 $(WIT_CODEGEN) --wit $(WIT_SCHEMA) --manifest $(DBI_MANIFEST) --header $(WIT_GEN_HDR) --source $(WIT_GEN_SRC)

wit-schema-cc-check: wit-schema-generate
	@mkdir -p $(dir $(WIT_GEN_OBJ))
	$(CC) $(CFLAGS) $(INCLUDES) -c $(WIT_GEN_SRC) -o $(WIT_GEN_OBJ)

schema-check: wit-schema-check wit-schema-cc-check
	python3 tools/check_dbi_manifest.py $(DBI_MANIFEST)
	$(MAKE) runner-dbi-status-check

runner-dbi-status-check: wit-schema-generate
	python3 tools/check_runner_dbi_status.py $(DBI_MANIFEST) $(WIT_GEN_HDR) .

runner-lifecycle-threaded-tsan-test: wit-schema-generate $(RUNNER_LIFECYCLE_TSAN_TEST_BIN)
	./$(RUNNER_LIFECYCLE_TSAN_TEST_BIN)
	rm -f $(RUNNER_LIFECYCLE_TSAN_TEST_BIN)

$(RUNNER_LIFECYCLE_TSAN_TEST_BIN): tests/unit/runner_lifecycle_test.c $(C_SOURCES) $(WIT_GEN_SRC)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -DSAPLING_THREADED -O1 -fsanitize=thread $(INCLUDES) tests/unit/runner_lifecycle_test.c $(filter src/runner/%, $(C_SOURCES)) $(SAPLING_SRC) $(filter src/common/%, $(C_SOURCES)) $(filter src/wasi/%, $(C_SOURCES)) $(WIT_GEN_SRC) -o $(RUNNER_LIFECYCLE_TSAN_TEST_BIN) -fsanitize=thread -lpthread

test-integration: runner-integration-test

runner-integration-test: $(BIN_DIR)/runner_atomic_integration_test
	./$(BIN_DIR)/runner_atomic_integration_test

wasm-runner-test: $(WASM_RUNNER_TEST_BIN)
	./$(WASM_RUNNER_TEST_BIN)

$(WASM_RUNNER_TEST_BIN): $(OBJ_DIR)/tests/unit/wasm_runner_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

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

wasi-dedupe-test: CFLAGS += -O2 -g
wasi-dedupe-test: wit-schema-generate $(WASI_DEDUPE_TEST_BIN)
	./$(WASI_DEDUPE_TEST_BIN)

$(WASI_DEDUPE_TEST_BIN): $(OBJ_DIR)/tests/unit/wasi_dedupe_test.o $(ALL_LIB_OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

stress-harness: $(STRESS_BIN)
	./$(STRESS_BIN)

phase0-check: lint schema-check stress-harness

phasea-check: phase0-check runner-wire-test runner-lifecycle-test runner-lifecycle-threaded-tsan-test wasi-runtime-test wasi-shim-test

phaseb-check: phasea-check runner-txctx-test runner-txstack-test runner-attempt-test runner-attempt-handler-test runner-integration-test

phasec-check: phaseb-check runner-mailbox-test runner-dead-letter-test runner-outbox-test runner-timer-test runner-scheduler-test runner-intent-sink-test runner-ttl-sweep-test runner-native-example runner-threaded-pipeline-example runner-multiwriter-stress-build runner-recovery-test

clean:
	rm -rf $(BUILD_DIR) test_sapling test_text test_seq bench_sapling bench_seq bench_text fuzz_seq fuzz_text fault_harness runner_wire_test \
		runner_lifecycle_test runner_lifecycle_test_tsan runner_txctx_test runner_txstack_test \
		runner_attempt_test runner_attempt_handler_test runner_atomic_integration_test \
		runner_recovery_integration_test runner_mailbox_test runner_dead_letter_test \
		runner_outbox_test runner_timer_test runner_scheduler_test runner_intent_sink_test \
		runner_native_example runner_host_api_example runner_threaded_pipeline_example runner_multiwriter_stress \
		bench_runner_phasee runner_ttl_sweep_test wasi_runtime_test wasi_shim_test wasi_dedupe_test wasm_runner_test runner_dedupe_test runner_lease_test wasm_guest_example.wasm wasm_smoke.wasm
