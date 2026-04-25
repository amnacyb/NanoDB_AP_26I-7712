# ─── NanoDB Build System ─────────────────────────────────────────────────────
# CS-4002 Applied Programming | MS-CS-Spring-2026

CXX      := g++
LDFLAGS  := -lm

# Detection of OS
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
else
    DETECTED_OS := $(shell uname)
endif

# Base flags
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Wpedantic \
            -Wno-unused-parameter -Wno-sign-compare \
            -I. -Iinclude

ifeq ($(DETECTED_OS), Windows)
    EXE      := .exe
    MKDIR    := if not exist $(1) mkdir $(1)
    RM       := del /q /f
    RMDIR    := rmdir /s /q
    # Windows doesn't need -lm usually, and some GCC flags differ
else
    EXE      :=
    MKDIR    := mkdir -p $(1)
    RM       := rm -f
    RMDIR    := rm -rf
    ifeq ($(DETECTED_OS), Linux)
        CXXFLAGS += -Wno-format-truncation -Wno-stringop-truncation
    endif
endif

MAIN_SRC    := main.cpp
RUNNER_SRC  := test_runner.cpp
MAIN_BIN    := nanodb$(EXE)
RUNNER_BIN  := test_runner$(EXE)
DATA_DIR    := data
LOGS_DIR    := logs

.PHONY: all clean run run_tests interactive dirs reset help

all: dirs $(MAIN_BIN) $(RUNNER_BIN)

dirs:
ifeq ($(DETECTED_OS), Windows)
	@if not exist $(DATA_DIR) mkdir $(DATA_DIR)
	@if not exist $(LOGS_DIR) mkdir $(LOGS_DIR)
else
	@mkdir -p $(DATA_DIR) $(LOGS_DIR)
endif

$(MAIN_BIN): $(MAIN_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $(MAIN_BIN)"

$(RUNNER_BIN): $(RUNNER_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $(RUNNER_BIN)"

run: $(MAIN_BIN)
	./$(MAIN_BIN) $(DATA_DIR) all

run_tests: $(RUNNER_BIN)
	./$(RUNNER_BIN) $(DATA_DIR) queries.txt

interactive: $(MAIN_BIN)
	./$(MAIN_BIN) $(DATA_DIR) interactive

test_%: $(MAIN_BIN)
	./$(MAIN_BIN) $(DATA_DIR) $(subst test_,,$@)

reset:
ifeq ($(DETECTED_OS), Windows)
	@if exist $(DATA_DIR)\*.dat del /q $(DATA_DIR)\*.dat
	@if exist $(DATA_DIR)\*.meta del /q $(DATA_DIR)\*.meta
	@if exist $(DATA_DIR)\catalog.txt del /q $(DATA_DIR)\catalog.txt
else
	@rm -rf $(DATA_DIR)/*.dat $(DATA_DIR)/*.meta $(DATA_DIR)/catalog.txt
endif
	@echo "Data files removed. Re-run to regenerate."

clean:
ifeq ($(DETECTED_OS), Windows)
	@if exist $(MAIN_BIN) del /q $(MAIN_BIN)
	@if exist $(RUNNER_BIN) del /q $(RUNNER_BIN)
	@if exist nanodb_execution.log del /q nanodb_execution.log
else
	@rm -f $(MAIN_BIN) $(RUNNER_BIN) nanodb_execution.log
endif
	@echo "Cleaned."

clean_all: clean reset

help:
	@echo "NanoDB Build Targets:"
	@echo "  make all          — Build nanodb and test_runner"
	@echo "  make run          — Run all test cases (A-G)"
	@echo "  make run_tests    — Run automated workload (queries.txt)"
	@echo "  make interactive  — Start interactive SQL shell"
	@echo "  make test_A..G    — Run individual test case"
	@echo "  make reset        — Delete generated data files"
	@echo "  make clean        — Remove binaries and logs"
