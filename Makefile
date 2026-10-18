# Cayrn — shared Rust REPL infrastructure

# ANSI colours; set NO_COLOR=1 for plain output.
ifeq ($(strip $(NO_COLOR)),)
BLUE := \033[1;34m
GREEN := \033[1;32m
YELLOW := \033[1;33m
RED := \033[1;31m
CYAN := \033[1;36m
RESET := \033[0m
endif

CARGO ?= cargo
MODE ?= debug
BIN_DIR ?= ./bin
export CARGO_TARGET_DIR ?= $(CURDIR)/target
TARGET = $(CARGO_TARGET_DIR)/$(MODE)
PROFILE_FLAG = $(if $(filter release,$(MODE)),--release,)
BINARIES := cayrn cayrn-fixture
PROJECT_NAME := Cayrn
GIT_COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_BRANCH = $(shell git branch --show-current 2>/dev/null || echo unknown)
# Porcelain status includes staged and unstaged tracked edits; ?? marks untracked paths.
GIT_CHANGES = $(shell git status --porcelain=v1 --untracked-files=normal 2>/dev/null | \
	awk '/^\?\?/ { untracked = 1; next } \
	     { changed = 1 } \
	     END { if (changed) printf "uncommitted changes"; \
	           if (changed && untracked) printf ", "; \
	           if (untracked) printf "untracked files" }')
GIT_BRANCH_STATUS = $(GIT_BRANCH)$(if $(GIT_CHANGES), ($(GIT_CHANGES)))

.DEFAULT_GOAL := help
.PHONY: help info check-tools check-mode build build-release clean clean-all \
	test lint fmt-check format fmt doc check check-all coverage coverage-html \
	check-deps deps tracked-files push

help:
	@printf '\n%b╔══════════════════════════════════════════════════════════╗%b\n' '$(CYAN)' '$(RESET)'
	@printf '%b║%b %-56s %b║%b\n' '$(CYAN)' '$(BLUE)' '$(PROJECT_NAME) Build System' '$(CYAN)' '$(RESET)'
	@printf '%b╚══════════════════════════════════════════════════════════╝%b\n\n' '$(CYAN)' '$(RESET)'
	@printf '%bBuilding:%b\n' '$(GREEN)' '$(RESET)'
	@printf '  %bmake build%b              - Build workspace; copy both binaries to ./bin\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake build-release%b      - Build and copy optimized release binaries\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake build MODE=release%b - Equivalent release build\n' '$(YELLOW)' '$(RESET)'
	@printf '\n%bTesting & Quality:%b\n' '$(GREEN)' '$(RESET)'
	@printf '  %bmake test-agent%b         - Real LFE/OTP integration (requires cayrn-lfe)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake test%b               - Run all workspace test targets\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake lint%b               - Check formatting and Clippy (warnings fail)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake fmt-check%b          - Check Rust formatting without changes\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake format%b             - Format Rust sources (alias: make fmt)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake doc%b                - Build workspace API docs (warnings fail)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake check%b              - Build + lint + test + docs (CI gate)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake check-all%b          - All checks plus coverage\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake coverage%b           - Summarize coverage (requires cargo-llvm-cov)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake coverage-html%b      - Generate an HTML coverage report\n' '$(YELLOW)' '$(RESET)'
	@printf '\n%bDependencies:%b\n' '$(GREEN)' '$(RESET)'
	@printf '  %bmake check-deps%b         - Report updates; fail if found (cargo-outdated)\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake deps%b               - Update Cargo.lock within manifest constraints\n' '$(YELLOW)' '$(RESET)'
	@printf '\n%bCleaning:%b\n' '$(GREEN)' '$(RESET)'
	@printf '  %bmake clean%b              - Remove Cayrn binaries from ./bin\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake clean-all%b          - Also remove Cargo build artifacts\n' '$(YELLOW)' '$(RESET)'
	@printf '\n%bUtilities & Information:%b\n' '$(GREEN)' '$(RESET)'
	@printf '  %bmake info%b               - Show tool versions, paths and binary status\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake check-tools%b        - Check required tools; list optional tools\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake tracked-files%b      - Save tracked paths under the target directory\n' '$(YELLOW)' '$(RESET)'
	@printf '  %bmake push%b               - Push main, planning and tags to GitHub origin\n' '$(YELLOW)' '$(RESET)'
	@printf '\n%bCurrent status:%b Branch: %s | Commit: %s\n\n' '$(CYAN)' '$(RESET)' '$(GIT_BRANCH_STATUS)' '$(GIT_COMMIT)'

info:
	@printf '\n%b╔══════════════════════════════════════════════════════════╗%b\n' '$(CYAN)' '$(RESET)'
	@printf '%b║%b %-56s %b║%b\n' '$(CYAN)' '$(BLUE)' 'Cayrn Build Information' '$(CYAN)' '$(RESET)'
	@printf '%b╚══════════════════════════════════════════════════════════╝%b\n\n' '$(CYAN)' '$(RESET)'
	@printf '%bProject:%b\n  Name:       %s\n  Mode:       %s\n  Workspace:  %s\n  Binaries:   %s\n  Target:     %s\n' '$(GREEN)' '$(RESET)' '$(PROJECT_NAME)' '$(MODE)' '$(CURDIR)' '$(BIN_DIR)' '$(TARGET)'
	@printf '\n%bGit:%b\n  Branch:     %s\n  Commit:     %s\n' '$(GREEN)' '$(RESET)' '$(GIT_BRANCH_STATUS)' '$(GIT_COMMIT)'
	@printf '\n%bTools:%b\n' '$(GREEN)' '$(RESET)'
	@rustc --version
	@$(CARGO) --version
	@$(CARGO) fmt --version
	@$(CARGO) clippy --version
	@printf '\n%bBinaries in %s:%b\n' '$(GREEN)' '$(BIN_DIR)' '$(RESET)'
	@for bin in $(BINARIES); do \
		if [ -x "$(BIN_DIR)/$$bin" ]; then \
			printf '  %b✓ %s%b\n' '$(GREEN)' "$$bin" '$(RESET)'; \
		else \
			printf '  %b✗ %s (run make build)%b\n' '$(YELLOW)' "$$bin" '$(RESET)'; \
		fi; \
	done
	@printf '\n'

check-tools:
	@printf '%bChecking required tools...%b\n' '$(BLUE)' '$(RESET)'
	@status=0; \
	for tool in rustc '$(CARGO)' '$(CC)' git; do \
		if "$$tool" --version >/dev/null 2>&1; then \
			printf '  %b✓ %s%b\n' '$(GREEN)' "$$tool" '$(RESET)'; \
		else \
			printf '  %b✗ %s unavailable%b\n' '$(RED)' "$$tool" '$(RESET)'; status=1; \
		fi; \
	done; \
	for component in fmt clippy; do \
		if $(CARGO) "$$component" --version >/dev/null 2>&1; then \
			printf '  %b✓ cargo %s%b\n' '$(GREEN)' "$$component" '$(RESET)'; \
		else \
			printf '  %b✗ cargo %s unavailable (see rust-toolchain.toml)%b\n' '$(RED)' "$$component" '$(RESET)'; status=1; \
		fi; \
	done; \
	exit $$status
	@printf '%bOptional tools (not required for make check):%b\n' '$(CYAN)' '$(RESET)'
	@for tool in llvm-cov outdated; do \
		if $(CARGO) "$$tool" --version >/dev/null 2>&1; then \
			printf '  %b✓ cargo %s%b\n' '$(GREEN)' "$$tool" '$(RESET)'; \
		else \
			printf '  %b• cargo %s unavailable%b\n' '$(YELLOW)' "$$tool" '$(RESET)'; \
		fi; \
	done

# Keep debug/release selection and output paths in agreement.
check-mode:
	@case '$(MODE)' in debug|release) ;; \
		*) printf '%b✗ MODE must be debug or release%b\n' '$(RED)' '$(RESET)' >&2; exit 1 ;; \
	esac

# Build incrementally; copy every expected binary or fail. Never clean as a prerequisite.
build: check-mode
	@printf '%bBuilding Cayrn in %s mode...%b\n' '$(BLUE)' '$(MODE)' '$(RESET)'
	$(CARGO) build --workspace --locked $(PROFILE_FLAG)
	@mkdir -p "$(BIN_DIR)"
	@set -e; for bin in $(BINARIES); do \
		cp "$(TARGET)/$$bin" "$(BIN_DIR)/$$bin"; \
		printf '  %b✓ %s → %s/%s%b\n' '$(GREEN)' "$$bin" '$(BIN_DIR)' "$$bin" '$(RESET)'; \
	done

build-release:
	+$(MAKE) build MODE=release

# Preserve unrelated files in BIN_DIR and the incremental Cargo cache.
clean:
	@printf '%bRemoving Cayrn binaries from %s...%b\n' '$(BLUE)' '$(BIN_DIR)' '$(RESET)'
	@set -e; for bin in $(BINARIES); do rm -f "$(BIN_DIR)/$$bin"; done
	@rmdir "$(BIN_DIR)" 2>/dev/null || :

clean-all: clean
	$(CARGO) clean

test:
	@printf '%bRunning workspace tests...%b\n' '$(BLUE)' '$(RESET)'
	$(CARGO) test --workspace --all-targets --locked

fmt-check:
	@printf '%bChecking Rust formatting...%b\n' '$(BLUE)' '$(RESET)'
	$(CARGO) fmt --all -- --check

lint: fmt-check
	@printf '%bRunning Clippy...%b\n' '$(BLUE)' '$(RESET)'
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings

format:
	@printf '%bFormatting Rust sources...%b\n' '$(BLUE)' '$(RESET)'
	$(CARGO) fmt --all

fmt: format

doc:
	@printf '%bBuilding workspace API documentation...%b\n' '$(BLUE)' '$(RESET)'
	RUSTDOCFLAGS="$(RUSTDOCFLAGS) -D warnings" $(CARGO) doc --workspace --no-deps --locked

check: build lint test doc
	@printf '\n%b✓ All checks passed (build + lint + test + docs)%b\n\n' '$(GREEN)' '$(RESET)'

check-all: check coverage
	@printf '\n%b✓ All checks and coverage complete%b\n\n' '$(GREEN)' '$(RESET)'

# Optional tools are installed explicitly, never as a side effect of checking.
coverage:
	@$(CARGO) llvm-cov --version >/dev/null 2>&1 || { \
		printf '%bInstall cargo-llvm-cov and llvm-tools-preview first.%b\n' '$(YELLOW)' '$(RESET)' >&2; exit 1; }
	$(CARGO) llvm-cov --workspace --all-targets --locked --summary-only

coverage-html:
	@$(CARGO) llvm-cov --version >/dev/null 2>&1 || { \
		printf '%bInstall cargo-llvm-cov and llvm-tools-preview first.%b\n' '$(YELLOW)' '$(RESET)' >&2; exit 1; }
	$(CARGO) llvm-cov --workspace --all-targets --locked --html
	@printf '%b→ Report: %s/llvm-cov/html/index.html%b\n' '$(CYAN)' '$(CARGO_TARGET_DIR)' '$(RESET)'

check-deps:
	@$(CARGO) outdated --version >/dev/null 2>&1 || { \
		printf '%bInstall cargo-outdated first: cargo install cargo-outdated --locked%b\n' '$(YELLOW)' '$(RESET)' >&2; exit 1; }
	$(CARGO) outdated --workspace --root-deps-only --exit-code 1

# Explicit mutation of the lockfile; manifest version requirements stay unchanged.
deps:
	$(CARGO) update
	@printf '%b✓ Dependencies updated; review Cargo.lock and rerun make check.%b\n' '$(GREEN)' '$(RESET)'

tracked-files:
	@mkdir -p "$(CARGO_TARGET_DIR)"
	@git ls-files > "$(CARGO_TARGET_DIR)/git-tracked-files.txt"
	@printf '%b✓ Tracked paths saved to %s/git-tracked-files.txt%b\n' '$(GREEN)' '$(CARGO_TARGET_DIR)' '$(RESET)'

# Cayrn has one GitHub origin and a separate orphan planning branch.
push:
	git push --atomic origin main planning --tags

# Publishing is intentionally absent: all current workspace packages set publish = false.

# Real BEAM integration is explicit; ordinary Rust checks need no OTP installation.
CAYRN_LFE_DIR ?= ../../lfe/cayrn-lfe
.PHONY: test-agent
test-agent: build
	$(MAKE) -C "$(CAYRN_LFE_DIR)" check
	python3 scripts/test-native-agent.py --agent-dir "$(CAYRN_LFE_DIR)"
