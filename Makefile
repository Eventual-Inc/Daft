.DEFAULT_GOAL := help

SHELL=/bin/bash
VENV := .venv
IS_M1 ?= 0
PYTHON_VERSION ?= python3.11
FORCE_RECREATE ?= 0

ifeq ($(origin FREETHREADED), undefined)
ifneq ($(wildcard $(VENV)/.build-*-freethreaded),)
FREETHREADED := 1
else ifneq ($(wildcard $(VENV)/.build-*-default),)
FREETHREADED := 0
endif
endif
FREETHREADED ?= 0

ifeq ($(FREETHREADED), 1)
PYTHON_VERSION := python3.14t
UV_SYNC_ARGS := --no-install-project --only-group dev-freethreaded
MATURIN_EXTRAS :=
VENV_PROFILE := freethreaded
else
UV_SYNC_ARGS := --no-install-project --extra all --no-group dev-freethreaded
MATURIN_EXTRAS := --extras=all
VENV_PROFILE := default
endif

VENV_SENTINEL := $(VENV)/.build-$(PYTHON_VERSION)-$(VENV_PROFILE)


# Hypothesis
HYPOTHESIS_MAX_EXAMPLES ?= 100
HYPOTHESIS_SEED ?= 0

# TPC-DS
SCALE_FACTOR ?= 1
OUTPUT_DIR ?= data/tpc-ds/


ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif


.PHONY: .venv
.venv:
	$(MAKE) $(VENV_SENTINEL) FREETHREADED=$(FREETHREADED) FORCE_RECREATE=1

$(VENV_SENTINEL):
	@which uv > /dev/null || (echo "Error: uv is required but not installed. Please install uv first." && exit 1)
	@if [ "$(FORCE_RECREATE)" = "1" ] || [ ! -f "$(VENV_SENTINEL)" ]; then \
		rm -rf $(VENV); \
		uv venv --clear -p $(PYTHON_VERSION) $(VENV); \
		if [ "$(IS_M1)" = "1" ]; then \
			GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 \
			GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 \
			CFLAGS="${CFLAGS} -I /opt/homebrew/opt/openssl/include" \
			LDFLAGS="${LDFLAGS} -L /opt/homebrew/opt/openssl/lib" \
			uv sync $(UV_SYNC_ARGS); \
		else \
			uv sync $(UV_SYNC_ARGS); \
		fi; \
		if [ "$(FREETHREADED)" = "1" ]; then \
			uv pip install maturin --python $(VENV_BIN)/python >/dev/null; \
		fi; \
		rm -f $(VENV)/.build-*; \
		touch $(VENV_SENTINEL); \
	fi

.PHONY: check-toolchain
check-toolchain:
	@TOOLCHAIN="$(shell rustup show active-toolchain)"; \
	if echo "$$TOOLCHAIN" | grep -q 'rust-toolchain.toml'; \
	then \
		echo "Toolchain is correct, continuing with build"; \
	else \
		echo "Failed to build: rust using incorrect toolchain: $$TOOLCHAIN"; \
		exit 1; \
	fi

.PHONY: hooks
hooks: $(VENV_SENTINEL)
	source $(VENV_BIN)/activate && pre-commit install --install-hooks

.PHONY: build
build: check-toolchain $(VENV_SENTINEL)  ## Compile and install Daft for development
	@unset CONDA_PREFIX && PYO3_PYTHON=$(VENV_BIN)/python $(VENV_BIN)/maturin develop $(MATURIN_EXTRAS) --uv

.PHONY: build-release
build-release: check-toolchain $(VENV_SENTINEL)  ## Compile and install a faster Daft binary
	@unset CONDA_PREFIX && PYO3_PYTHON=$(VENV_BIN)/python $(VENV_BIN)/maturin develop --release --uv

.PHONY: build-whl
build-whl: check-toolchain $(VENV_SENTINEL)  ## Compile Daft for development, only generate whl file without installation
	cargo clean --target-dir target
	@unset CONDA_PREFIX && PYO3_PYTHON=$(VENV_BIN)/python $(VENV_BIN)/maturin build

.PHONY: test
test: $(VENV_SENTINEL) build  ## Run tests
	# You can set additional run parameters through EXTRA_ARGS, such as running a specific test case file or method:
	# make test EXTRA_ARGS="-v tests/dataframe/test_select.py" # Run a single test file
	# make test EXTRA_ARGS="-v tests/dataframe/test_select.py::test_select_dataframe" # Run a single test method
	HYPOTHESIS_MAX_EXAMPLES=$(HYPOTHESIS_MAX_EXAMPLES) $(VENV_BIN)/pytest --hypothesis-seed=$(HYPOTHESIS_SEED) --ignore tests/integration $(EXTRA_ARGS)

.PHONY: doctests
doctests: $(VENV_SENTINEL)
	DAFT_BOLD_TABLE_HEADERS=0 DAFT_PROGRESS_BAR=0 $(VENV_BIN)/pytest --doctest-modules --continue-on-collection-errors --ignore=daft/functions/llm.py --ignore=daft/functions/ai/__init__.py daft/dataframe/dataframe.py daft/expressions/expressions.py daft/convert.py daft/udf/__init__.py daft/functions/ daft/datatype.py

.PHONY: dsdgen
dsdgen: $(VENV_SENTINEL) ## Generate TPC-DS data
	$(VENV_BIN)/python benchmarking/tpcds/datagen.py --scale-factor=$(SCALE_FACTOR) --tpcds-gen-folder=$(OUTPUT_DIR)

.PHONY: install-docs-deps
install-docs-deps:
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "Installing Bun..."; \
		curl -fsSL https://bun.sh/install | bash; \
		export PATH="$$HOME/.bun/bin:$$PATH"; \
	fi
	source $(VENV_BIN)/activate && uv sync --all-extras --all-groups
	source $(VENV_BIN)/activate && uv pip install -e docs/plugins/nav_hide_children

.PHONY: docs
docs: $(VENV_SENTINEL) install-docs-deps ## Build Daft documentation
	JUPYTER_PLATFORM_DIRS=1 uv run mkdocs build -f mkdocs.yml

.PHONY: docs-serve
docs-serve: $(VENV_SENTINEL) install-docs-deps ## Build Daft documentation in development server
	JUPYTER_PLATFORM_DIRS=1 uv run mkdocs serve -f mkdocs.yml

.PHONY: daft-proto
daft-proto: check-toolchain $(VENV_SENTINEL) ## Build Daft proto sources to avoid protoc build-time dependency.
	trap 'mv src/daft-proto/build.rs src/daft-proto/.build.rs' EXIT && mv src/daft-proto/.build.rs src/daft-proto/build.rs && cargo build -p daft-proto

.PHONY: check-format
check-format: check-toolchain $(VENV_SENTINEL)  ## Check if code is properly formatted
	source $(VENV_BIN)/activate && pre-commit run --all-files

.PHONY: format-check
format-check: check-format  ## Alias for check-format

format: check-toolchain $(VENV_SENTINEL)  ## Format Python and Rust code
	source $(VENV_BIN)/activate && pre-commit run ruff-format --all-files
	source $(VENV_BIN)/activate && pre-commit run fmt --all-files

.PHONY: lint
lint: check-toolchain $(VENV_SENTINEL)  ## Lint Python and Rust code
	source $(VENV_BIN)/activate && pre-commit run ruff --all-files
	source $(VENV_BIN)/activate && pre-commit run clippy --all-files

.PHONY: precommit
precommit:  check-toolchain $(VENV_SENTINEL)  ## Run all pre-commit hooks
	source $(VENV_BIN)/activate && pre-commit run --all-files

.PHONY: clean
clean:
	rm -rf $(VENV)
	rm -rf ./target
	rm -rf ./site
	rm -f daft/daft.abi3.so
