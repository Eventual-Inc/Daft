.DEFAULT_GOAL := help

SHELL=/bin/bash
VENV = .venv
IS_M1 ?= 0
PYTHON_VERSION ?= python3.11

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


.venv:  ## Set up virtual environment
ifeq (, $(shell which uv))
	$(PYTHON_VERSION) -m venv $(VENV)
	$(VENV_BIN)/python -m pip install --upgrade uv
else
	uv venv $(VENV) -p $(PYTHON_VERSION)
endif
ifeq ($(IS_M1), 1)
	## Hacks to deal with grpcio compile errors on m1 macs
	GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 \
	GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1	\
	CFLAGS="${CFLAGS} -I /opt/homebrew/opt/openssl/include"	\
	LDFLAGS="${LDFLAGS} -L /opt/homebrew/opt/openssl/lib" \
	. $(VENV_BIN)/activate; uv pip install -r requirements-dev.txt
else
	. $(VENV_BIN)/activate; uv pip install -r requirements-dev.txt
endif

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
hooks: .venv
	source $(VENV_BIN)/activate && pre-commit install --install-hooks

.PHONY: build
build: check-toolchain .venv  ## Compile and install Daft for development
	@unset CONDA_PREFIX && PYO3_PYTHON=$(VENV_BIN)/python $(VENV_BIN)/maturin develop --extras=all --uv

.PHONY: build-release
build-release: check-toolchain .venv  ## Compile and install a faster Daft binary
	@unset CONDA_PREFIX && PYO3_PYTHON=$(VENV_BIN)/python $(VENV_BIN)/maturin develop --release --uv

.PHONY: build-whl
build-whl: check-toolchain .venv  ## Compile Daft for development, only generate whl file without installation
	cargo clean --target-dir target
	@unset CONDA_PREFIX && PYO3_PYTHON=$(VENV_BIN)/python $(VENV_BIN)/maturin build

.PHONY: test
test: .venv build  ## Run tests
	# You can set additional run parameters through EXTRA_ARGS, such as running a specific test case file or method:
	# make test EXTRA_ARGS="-v tests/dataframe/test_select.py" # Run a single test file
	# make test EXTRA_ARGS="-v tests/dataframe/test_select.py::test_select_dataframe" # Run a single test method
	HYPOTHESIS_MAX_EXAMPLES=$(HYPOTHESIS_MAX_EXAMPLES) $(VENV_BIN)/pytest --hypothesis-seed=$(HYPOTHESIS_SEED) --ignore tests/integration $(EXTRA_ARGS)

.PHONY: doctests
doctests:
	DAFT_BOLD_TABLE_HEADERS=0 pytest --doctest-modules --continue-on-collection-errors daft/dataframe/dataframe.py daft/expressions/expressions.py daft/convert.py daft/udf.py daft/functions/functions.py daft/datatype.py

.PHONY: dsdgen
dsdgen: .venv ## Generate TPC-DS data
	$(VENV_BIN)/python benchmarking/tpcds/datagen.py --scale-factor=$(SCALE_FACTOR) --tpcds-gen-folder=$(OUTPUT_DIR)

.PHONY: install-docs-deps
install-docs-deps:
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "Installing Bun..."; \
		curl -fsSL https://bun.sh/install | bash; \
		export PATH="$$HOME/.bun/bin:$$PATH"; \
	fi
	. $(VENV_BIN)/activate && uv pip install -r requirements-doc.txt
	. $(VENV_BIN)/activate && yamlfix mkdocs.yml

.PHONY: docs
docs: .venv install-docs-deps ## Build Daft documentation
	JUPYTER_PLATFORM_DIRS=1 uv run mkdocs build -f mkdocs.yml

.PHONY: docs-serve
docs-serve: .venv install-docs-deps ## Build Daft documentation in development server
	JUPYTER_PLATFORM_DIRS=1 uv run mkdocs serve -f mkdocs.yml

.PHONY: daft-proto
daft-proto: check-toolchain .venv ## Build Daft proto sources to avoid protoc build-time dependency.
	trap 'mv src/daft-proto/build.rs src/daft-proto/.build.rs' EXIT && mv src/daft-proto/.build.rs src/daft-proto/build.rs && cargo build -p daft-proto

.PHONY: check-format
check-format: check-toolchain .venv  ## Check if code is properly formatted
	source $(VENV_BIN)/activate && pre-commit run --all-files

.PHONY: format-check
format-check: check-format  ## Alias for check-format

format: check-toolchain .venv  ## Format Python and Rust code
	source $(VENV_BIN)/activate && pre-commit run ruff-format --all-files
	source $(VENV_BIN)/activate && pre-commit run fmt --all-files

.PHONY: lint
lint: check-toolchain .venv  ## Lint Python and Rust code
	source $(VENV_BIN)/activate && pre-commit run ruff --all-files
	source $(VENV_BIN)/activate && pre-commit run clippy --all-files

.PHONY: precommit
precommit:  check-toolchain .venv  ## Run all pre-commit hooks
	source $(VENV_BIN)/activate && pre-commit run --all-files

.PHONY: clean
clean:
	rm -rf $(VENV)
	rm -rf ./target
	rm -rf ./site
	rm -f daft/daft.abi3.so
