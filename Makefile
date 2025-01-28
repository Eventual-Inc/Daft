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
	python3 -m venv $(VENV)
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

.PHONY: test
test: .venv build  ## Run tests
	HYPOTHESIS_MAX_EXAMPLES=$(HYPOTHESIS_MAX_EXAMPLES) $(VENV_BIN)/pytest --hypothesis-seed=$(HYPOTHESIS_SEED)

.PHONY: dsdgen
dsdgen: .venv ## Generate TPC-DS data
	$(VENV_BIN)/python benchmarking/tpcds/datagen.py --scale-factor=$(SCALE_FACTOR) --tpcds-gen-folder=$(OUTPUT_DIR)

# .PHONY: html
# docs: .venv ## Serve docs
# 	JUPYTER_PLATFORM_DIRS=1 uv run --with-requirements requirements-docs.txt mkdocs build -f docs/mkdocs.yml

.PHONY: docs
docs: .venv sphinx-docs ## Build both MkDocs and Sphinx documentation
	JUPYTER_PLATFORM_DIRS=1 uv run --with-requirements requirements-docs.txt mkdocs build -f docs/mkdocs.yml

.PHONY: sphinx-docs
sphinx-docs: .venv ## Build Sphinx API documentation
	uv run --with-requirements requirements-docs.txt sphinx-build -b html "docs/sphinx/source" "docs/sphinx/_build/html"

.PHONY: clean
clean:
	rm -rf $(VENV)
