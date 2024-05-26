.DEFAULT_GOAL := help

SHELL=/bin/bash
VENV = .venv
IS_M1 ?= 1

# Hypothesis
HYPOTHESIS_MAX_EXAMPLES ?= 100
HYPOTHESIS_SEED ?= 1

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif


.venv:  ## Set up virtual environment
	python3 -m venv $(VENV)
	$(VENV_BIN)/python -m pip install --upgrade uv
	## Hacks to deal with grpcio compile errors on m1 macs
ifeq ($(IS_M1), 1)
	GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1	\
	GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1	\
	CFLAGS="${CFLAGS} -I /opt/homebrew/opt/openssl/include"	\
	LDFLAGS="${LDFLAGS} -L /opt/homebrew/opt/openssl/lib" \
	$(VENV_BIN)/uv pip install -r requirements-dev.txt
else
	$(VENV_BIN)/uv pip install -r requirements-dev.txt
endif

.PHONY: hooks
hooks: .venv
	source $(VENV_BIN)/activate && pre-commit install --install-hooks

.PHONY: build
build: .venv  ## Compile and install Daft for development
	$(CONDA_PREFIX)/bin/maturin develop --extras=all

.PHONY: build-release
build-release: .venv  ## Compile and install a faster Daft binary
	@unset CONDA_PREFIX && $(VENV_BIN)/maturin develop --release

.PHONY: test
test: .venv build  ## Run tests
	HYPOTHESIS_MAX_EXAMPLES=$(HYPOTHESIS_MAX_EXAMPLES) $(CONDA_PREFIX)/bin/pytest tests/table/test_between.py --hypothesis-seed=$(HYPOTHESIS_SEED)

.PHONY: clean
clean:
	rm -rf .venv
