.DEFAULT_GOAL := help

SHELL=/bin/bash
VENV = .venv
IS_M1 ?= 0

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
	uv sync --no-install-project
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

.PHONY: docs
docs: .venv sphinx-docs ## Build both MkDocs and Sphinx documentation
	JUPYTER_PLATFORM_DIRS=1 uv run --with-requirements requirements-docs.txt mkdocs build -f docs/mkdocs.yml

.PHONY: sphinx-docs
sphinx-docs: .venv ## Build Sphinx API documentation
	uv run --with-requirements requirements-docs.txt sphinx-build -b html "docs/sphinx/source" "docs/sphinx/_build"

.PHONY: clean
clean:
	rm -rf $(VENV)
