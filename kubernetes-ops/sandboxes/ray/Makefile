.SILENT: help
.PHONY: help
help:
	echo Available recipes:
	cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' | awk 'BEGIN { FS = ":.*?## " } { cnt++; a[cnt] = $$1; b[cnt] = $$2; if (length($$1) > max) max = length($$1) } END { for (i = 1; i <= cnt; i++) printf "  $(shell tput setaf 6)%-*s$(shell tput setaf 0) %s\n", max, a[i], b[i] }'
	tput sgr0

export NOTEBOOK_HOST_PORT := 8888
export KIND_CLUSTER_NAME := ray

BREW_BIN := $(shell brew --prefix)/bin

.PHONY: _install-prereqs
_install-prereqs:
	test -x $(BREW_BIN)/envsubst || brew install gettext
	test -x $(BREW_BIN)/kind || brew install kind
	test -x $(BREW_BIN)/kubectl || brew install kubernetes-cli
	test -x $(BREW_BIN)/kustomize || brew install kustomize
	test -x $(BREW_BIN)/skaffold || brew install skaffold
	test -x $(BREW_BIN)/timeout || brew install coreutils

.PHONY: _create-k8s-cluster
_create-k8s-cluster: _install-prereqs
	($(BREW_BIN)/kind get clusters | grep -q $(KIND_CLUSTER_NAME)) || \
	cat kind.cfg.tmpl | $(BREW_BIN)/envsubst | $(BREW_BIN)/kind create cluster --name=$(KIND_CLUSTER_NAME) --config=/dev/stdin

.PHONY: start
start: _create-k8s-cluster ## Start Kind cluster
	$(BREW_BIN)/kubectl --context=kind-$(KIND_CLUSTER_NAME) apply -k kustomize/_pre
	$(BREW_BIN)/skaffold run --kube-context=kind-$(KIND_CLUSTER_NAME) --wait-for-deletions=false --cleanup=false

.PHONY: stop
stop: ## Stop Kind cluster
	$(BREW_BIN)/kind delete cluster --name=$(KIND_CLUSTER_NAME)

.PHONY: ui
ui: ## Open notebook in browser
	open http://127.0.0.1:$(NOTEBOOK_HOST_PORT)/lab?token=token
