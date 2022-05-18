
CLUSTER_NAME ?= ${USER}-default
init:
	poetry install
	git config core.hooksPath .githooks

test:
	poetry run pytest

ray-up:
	@helm install ${CLUSTER_NAME} ./kubernetes-ops/ray-static-cluster --set clusterName=${CLUSTER_NAME} --namespace ray --create-namespace
	@kubectl get pods -n ray