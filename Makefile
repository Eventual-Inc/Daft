
NOTEBOOK_IMAGE_TAG ?= latest
CLUSTER_NAME ?= default
init:
	poetry install
	git config core.hooksPath .githooks

test:
	poetry run pytest

ray-up:
	@helm install ${CLUSTER_NAME} ./kubernetes-ops/ray-static-cluster --set clusterName=${CLUSTER_NAME} --namespace ray --create-namespace
	@kubectl get pods -n ray

ray-down:
	@helm uninstall -n ray ${CLUSTER_NAME}
	@kubectl get pods -n ray

deploy-notebook-image:
	@DOCKER_BUILDKIT=1 docker build . -t 941892620273.dkr.ecr.us-west-2.amazonaws.com/daft/notebook:${NOTEBOOK_IMAGE_TAG}
	@docker push 941892620273.dkr.ecr.us-west-2.amazonaws.com/daft/notebook:${NOTEBOOK_IMAGE_TAG}
