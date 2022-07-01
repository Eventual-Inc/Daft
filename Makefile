ECR_PREFIX ?= 941892620273.dkr.ecr.us-west-2.amazonaws.com
RAY_IMAGE_TAG ?= latest
NOTEBOOK_IMAGE_TAG ?= latest
CLUSTER_NAME ?= default
WORKER_REPLICA_COUNT ?= 4

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

init:
	poetry install
	git config core.hooksPath .githooks

test:
	poetry run pytest

ray-up:
	@helm install ${CLUSTER_NAME} ./kubernetes-ops/ray-static-cluster --set clusterName=${CLUSTER_NAME} --set workerReplicaCount=${WORKER_REPLICA_COUNT} --namespace ray --create-namespace
	@kubectl get pods -n ray

ray-down:
	@helm uninstall -n ray ${CLUSTER_NAME}
	@kubectl get pods -n ray

deploy-package-zip:
	@poetry build
	@mkdir -p dist/full-package
	@poetry run pip install --upgrade -t dist/full-package dist/*.whl
	@cd dist/full-package && zip -r - . -x '*.pyc' > ../full-package.zip
	@aws s3 cp dist/full-package.zip s3://eventual-release-artifacts-bucket/daft_package-amd64/latest.zip

deploy-ray-image:
	@DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ray -t ${ECR_PREFIX}/daft/ray:${RAY_IMAGE_TAG}
	@docker push ${ECR_PREFIX}/daft/ray:${RAY_IMAGE_TAG}

deploy-notebook-image:
	@DOCKER_BUILDKIT=1 docker build . -f Dockerfile.notebook -t ${ECR_PREFIX}/daft/notebook:${NOTEBOOK_IMAGE_TAG}
	@docker push ${ECR_PREFIX}/daft/notebook:${NOTEBOOK_IMAGE_TAG}

deploy-jupyterhub:
ifndef AUTH0_JUPYTERHUB_CLIENT_SECRET
	$(error AUTH0_JUPYTERHUB_CLIENT_SECRET is undefined)
endif
	helm upgrade --cleanup-on-fail \
		--install jupyterhub \
		kubernetes-ops/jupyterhub \
		--namespace jupyterhub \
		--values kubernetes-ops/jupyterhub/values.yaml \
		--set jupyterhub.hub.config.Auth0OAuthenticator.client_secret=${AUTH0_JUPYTERHUB_CLIENT_SECRET}
