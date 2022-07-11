ECR_PREFIX ?= 941892620273.dkr.ecr.us-west-2.amazonaws.com
RAY_IMAGE_TAG ?= latest
NOTEBOOK_IMAGE_TAG ?= latest
CLUSTER_NAME ?= default
EVENTUAL_HUB_RELEASE_TAG ?= latest
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

###
# Deployment of Docker images
###

deploy-ray-image:
	@DOCKER_BUILDKIT=1 docker build --platform linux/amd64 . -f Dockerfile.ray -t ${ECR_PREFIX}/daft/ray:${RAY_IMAGE_TAG}
	@docker push ${ECR_PREFIX}/daft/ray:${RAY_IMAGE_TAG}

deploy-notebook-image:
	@DOCKER_BUILDKIT=1 docker build --platform linux/amd64 . -f Dockerfile.notebook -t ${ECR_PREFIX}/daft/notebook:${NOTEBOOK_IMAGE_TAG}
	@docker push ${ECR_PREFIX}/daft/notebook:${NOTEBOOK_IMAGE_TAG}

deploy-eventual-hub-images:
	@DOCKER_BUILDKIT=1 docker build --platform linux/amd64 ./eventual-hub -f Dockerfile.jupyterhub -t ${ECR_PREFIX}/eventual/jupyterhub:${EVENTUAL_HUB_RELEASE_TAG}
	@DOCKER_BUILDKIT=1 docker build --platform linux/amd64 ./eventual-hub -f Dockerfile.backend -t ${ECR_PREFIX}/eventual/backend:${EVENTUAL_HUB_RELEASE_TAG}
	@DOCKER_BUILDKIT=1 docker build --platform linux/amd64 ./eventual-hub -f Dockerfile.frontend -t ${ECR_PREFIX}/eventual/frontend:${EVENTUAL_HUB_RELEASE_TAG}
	@docker push ${ECR_PREFIX}/eventual/jupyterhub:${EVENTUAL_HUB_RELEASE_TAG}
	@docker push ${ECR_PREFIX}/eventual/backend:${EVENTUAL_HUB_RELEASE_TAG}
	@docker push ${ECR_PREFIX}/eventual/frontend:${EVENTUAL_HUB_RELEASE_TAG}

###
# Deployment of environments (local/dev/prod)
###

deploy-eventual-hub:
	@echo "Switching kubectl to jay_sandbox_eks_cluster..."
	@kubectl config use-context arn:aws:eks:us-west-2:941892620273:cluster/jay_sandbox_eks_cluster
	kubectl apply -k kubernetes-ops/eventual-hub/_pre
	kubectl apply -k kubernetes-ops/eventual-hub/installs/cluster_dev

local-dev:
	ctlptl apply -f kubernetes-ops/ctlptl.yaml
	tilt up; ret=$$?; \
	ctlptl delete cluster kind; \
	exit $$ret
