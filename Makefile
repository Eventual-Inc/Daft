DOCKER ?= docker

IMAGES=daftlet reader sleepy web

K8S_CLUSTER_NAME="kind-${USER}-kind"

TILT_PORT ?= 10350

define BUILD_IMAGE
BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@
endef

${IMAGES}:
	@echo $@
	$(call BUILD_IMAGE)

gen-fbs:
	flatc --go --grpc -o ./codegen/go ./fbs/*.fbs

dist:
	BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@ --output .

start-local-cluster:
	@kubectl cluster-info --context ${K8S_CLUSTER_NAME} || /bin/bash scripts/kind-with-registry.sh

stop-local-cluster:
	/bin/bash scripts/teardown-kind-with-registry.sh

local-dev: start-local-cluster
	tilt up --stream=true --port ${TILT_PORT}

.PHONY: ${IMAGES} start-local-cluster local-dev dist