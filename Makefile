DOCKER ?= docker

IMAGES=runtime reader


define BUILD_IMAGE
BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@
endef

${IMAGES}:
	@echo $@
	$(call BUILD_IMAGE)

cli:
	BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t cli-builder:latest --target cli --output build


start-local-cluster:
	@kubectl cluster-info --context kind-kind || /bin/bash scripts/kind-with-registry.sh

stop-local-cluster:
	/bin/bash scripts/teardown-kind-with-registry.sh

local-dev: start-local-cluster
	tilt up --stream=true

.PHONY: ${IMAGES} start-local-cluster local-dev