DOCKER ?= docker

IMAGES=runtime reader

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

gen-web-service-openapi:
	@mkdir -p codegen/openapi/go
	@docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
		-i /local/api/web_service/openapi.yaml \
		-g go \
		-o /local/codegen/openapi/go

dist:
	BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@ --output .

start-local-cluster:
	@kubectl cluster-info --context ${K8S_CLUSTER_NAME} || /bin/bash scripts/kind-with-registry.sh

stop-local-cluster:
	/bin/bash scripts/teardown-kind-with-registry.sh

local-dev: start-local-cluster
	tilt up --stream=true --port ${TILT_PORT}

.PHONY: ${IMAGES} start-local-cluster local-dev dist