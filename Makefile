DOCKER ?= docker

IMAGES=daftlet reader

K8S_CLUSTER_NAME="kind-${USER}-kind"

TILT_PORT ?= 10350

OPENAPI_YAML_PATH ?= "api/web_service/openapi.yaml"

OPENAPI_GENERATOR ?= "go"

define BUILD_IMAGE
BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@
endef

define GEN_OPENAPI_STUBS
@mkdir -p codegen/openapi/$@
@docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
		-i /local/$(OPENAPI_YAML_PATH) \
		-g $(OPENAPI_GENERATOR) \
		-o /local/codegen/openapi/$@
@rm -f codegen/openapi/$@/go.mod
@rm -f codegen/openapi/$@/go.sum
endef

${IMAGES}:
	@echo $@
	$(call BUILD_IMAGE)

gen-fbs:
	flatc --go --grpc -o ./codegen/go ./fbs/*.fbs

gen-openapi-web-client:
	OPENAPI_YAML_PATH=api/web_service/openapi.yaml
	OPENAPI_GENERATOR=go
	$(call GEN_OPENAPI_STUBS)

gen-openapi-web-service:
	OPENAPI_YAML_PATH=api/web_service/openapi.yaml
	OPENAPI_GENERATOR=go-server
	$(call GEN_OPENAPI_STUBS)

dist:
	BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@ --output .

start-local-cluster:
	@kubectl cluster-info --context ${K8S_CLUSTER_NAME} || /bin/bash scripts/kind-with-registry.sh

stop-local-cluster:
	/bin/bash scripts/teardown-kind-with-registry.sh

local-dev: start-local-cluster
	tilt up --stream=true --port ${TILT_PORT}

.PHONY: ${IMAGES} start-local-cluster local-dev dist