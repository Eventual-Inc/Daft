DOCKER ?= docker

IMAGES=daftlet reader sleepy web

K8S_CLUSTER_NAME="kind-${USER}-kind"

TILT_PORT :=

ENVFILE := $(strip $(wildcard .env))

# Look for .env file otherwise or .env.example
ifneq (${ENVFILE}, .env)
	ENVFILE = .env.example
endif

# Include variables in .env file
include ${ENVFILE}
export $(shell sed 's/=.*//' ${ENVFILE})

define BUILD_IMAGE
BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@
endef

# Recursive wildcard from https://stackoverflow.com/questions/2483182/recursive-wildcards-in-gnu-make/18258352#18258352
rwildcard=$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))


${IMAGES}:
	@echo $@
	$(call BUILD_IMAGE)

FLATBUFFERS = $(call rwildcard,./fbs,*.fbs)

gen-fbs:
	rm -rf ./codegen/go && flatc --go --grpc -o ./codegen/go ${FLATBUFFERS}

dist:
	BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@ --output .

start-local-cluster:
	@kubectl cluster-info --context ${K8S_CLUSTER_NAME} || /bin/bash scripts/kind-with-registry.sh

stop-local-cluster:
	/bin/bash scripts/teardown-kind-with-registry.sh

local-dev: start-local-cluster
	tilt up --stream=true --port ${TILT_PORT}

.PHONY: ${IMAGES} start-local-cluster local-dev dist