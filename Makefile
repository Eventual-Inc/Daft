DOCKER ?= docker

IMAGES=runtime reader flatc


define BUILD_IMAGE
BUILDKIT_PROGRESS=plain DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@
endef

${IMAGES}:
	@echo $@
	$(call BUILD_IMAGE)

gen-fbs: flatc
	$(DOCKER) run --rm -e UID=`id -u` -v `pwd`/codegen:/codegen:rw flatc:latest

.PHONY: ${IMAGES}