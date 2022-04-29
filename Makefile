DOCKER ?= docker

IMAGES=runtime reader


define BUILD_IMAGE
DOCKER_BUILDKIT=1 $(DOCKER) build . -t $@:latest --target $@
endef

${IMAGES}:
	@echo $@
	$(call BUILD_IMAGE)

.PHONY: images