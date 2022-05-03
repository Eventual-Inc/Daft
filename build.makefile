GO ?= go

COMMANDS=reader daftlet cli web
BINARIES=$(addprefix bin/,$(COMMANDS))


all: binaries

binaries: ${BINARIES}
	@echo $@

define BUILD_BINARY
@$(GO) build ${DEBUG_GO_GCFLAGS} ${GO_GCFLAGS} ${GO_BUILD_FLAGS} -o $@ ${GO_LDFLAGS} ${GO_TAGS}  ./$<
endef

bin/%: cmd/%
	$(call BUILD_BINARY)

.PHONY: all binaries