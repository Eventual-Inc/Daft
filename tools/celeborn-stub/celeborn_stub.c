#include <stdio.h>
#include <stdlib.h>

// Stub implementations of the celeborn client C FFI symbols.
//
// Enabling the `celeborn` cargo feature links against libceleborn_client -- the
// aggregated shared library of the Apache Celeborn C++ client. Apache Celeborn
// publishes no prebuilt copy of it, so without one on the machine the build has
// to compile Celeborn's `cpp/` tree from source (folly, glog, gflags, OpenSSL)
// or it fails outright. That burden falls on two builds that never run a
// Celeborn shuffle: `cargo clippy --all-features`, which lints unrelated Rust
// code, and `make build CELEBORN=1`, when a developer only wants to compile the
// feature rather than exercise it.
//
// This stub satisfies both the linker and the dynamic loader; any actual
// Celeborn call aborts loudly rather than misbehaving. See the `celeborn-stub`
// target in the Makefile, and set CELEBORN_CPP_PREFIX to build against a real
// client instead.

// The message has to name CELEBORN_CPP_PREFIX: reaching a stub means the
// process linked the stub library instead of a real client, which is a build
// misconfiguration, not a bug in the caller.
#define STUB(name) \
    void name(void) { \
        fprintf(stderr, \
                "FATAL: " #name " called on the Celeborn stub library, which " \
                "implements nothing. This build linked tools/celeborn-stub " \
                "instead of a real libceleborn_client. Point " \
                "CELEBORN_CPP_PREFIX at a Celeborn C++ client install and " \
                "rebuild.\n"); \
        abort(); \
    }

STUB(celeborn_ffi_close_partition_reader)
STUB(celeborn_ffi_create_client)
STUB(celeborn_ffi_free_error)
STUB(celeborn_ffi_mapper_end)
STUB(celeborn_ffi_open_partition_reader)
STUB(celeborn_ffi_push_data)
STUB(celeborn_ffi_read_partition_chunk)
STUB(celeborn_ffi_setup_lifecycle_manager)
STUB(celeborn_ffi_shutdown)
STUB(celeborn_ffi_update_reducer_file_group)
