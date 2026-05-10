#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

// ── Daft ABI (must match daft-ext repr(C) layouts) ─────────────────

static constexpr uint32_t DAFT_ABI_VERSION = 1;

struct FFI_ScalarFunction {
    const void *ctx;
    const char *(*name)(const void *ctx);
    int (*get_return_field)(const void *ctx, const ArrowSchema *args, size_t args_count,
                            ArrowSchema *ret, char **errmsg);
    int (*call)(const void *ctx, const ArrowArray *args, const ArrowSchema *args_schemas,
                size_t args_count, ArrowArray *ret_array, ArrowSchema *ret_schema, char **errmsg);
    void (*fini)(void *ctx);
};

struct FFI_SessionContext {
    void *ctx;
    int (*define_function)(void *ctx, FFI_ScalarFunction function);
};

struct FFI_Module {
    uint32_t daft_abi_version;
    const char *name;
    int (*init)(FFI_SessionContext *session);
    void (*free_string)(char *s);
};

// ── Helpers ────────────────────────────────────────────────────────

static char *alloc_error(const std::string &msg) { return strdup(msg.c_str()); }

static void module_free_string(char *s) { free(s); }

// ── greet_cpp scalar function ──────────────────────────────────────

static const char *greet_name(const void *) { return "greet_cpp"; }

static int greet_get_return_field(const void *, const ArrowSchema *args, size_t args_count,
                                  ArrowSchema *ret, char **errmsg) {
    if (args_count != 1) {
        *errmsg = alloc_error("greet_cpp: expected 1 argument, got " + std::to_string(args_count));
        return 1;
    }

    const char *fmt = args[0].format;
    if (strcmp(fmt, "u") != 0 && strcmp(fmt, "U") != 0) {
        *errmsg = alloc_error(std::string("greet_cpp: expected string argument, got format '") +
                              fmt + "'");
        return 1;
    }

    auto field = arrow::field("greet_cpp", arrow::utf8());
    auto status = arrow::ExportField(*field, ret);
    if (!status.ok()) {
        *errmsg = alloc_error("greet_cpp: failed to export return field: " + status.ToString());
        return 1;
    }
    return 0;
}

static int greet_call(const void *, const ArrowArray *args, const ArrowSchema *args_schemas,
                      size_t args_count, ArrowArray *ret_array, ArrowSchema *ret_schema,
                      char **errmsg) {
    if (args_count != 1) {
        *errmsg = alloc_error("greet_cpp: expected 1 argument, got " + std::to_string(args_count));
        return 1;
    }

    // The host transfers ownership via these pointers (same semantics as
    // Rust's ptr::read in the trampoline). Copy the C structs so Arrow C++
    // can consume them through ImportArray.
    ArrowArray in_array;
    memcpy(&in_array, &args[0], sizeof(ArrowArray));

    ArrowSchema in_schema;
    memcpy(&in_schema, &args_schemas[0], sizeof(ArrowSchema));

    auto import_result = arrow::ImportArray(&in_array, &in_schema);
    if (!import_result.ok()) {
        *errmsg =
            alloc_error("greet_cpp: failed to import array: " + import_result.status().ToString());
        return 1;
    }
    auto input = *import_result;

    arrow::StringBuilder builder;
    arrow::Status st;

    if (input->type_id() == arrow::Type::LARGE_STRING) {
        auto sa = std::static_pointer_cast<arrow::LargeStringArray>(input);
        for (int64_t i = 0; i < sa->length(); i++) {
            if (sa->IsNull(i)) {
                st = builder.AppendNull();
            } else {
                st = builder.Append("Hello, " + sa->GetString(i) + "!");
            }
            if (!st.ok()) {
                *errmsg = alloc_error("greet_cpp: " + st.ToString());
                return 1;
            }
        }
    } else {
        auto sa = std::static_pointer_cast<arrow::StringArray>(input);
        for (int64_t i = 0; i < sa->length(); i++) {
            if (sa->IsNull(i)) {
                st = builder.AppendNull();
            } else {
                st = builder.Append("Hello, " + sa->GetString(i) + "!");
            }
            if (!st.ok()) {
                *errmsg = alloc_error("greet_cpp: " + st.ToString());
                return 1;
            }
        }
    }

    auto finish_result = builder.Finish();
    if (!finish_result.ok()) {
        *errmsg = alloc_error("greet_cpp: " + finish_result.status().ToString());
        return 1;
    }

    st = arrow::ExportArray(**finish_result, ret_array, ret_schema);
    if (!st.ok()) {
        *errmsg = alloc_error("greet_cpp: failed to export result: " + st.ToString());
        return 1;
    }
    return 0;
}

static void greet_fini(void *) {}

// ── Module init ────────────────────────────────────────────────────

static int module_init(FFI_SessionContext *session) {
    FFI_ScalarFunction greet{};
    greet.ctx = nullptr;
    greet.name = greet_name;
    greet.get_return_field = greet_get_return_field;
    greet.call = greet_call;
    greet.fini = greet_fini;
    return session->define_function(session->ctx, greet);
}

// ── Entry point ────────────────────────────────────────────────────

extern "C" FFI_Module daft_module_magic() {
    FFI_Module mod{};
    mod.daft_abi_version = DAFT_ABI_VERSION;
    mod.name = "hello_cpp";
    mod.init = module_init;
    mod.free_string = module_free_string;
    return mod;
}
