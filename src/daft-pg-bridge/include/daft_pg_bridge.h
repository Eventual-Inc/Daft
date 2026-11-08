#ifndef DAFT_PG_BRIDGE_V1_REFERENCE_H
#define DAFT_PG_BRIDGE_V1_REFERENCE_H

#include <stdint.h>
#include "arrow/c/abi.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DPG_ABI_MAJOR 1u
#define DPG_ABI_MINOR 1u
#define DPG_CAPABILITY_DIGEST_BYTES 32u

typedef struct dpg_handle dpg_handle;
typedef struct dpg_error dpg_error;

typedef enum dpg_status {
    DPG_OK = 0,
    DPG_AGAIN = 1,
    DPG_END = 2,
    DPG_ERROR = 3,
    DPG_INCOMPATIBLE = 4
} dpg_status;

typedef struct dpg_config_v1 {
    uint32_t struct_size;
    uint32_t worker_threads;
    uint32_t max_inflight_batches_per_source;
    uint64_t memory_limit_bytes;
    const char *spill_dir;
    uint32_t wait_timeout_ms;
} dpg_config_v1;

typedef enum dpg_spill_cleanup_status_v1 {
    DPG_SPILL_NOT_ENABLED = 0,
    DPG_SPILL_PENDING = 1,
    DPG_SPILL_CLEANED = 2,
    DPG_SPILL_CLEANUP_FAILED = 3
} dpg_spill_cleanup_status_v1;

typedef struct dpg_metrics_v1 {
    uint32_t struct_size;
    uint32_t reserved0;
    uint64_t input_rows;
    uint64_t input_batches;
    uint64_t input_bytes;
    uint64_t output_rows;
    uint64_t output_batches;
    uint64_t output_bytes;
    uint64_t operator_compute_us;
    uint64_t current_memory_bytes;
    uint64_t peak_memory_bytes;
    uint64_t spill_bytes;
    uint64_t spill_count;
    uint32_t spill_cleanup_status;
    uint32_t reserved1;
} dpg_metrics_v1;

uint32_t dpg_abi_major(void);
uint32_t dpg_abi_minor(void);
uint64_t dpg_capabilities(void);
dpg_status dpg_capability_digest_v1(
    uint8_t out_sha256[DPG_CAPABILITY_DIGEST_BYTES]);

dpg_status dpg_open_v1(
    const uint8_t *plan,
    uint64_t plan_len,
    const dpg_config_v1 *config,
    dpg_handle **out,
    dpg_error **err);

/* DPG_OK transfers array/schema ownership to bridge. Other results do not. */
dpg_status dpg_try_push_batch_v1(
    dpg_handle *handle,
    uint32_t source_id,
    struct ArrowArray *array,
    struct ArrowSchema *schema,
    dpg_error **err);

/* DPG_OK accepts the one Finish for this source; DPG_AGAIN does not. */
dpg_status dpg_finish_source_v1(
    dpg_handle *handle,
    uint32_t source_id,
    dpg_error **err);

/* DPG_OK transfers output ownership to caller; DPG_END is permanent. */
dpg_status dpg_try_next_batch_v1(
    dpg_handle *handle,
    struct ArrowArray *out_array,
    struct ArrowSchema *out_schema,
    dpg_error **err);

dpg_status dpg_wait_v1(
    dpg_handle *handle,
    uint32_t timeout_ms,
    uint64_t *event_bits,
    dpg_error **err);

dpg_status dpg_metrics_snapshot_v1(
    dpg_handle *handle,
    dpg_metrics_v1 *out,
    dpg_error **err);

void dpg_cancel_v1(dpg_handle *handle);
void dpg_close_v1(dpg_handle **handle);

const char *dpg_error_message_v1(const dpg_error *err);
void dpg_error_free_v1(dpg_error **err);

#ifdef __cplusplus
}
#endif

#endif
