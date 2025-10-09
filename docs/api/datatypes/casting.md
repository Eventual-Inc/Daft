# Casting

<style>
.md-typeset table th:first-child {
    vertical-align: middle;
    text-align: center !important;
}

.md-typeset table td:first-child {
    font-weight: bold;
    white-space: nowrap;
    text-align: right !important;
}

.md-typeset table th:not(:first-child) {
    writing-mode: vertical-rl;
    transform: rotate(180deg);
    min-width: 0 !important;
}

.md-typeset table td:not(:first-child) {
    text-align: center !important;
}


.md-typeset table td,
.md-typeset table th {
    border-left: 1px solid var(--md-typeset-table-color);
    border-right: 1px solid var(--md-typeset-table-color);
    padding: 2px !important;
}

.md-typeset table td:not(:first-child).highlight-column,
.md-typeset table th:not(:first-child).highlight-column {
    background-color: rgba(0, 0, 0, 0.1);
}

.md-typeset table tbody tr:hover {
    background-color: rgba(0, 0, 0, 0.1) !important;
}
</style>

<script>
function initTableColumnHighlight() {
  document.querySelectorAll('table td, table th').forEach(cell => {
    cell.addEventListener('mouseenter', function() {
      const index = Array.from(this.parentElement.children).indexOf(this);
      this.closest('table').querySelectorAll('tr').forEach(row => {
        row.children[index]?.classList.add('highlight-column');
      });
    });

    cell.addEventListener('mouseleave', function() {
      this.closest('table').querySelectorAll('.highlight-column')
        .forEach(el => el.classList.remove('highlight-column'));
    });
  });
}

// Initial load
document.addEventListener('DOMContentLoaded', initTableColumnHighlight);

// MkDocs Material instant navigation
if (typeof document$ !== 'undefined') {
  document$.subscribe(function() {
    initTableColumnHighlight();
  });
}
</script>

## Casting Matrix

This matrix is a high-level overview of the compatible casts between Daft DataTypes.

Casting can be done via the [`cast`][daft.functions.cast] function. It is also performed implicitly in certain operations, such as to match a returned value to the specified `return_dtype` in a UDF, or when calling [`Series.from_pylist`][daft.series.Series.from_pylist] with a specified `dtype`.

🟢 = can cast

🔴 = cannot cast

| FROM ↓ \ TO →          | Null | Boolean | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 | Decimal128 | Timestamp | Date | Time | Duration | Interval | Binary | FixedSizeBinary | Utf8 | List | FixedSizeList | Struct | Map | Embedding | Image | FixedShapeImage | Tensor | FixedShapeTensor | SparseTensor | FixedShapeSparseTensor | Python | File |
| ---------------------- | ---- | ------- | ---- | ----- | ----- | ----- | ----- | ------ | -------| ------ | ------- | ------- | ---------- | --------- | ---- | ---- | -------- | -------- | ------ | --------------- | ---- | ---- | ------------- | ------ | --- | --------- | ----- | --------------- | ------ | ---------------- | ------------ | ---------------------- | ------ | ---- |
| Null                   |      | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🟢       | 🟢      | 🟢              | 🟢   | 🟢    | 🟢            | 🟢     | 🟢   | 🟢        | 🟢    | 🟢               | 🟢     | 🟢               | 🟢            | 🟢                     | 🟢     | 🟢    |
| Boolean                | 🟢   |          | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Int8                   | 🟢   | 🟢       |      | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Int16                  | 🟢   | 🟢       | 🟢   |       | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Int32                  | 🟢   | 🟢       | 🟢   | 🟢    |        | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Int64                  | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     |       | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| UInt8                  | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    |       | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| UInt16                 | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    |         | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| UInt32                 | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      |        | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| UInt64                 | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     |        | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Float32                | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     |          | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Float64                | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       |         | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              | 🟢   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Decimal128             | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      |             | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Timestamp              | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          |           | 🟢   | 🟢   | 🔴        | 🔴       | 🔴      | 🔴              | 🟢   | 🔴    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Date                   | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          | 🟢        |      | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🟢   | 🔴    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Time                   | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          | 🔴        | 🔴   |      | 🔴        | 🔴       | 🔴      | 🔴              | 🟢   | 🔴    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Duration               | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          | 🔴        | 🔴   | 🔴   |           | 🔴       | 🔴      | 🔴              | 🔴   | 🔴    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Interval               | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        |          | 🔴      | 🔴              | 🔴   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🔴     | 🔴    |
| Binary                 | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       |         | 🟢              | 🟢   | 🔴    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| FixedSizeBinary        | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🟢      |                 | 🔴   | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Utf8                   | 🟢   | 🔴       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🔴          | 🟢        | 🟢   | 🟢   | 🟢        | 🔴       | 🟢      | 🔴              |      | 🟢    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| List                   | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   |       | 🟢            | 🔴     | 🟢   | 🟢        | 🔴    | 🔴               | 🔴     | 🟢               | 🔴            | 🔴                     | 🟢     | 🔴    |
| FixedSizeList          | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    |               | 🔴     | 🔴   | 🟢        | 🔴    | 🟢               | 🔴     | 🟢               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Struct                 | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    | 🟢            |        | 🔴   | 🟢        | 🟢    | 🔴               | 🟢     | 🔴               | 🟢            | 🟢                     | 🟢     | 🔴    |
| Map                    | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🔴    | 🔴            | 🔴     |      | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Embedding              | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    | 🟢            | 🔴     | 🔴   |           | 🔴    | 🟢               | 🟢     | 🟢               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Image                  | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🔴    | 🔴            | 🟢     | 🔴   | 🔴        |       | 🟢               | 🟢     | 🟢               | 🔴            | 🔴                     | 🟢     | 🔴    |
| FixedShapeImage        | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    | 🟢            | 🔴     | 🔴   | 🟢        | 🟢    |                  | 🟢     | 🟢               | 🔴            | 🔴                     | 🟢     | 🔴    |
| Tensor                 | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    | 🟢            | 🟢     | 🔴   | 🟢        | 🟢    | 🟢               |        | 🟢               | 🟢            | 🔴                     | 🟢     | 🔴    |
| FixedShapeTensor       | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🟢    | 🟢            | 🔴     | 🔴   | 🟢        | 🔴    | 🟢               | 🟢     |                  | 🔴            | 🟢                     | 🟢     | 🔴    |
| SparseTensor           | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🔴    | 🔴            | 🟢     | 🔴   | 🔴        | 🔴    | 🔴               | 🟢     | 🔴               |               | 🟢                     | 🟢     | 🔴    |
| FixedShapeSparseTensor | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🔴    | 🔴            | 🟢     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🟢               | 🟢            |                        | 🟢     | 🔴    |
| Python                 | 🟢   | 🟢       | 🟢   | 🟢    | 🟢     | 🟢    | 🟢    | 🟢      | 🟢     | 🟢     | 🟢       | 🟢      | 🟢          | 🟢        | 🟢   | 🟢   | 🟢        | 🟢       | 🟢      | 🟢              | 🟢   | 🟢    | 🟢            | 🟢     | 🟢   | 🟢        | 🟢    | 🟢               | 🟢     | 🟢               | 🟢            | 🟢                     |        | 🟢    |
| File                   | 🟢   | 🔴       | 🔴   | 🔴    | 🔴     | 🔴    | 🔴    | 🔴      | 🔴     | 🔴     | 🔴       | 🔴      | 🔴          | 🔴        | 🔴   | 🔴   | 🔴        | 🔴       | 🔴      | 🔴              | 🔴   | 🔴    | 🔴            | 🔴     | 🔴   | 🔴        | 🔴    | 🔴               | 🔴     | 🔴               | 🔴            | 🔴                     | 🔴     |       |

!!! note
    Overflowing values will be wrapped, e.g. 256 will be cast to 0 for an unsigned 8-bit integer.
