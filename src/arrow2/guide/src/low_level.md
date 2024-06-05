# Low-level API

The starting point of this crate is the idea that data is stored in memory in a specific arrangement to be interoperable with Arrow's ecosystem.

The most important design aspect of this crate is that contiguous regions are shared via an
`Arc`. In this context, the operation of slicing a memory region is `O(1)` because it
corresponds to changing an offset and length. The tradeoff is that once under
an `Arc`, memory regions are immutable. See note below on how to overcome this.

The second most important aspect is that Arrow has two main types of data buffers: bitmaps,
whose offsets are measured in bits, and byte types (such as `i32`), whose offsets are
measured in bytes. With this in mind, this crate has 2 main types of containers of
contiguous memory regions:

* `Buffer<T>`: handle contiguous memory regions of type T whose offsets are measured in items
* `Bitmap`: handle contiguous memory regions of bits whose offsets are measured in bits

These hold _all_ data-related memory in this crate.

Due to their intrinsic immutability, each container has a corresponding mutable
(and non-shareable) variant:

* `Vec<T>`
* `MutableBitmap`

Let's see how these structures are used.

Create a new `Buffer<u32>`:

```rust
# use arrow2::buffer::Buffer;
# fn main() {
let x = vec![1u32, 2, 3];
let x: Buffer<u32> = x.into();
assert_eq!(x.as_slice(), &[1u32, 2, 3]);

let x = x.sliced(1, 2); // O(1)
assert_eq!(x.as_slice(), &[2, 3]);
# }
```

Contrarily to `Vec`, `Buffer` (and all structs in this crate) only supports
the following physical types:

* `i8-i128`
* `u8-u64`
* `f32` and `f64`
* `arrow2::types::days_ms`
* `arrow2::types::months_days_ns`

This is because the arrow specification only supports the above Rust types; all other complex
types supported by arrow are built on top of these types, which enables Arrow to be a highly
interoperable in-memory format.

## Bitmaps

Arrow's in-memory arrangement of boolean values is different from `Vec<bool>`. Specifically,
arrow uses individual bits to represent a boolean, as opposed to the usual byte
that `bool` holds.
Besides the 8x compression, this makes the validity particularly useful for 
[AVX512](https://en.wikipedia.org/wiki/AVX-512) masks.
One tradeoff is that an arrows' bitmap is not represented as a Rust slice, as Rust slices use
pointer arithmetics, whose smallest unit is a byte.

Arrow2 has two containers for bitmaps: `Bitmap` (immutable and sharable)
and `MutableBitmap` (mutable):

```rust
use arrow2::bitmap::Bitmap;
# fn main() {
let x = Bitmap::from(&[true, false]);
let iter = x.iter().map(|x| !x);
let y = Bitmap::from_trusted_len_iter(iter);
assert_eq!(y.get_bit(0), false);
assert_eq!(y.get_bit(1), true);
# }
```

```rust
use arrow2::bitmap::MutableBitmap;
# fn main() {
let mut x = MutableBitmap::new();
x.push(true);
x.push(false);
assert_eq!(x.get(1), false);
x.set(1, true);
assert_eq!(x.get(1), true);
# }
```

## Copy on write (COW) semantics

Both `Buffer` and `Bitmap` support copy on write semantics via `into_mut`, that may convert
them to a `Vec` or `MutableBitmap` respectively.

This allows re-using them to e.g. perform multiple operations without allocations.
