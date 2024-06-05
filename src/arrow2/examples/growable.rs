use arrow2::array::growable::{Growable, GrowablePrimitive};
use arrow2::array::PrimitiveArray;

fn main() {
    // say we have two sorted arrays
    let array0 = PrimitiveArray::<i64>::from_vec(vec![1, 2, 5]);
    let array1 = PrimitiveArray::<i64>::from_vec(vec![3, 4, 6]);

    // and we found a way to compute the slices that sort them:
    // (array_index, start of the slice, length of the slice)
    let slices = &[
        // [1, 2] from array0
        (0, 0, 2),
        // [3, 4] from array1
        (1, 0, 2),
        // [5] from array0
        (0, 2, 1),
        // [6] from array1
        (1, 2, 1),
    ];

    // we can build a new array out of these slices as follows:
    // first, declare the growable out of the arrays. Since we are not extending with nulls,
    // we use `false`. We also pre-allocate, as we know that we will need all entries.
    let capacity = array0.len() + array1.len();
    let use_validity = false;
    let mut growable = GrowablePrimitive::new(vec![&array0, &array1], use_validity, capacity);

    // next, extend the growable:
    for slice in slices {
        growable.extend(slice.0, slice.1, slice.2);
    }
    // finally, convert it to the array (this is `O(1)`)
    let result: PrimitiveArray<i64> = growable.into();

    let expected = PrimitiveArray::<i64>::from_vec(vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(result, expected);
}
