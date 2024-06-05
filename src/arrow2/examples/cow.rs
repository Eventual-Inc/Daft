// This example demos how to operate on arrays in-place.
use arrow2::array::{Array, PrimitiveArray};
use arrow2::compute::arity_assign;

fn main() {
    // say we have have received an `Array`
    let mut array: Box<dyn Array> = PrimitiveArray::from_vec(vec![1i32, 2]).boxed();

    // we can apply a transformation to its values without allocating a new array as follows:

    // 1. downcast it to the correct type (known via `array.data_type().to_physical_type()`)
    let array = array
        .as_any_mut()
        .downcast_mut::<PrimitiveArray<i32>>()
        .unwrap();

    // 2. call `unary` with the function to apply to each value
    arity_assign::unary(array, |x| x * 10);

    // confirm that it gives the right result :)
    assert_eq!(array, &PrimitiveArray::from_vec(vec![10i32, 20]));

    // alternatively, you can use `get_mut_values`. Unwrap works because it is single owned
    let values = array.get_mut_values().unwrap();
    values[0] = 0;

    assert_eq!(array, &PrimitiveArray::from_vec(vec![0, 20]));

    // you can also modify the validity:
    array.set_validity(Some([true, false].into()));
    array.apply_validity(|bitmap| {
        let mut mut_bitmap = bitmap.into_mut().right().unwrap();
        mut_bitmap.set(1, true);
        mut_bitmap.into()
    });

    assert_eq!(array, &PrimitiveArray::from_vec(vec![0, 20]));
}
