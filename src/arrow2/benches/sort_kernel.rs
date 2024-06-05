// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::compute::sort::{lexsort, sort, sort_to_indices, SortColumn, SortOptions};
use arrow2::util::bench_util::*;

fn bench_lexsort(arr_a: &dyn Array, array_b: &dyn Array) {
    let columns = vec![
        SortColumn {
            values: arr_a,
            options: None,
        },
        SortColumn {
            values: array_b,
            options: None,
        },
    ];

    criterion::black_box(lexsort::<u32>(&columns, None).unwrap());
}

fn bench_sort(arr_a: &dyn Array) {
    sort(criterion::black_box(arr_a), &SortOptions::default(), None).unwrap();
}

fn bench_sort_limit(arr_a: &dyn Array) {
    let _: PrimitiveArray<u32> = sort_to_indices(
        criterion::black_box(arr_a),
        &SortOptions::default(),
        Some(100),
    )
    .unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let arr_a = create_primitive_array::<f32>(size, 0.0);

        c.bench_function(&format!("sort 2^{log2_size} f32"), |b| {
            b.iter(|| bench_sort(&arr_a))
        });

        c.bench_function(&format!("sort-limit 2^{log2_size} f32"), |b| {
            b.iter(|| bench_sort_limit(&arr_a))
        });

        let arr_b = create_primitive_array_with_seed::<f32>(size, 0.0, 43);
        c.bench_function(&format!("lexsort 2^{log2_size} f32"), |b| {
            b.iter(|| bench_lexsort(&arr_a, &arr_b))
        });

        let arr_a = create_primitive_array::<f32>(size, 0.5);

        c.bench_function(&format!("sort null 2^{log2_size} f32"), |b| {
            b.iter(|| bench_sort(&arr_a))
        });

        let arr_b = create_primitive_array_with_seed::<f32>(size, 0.5, 43);
        c.bench_function(&format!("lexsort null 2^{log2_size} f32"), |b| {
            b.iter(|| bench_lexsort(&arr_a, &arr_b))
        });

        let arr_a = create_string_array::<i32>(size, 4, 0.1, 42);
        c.bench_function(&format!("sort utf8 null 2^{log2_size}"), |b| {
            b.iter(|| bench_sort(&arr_a))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
