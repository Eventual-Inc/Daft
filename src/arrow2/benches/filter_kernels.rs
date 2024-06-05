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
use arrow2::chunk::Chunk;
use arrow2::compute::filter::{build_filter, filter, filter_chunk, Filter};
use arrow2::datatypes::DataType;
use arrow2::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

fn bench_filter(data_array: &dyn Array, filter_array: &BooleanArray) {
    criterion::black_box(filter(data_array, filter_array).unwrap());
}

fn bench_built_filter(filter: &Filter, array: &dyn Array) {
    criterion::black_box(filter(array));
}

fn add_benchmark(c: &mut Criterion) {
    // scaling benchmarks
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let filter_array = create_boolean_array(size, 0.0, 0.9);
        let filter_array =
            BooleanArray::new(DataType::Boolean, filter_array.values().clone(), None);

        let arr_a = create_primitive_array::<f32>(size, 0.0);
        c.bench_function(&format!("filter 2^{log2_size} f32"), |b| {
            b.iter(|| bench_filter(&arr_a, &filter_array))
        });

        let arr_a = create_primitive_array::<f32>(size, 0.1);

        c.bench_function(&format!("filter null 2^{log2_size} f32"), |b| {
            b.iter(|| bench_filter(&arr_a, &filter_array))
        });
    });

    let size = 65536;
    let filter_array = create_boolean_array(size, 0.0, 0.5);
    let dense_filter_array = create_boolean_array(size, 0.0, 1.0 - 1.0 / 1024.0);
    let sparse_filter_array = create_boolean_array(size, 0.0, 1.0 / 1024.0);

    let filter = build_filter(&filter_array).unwrap();
    let dense_filter = build_filter(&dense_filter_array).unwrap();
    let sparse_filter = build_filter(&sparse_filter_array).unwrap();

    let data_array = create_primitive_array::<u8>(size, 0.0);

    c.bench_function("filter u8", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter u8 low selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context u8", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<u8>(size, 0.5);
    c.bench_function("filter context u8 w NULLs", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 w NULLs high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 w NULLs low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<f32>(size, 0.5);
    let data_array_nonull = create_primitive_array::<f32>(size, 0.0);
    c.bench_function("filter f32", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter f32 high selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });

    c.bench_function("filter f32 nonull", |b| {
        b.iter(|| bench_filter(&data_array_nonull, &filter_array))
    });
    c.bench_function("filter f32 nonull high selectivity", |b| {
        b.iter(|| bench_filter(&data_array_nonull, &dense_filter_array))
    });

    c.bench_function("filter context f32", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context f32 high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context f32 low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_array::<i32>(size, 4, 0.5, 42);
    c.bench_function("filter context string", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context string high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context string low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<f32>(size, 0.0);

    let columns = Chunk::try_new(vec![&data_array as &dyn Array]).unwrap();

    c.bench_function("filter single chunk", |b| {
        b.iter(|| filter_chunk(&columns, &filter_array))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
