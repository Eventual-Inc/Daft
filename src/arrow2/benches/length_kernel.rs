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
use arrow2::compute::length::length;

fn bench_length(array: &Utf8Array<i32>) {
    criterion::black_box(length(array).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    fn double_vec<T: Clone>(v: Vec<T>) -> Vec<T> {
        [&v[..], &v[..]].concat()
    }

    // double ["hello", " ", "world", "!"] 10 times
    let mut values = vec!["one", "on", "o", ""];
    for _ in 0..10 {
        values = double_vec(values);
    }
    let array = Utf8Array::<i32>::from_slice(&values);

    c.bench_function("length", |b| b.iter(|| bench_length(&array)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
