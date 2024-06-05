# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
import decimal

import pyarrow
import arrow_pyarrow_integration_testing


class UuidType(pyarrow.PyExtensionType):
    def __init__(self):
        super().__init__(pyarrow.binary(16))

    def __reduce__(self):
        return UuidType, ()


class TestCase(unittest.TestCase):
    def setUp(self):
        self.old_allocated_cpp = pyarrow.total_allocated_bytes()

    def tearDown(self):
        # No leak of C++ memory
        self.assertEqual(self.old_allocated_cpp, pyarrow.total_allocated_bytes())

    def test_null(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array([None], type=pyarrow.null())
        b = arrow_pyarrow_integration_testing.round_trip_array(a)
        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_primitive(self):
        a = pyarrow.array([0, None, 2, 3, 4])
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_primitive_sliced(self):
        a = pyarrow.array([0, None, 2, 3, 4]).slice(1, 2)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_boolean(self):
        a = pyarrow.array([True, None, False, True, False])
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_boolean_sliced(self):
        a = pyarrow.array([True, None, False, True, False]).slice(1, 2)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_string(self):
        a = pyarrow.array(["a", None, "ccc"])
        b = arrow_pyarrow_integration_testing.round_trip_array(a)
        c = pyarrow.array(["a", None, "ccc"])
        self.assertEqual(b, c)

    def test_string_sliced(self):
        a = pyarrow.array(["a", None, "ccc"]).slice(1, 2)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_fixed_binary(self):
        a = pyarrow.array([b"aa", None, b"cc"], pyarrow.binary(2))
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_decimal_roundtrip(self):
        """
        Python -> Rust -> Python
        """
        data = [
            round(decimal.Decimal(722.82), 2),
            round(decimal.Decimal(-934.11), 2),
            None,
        ]
        a = pyarrow.array(data, pyarrow.decimal128(5, 2))
        b = arrow_pyarrow_integration_testing.round_trip_array(a)
        self.assertEqual(a, b)

    def test_decimal256_roundtrip(self):
        """
        Python -> Rust -> Python
        """
        data = [
            round(decimal.Decimal(722.82), 2),
            round(decimal.Decimal(-934.11), 2),
            None,
        ]
        a = pyarrow.array(data, pyarrow.decimal256(5, 2))
        b = arrow_pyarrow_integration_testing.round_trip_array(a)
        self.assertEqual(a, b)

    def test_list_array(self):
        """
        Python -> Rust -> Python
        """
        for _ in range(2):
            a = pyarrow.array(
                [[], None, [1, 2], [4, 5, 6]], pyarrow.list_(pyarrow.int64())
            )
            b = arrow_pyarrow_integration_testing.round_trip_array(a)

            b.validate(full=True)
            assert a.to_pylist() == b.to_pylist()
            assert a.type == b.type

    def test_list_sliced(self):
        a = pyarrow.array(
            [[], None, [1, 2], [4, 5, 6]], pyarrow.list_(pyarrow.int64())
        ).slice(1, 2)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_struct(self):
        fields = [
            ("f1", pyarrow.int32()),
            ("f2", pyarrow.string()),
        ]
        a = pyarrow.array(
            [
                {"f1": 1, "f2": "a"},
                None,
                {"f1": 3, "f2": None},
                {"f1": None, "f2": "d"},
                {"f1": None, "f2": None},
            ],
            pyarrow.struct(fields),
        )
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    # see https://issues.apache.org/jira/browse/ARROW-14383
    def _test_struct_sliced(self):
        fields = [
            ("f1", pyarrow.int32()),
            ("f2", pyarrow.string()),
        ]
        a = pyarrow.array(
            [
                {"f1": 1, "f2": "a"},
                None,
                {"f1": 3, "f2": None},
                {"f1": None, "f2": "d"},
                {"f1": None, "f2": None},
            ],
            pyarrow.struct(fields),
        ).slice(1, 2)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)
        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_list_list_array(self):
        """
        Python -> Rust -> Python
        """
        for _ in range(2):
            a = pyarrow.array(
                [[None], None, [[1], [2]], [[4, 5], [6]]],
                pyarrow.list_(pyarrow.list_(pyarrow.int64())),
            )
            b = arrow_pyarrow_integration_testing.round_trip_array(a)

            b.validate(full=True)
            assert a.to_pylist() == b.to_pylist()
            assert a.type == b.type

    def test_fixed_list(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array(
            [None, [1, 2], [4, 5]],
            pyarrow.list_(pyarrow.int64(), 2),
        )
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    # same as https://issues.apache.org/jira/browse/ARROW-14383
    def _test_fixed_list_sliced(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array(
            [None, [1, 2], [4, 5]],
            pyarrow.list_(pyarrow.int64(), 2),
        ).slice(1, 2)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_dict(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.array(
            ["a", "a", "b", None, "c"],
            pyarrow.dictionary(pyarrow.int64(), pyarrow.utf8()),
        )
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_map(self):
        """
        Python -> Rust -> Python
        """
        offsets = [0, None, 2, 6]
        pykeys = [b"a", b"b", b"c", b"d", b"e", b"f"]
        pyitems = [1, 2, 3, None, 4, 5]
        keys = pyarrow.array(pykeys, type="binary")
        items = pyarrow.array(pyitems, type="i4")

        a = pyarrow.MapArray.from_arrays(offsets, keys, items)
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_sparse_union(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.UnionArray.from_sparse(
            pyarrow.array([0, 1, 1, 0, 1], pyarrow.int8()),
            [
                pyarrow.array(["a", "", "", "", "c"], pyarrow.utf8()),
                pyarrow.array([0, 1, 2, None, 0], pyarrow.int64()),
            ],
        )
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_dense_union(self):
        """
        Python -> Rust -> Python
        """
        a = pyarrow.UnionArray.from_dense(
            pyarrow.array([0, 1, 1, 0, 1], pyarrow.int8()),
            pyarrow.array([0, 1, 2, 3, 4], type=pyarrow.int32()),
            [
                pyarrow.array(["a", "", "", "", "c"], pyarrow.utf8()),
                pyarrow.array([0, 1, 2, None, 0], pyarrow.int64()),
            ],
        )
        b = arrow_pyarrow_integration_testing.round_trip_array(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type

    def test_field(self):
        field = pyarrow.field("aa", pyarrow.bool_())
        result = arrow_pyarrow_integration_testing.round_trip_field(field)
        assert field == result

    def test_field_nested(self):
        field = pyarrow.field("aa", pyarrow.list_(pyarrow.field("ab", pyarrow.bool_())))
        result = arrow_pyarrow_integration_testing.round_trip_field(field)
        assert field == result

    def test_field_metadata(self):
        field = pyarrow.field("aa", pyarrow.bool_(), {"a": "b"})
        result = arrow_pyarrow_integration_testing.round_trip_field(field)
        assert field == result
        assert field.metadata == result.metadata

    # see https://issues.apache.org/jira/browse/ARROW-13855
    def _test_field_extension(self):
        field = pyarrow.field("aa", UuidType())
        result = arrow_pyarrow_integration_testing.round_trip_field(field)
        assert field == result
        assert field.metadata == result.metadata
