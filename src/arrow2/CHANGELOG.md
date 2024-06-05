# Changelog

## [v0.17.0](https://github.com/jorgecarleitao/arrow2/tree/v0.17.0) (2023-03-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.16.0...v0.17.0)

**Breaking changes:**

- Changed async ipc writer to accept schema by value [\#1439](https://github.com/jorgecarleitao/arrow2/pull/1439) ([ritchie46](https://github.com/ritchie46))
- Made `len/len_proxy` consistent with `Offsets` [\#1434](https://github.com/jorgecarleitao/arrow2/pull/1434) ([ritchie46](https://github.com/ritchie46))
- Changed methods to slice arrays [\#1396](https://github.com/jorgecarleitao/arrow2/pull/1396) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added buffer interoperability with arrow-rs [\#1437](https://github.com/jorgecarleitao/arrow2/pull/1437) ([tustvold](https://github.com/tustvold))
- Added MapScalar [\#1428](https://github.com/jorgecarleitao/arrow2/pull/1428) ([b41sh](https://github.com/b41sh))
- Added support for JSON serialization of dictionary [\#1424](https://github.com/jorgecarleitao/arrow2/pull/1424) ([ritchie46](https://github.com/ritchie46))
- Added support for MapArray read and write to parquet [\#1419](https://github.com/jorgecarleitao/arrow2/pull/1419) ([b41sh](https://github.com/b41sh))

**Fixed bugs:**

- `parquet_read` panics when working with `date64`s [\#1400](https://github.com/jorgecarleitao/arrow2/issues/1400)
- Round Trip \[Rust -\> arrow2\_convert -\> Arrow -\> Parquet -\> Arrow -\> Rust\] [\#1376](https://github.com/jorgecarleitao/arrow2/issues/1376)
- Parquet writes incorrect `List<u32>` [\#1368](https://github.com/jorgecarleitao/arrow2/issues/1368)
- Slicing nullable list arrays into multiple parquet pages doesn't work [\#1356](https://github.com/jorgecarleitao/arrow2/issues/1356)
- Reading parquet file with multiple row groups and nested nullable struct types panics  [\#1249](https://github.com/jorgecarleitao/arrow2/issues/1249)
- Changed encoded float::Inf as null in json [\#1427](https://github.com/jorgecarleitao/arrow2/pull/1427) ([SimonSchneider](https://github.com/SimonSchneider))
- Fixed statistics writing flag and correct null\_count in dictionaries [\#1414](https://github.com/jorgecarleitao/arrow2/pull/1414) ([ritchie46](https://github.com/ritchie46))
- Fixed ahash dependency for wasm [\#1407](https://github.com/jorgecarleitao/arrow2/pull/1407) ([hzuo](https://github.com/hzuo))
- Fixed writing of sliced arrays to parquet [\#1397](https://github.com/jorgecarleitao/arrow2/pull/1397) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed writing nested parquet [\#1390](https://github.com/jorgecarleitao/arrow2/pull/1390) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added interoperability with arrow-schema [\#1442](https://github.com/jorgecarleitao/arrow2/pull/1442) ([tustvold](https://github.com/tustvold))
- Updated dependencies [\#1441](https://github.com/jorgecarleitao/arrow2/pull/1441) ([ritchie46](https://github.com/ritchie46))
- Updated multiversion and support wider registers [\#1440](https://github.com/jorgecarleitao/arrow2/pull/1440) ([ritchie46](https://github.com/ritchie46))
- Added impl\_mutable\_array\_mut\_validity macro for mutable arrays [\#1435](https://github.com/jorgecarleitao/arrow2/pull/1435) ([Arty-Maly](https://github.com/Arty-Maly))
- Re-exported the `bloom_filter` module from `parquet2` crate [\#1420](https://github.com/jorgecarleitao/arrow2/pull/1420) ([ozgrakkurt](https://github.com/ozgrakkurt))
- Updated base64 to 0.21 [\#1408](https://github.com/jorgecarleitao/arrow2/pull/1408) ([WindSoilder](https://github.com/WindSoilder))
- Added apply\_validity and set\_validity to mutable utf8 array [\#1406](https://github.com/jorgecarleitao/arrow2/pull/1406) ([Arty-Maly](https://github.com/Arty-Maly))
- Added cast for FixedSizeBinary to \(Large\)Binary [\#1403](https://github.com/jorgecarleitao/arrow2/pull/1403) ([ritchie46](https://github.com/ritchie46))
- Improved support for date64 written by pyarrow to parquet [\#1402](https://github.com/jorgecarleitao/arrow2/pull/1402) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code [\#1401](https://github.com/jorgecarleitao/arrow2/pull/1401) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved API of getting mutable from Buffer [\#1399](https://github.com/jorgecarleitao/arrow2/pull/1399) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code via DRY [\#1398](https://github.com/jorgecarleitao/arrow2/pull/1398) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `set_len` method to Buffer [\#1374](https://github.com/jorgecarleitao/arrow2/pull/1374) ([haixuanTao](https://github.com/haixuanTao))

**Documentation updates:**

- Fixed broken guide link [\#1395](https://github.com/jorgecarleitao/arrow2/pull/1395) ([kjschiroo](https://github.com/kjschiroo))

## [v0.16.0](https://github.com/jorgecarleitao/arrow2/tree/v0.16.0) (2023-02-09)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.15.0...v0.16.0)

**Breaking changes:**

- Made IPC writer take owned schema [\#1361](https://github.com/jorgecarleitao/arrow2/pull/1361) ([ritchie46](https://github.com/ritchie46))
- Correctly update child-offsets in `GrowableUnion` [\#1360](https://github.com/jorgecarleitao/arrow2/pull/1360) ([jleibs](https://github.com/jleibs))

**Fixed bugs:**

- invalid written parquet file of nested structures. \(Mixing list with structs\) [\#1325](https://github.com/jorgecarleitao/arrow2/issues/1325)
- Fix incorrect downcast in `estimated_size_bytes` [\#1351](https://github.com/jorgecarleitao/arrow2/pull/1351) ([jleibs](https://github.com/jleibs))
- fix\(parquet\): nested struct /list writing [\#1347](https://github.com/jorgecarleitao/arrow2/pull/1347) ([ritchie46](https://github.com/ritchie46))
- Fixed csv infer\_schema on empty fields [\#1342](https://github.com/jorgecarleitao/arrow2/pull/1342) ([tripokey](https://github.com/tripokey))

**Enhancements:**

- Added support for `take` of `FixedSizeListArray` [\#1386](https://github.com/jorgecarleitao/arrow2/pull/1386) ([kylebarron](https://github.com/kylebarron))
- Renamed `factory` argument on parquet read functions to `reader_factory` [\#1380](https://github.com/jorgecarleitao/arrow2/pull/1380) ([ozgrakkurt](https://github.com/ozgrakkurt))
- Made some structs and functions public [\#1375](https://github.com/jorgecarleitao/arrow2/pull/1375) ([b41sh](https://github.com/b41sh))
- Added `Utf8Array::apply_validity` [\#1367](https://github.com/jorgecarleitao/arrow2/pull/1367) ([Arty-Maly](https://github.com/Arty-Maly))
- Added set/get scratches [\#1363](https://github.com/jorgecarleitao/arrow2/pull/1363) ([ritchie46](https://github.com/ritchie46))
- Amortized intermediate allocations in IPC writer [\#1362](https://github.com/jorgecarleitao/arrow2/pull/1362) ([ritchie46](https://github.com/ritchie46))
- Improved clippy [\#1353](https://github.com/jorgecarleitao/arrow2/pull/1353) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Fixed typo in `OffsetsBuffer` docs [\#1373](https://github.com/jorgecarleitao/arrow2/pull/1373) ([DzenanJupic](https://github.com/DzenanJupic))
- Update README.md to fix capitalization and spelling [\#1338](https://github.com/jorgecarleitao/arrow2/pull/1338) ([yerke](https://github.com/yerke))

**Testing updates:**

- add toolchain.toml [\#1349](https://github.com/jorgecarleitao/arrow2/pull/1349) ([ritchie46](https://github.com/ritchie46))

## [v0.15.0](https://github.com/jorgecarleitao/arrow2/tree/v0.15.0) (2022-12-18)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.14.2...v0.15.0)

**Breaking changes:**

- Added values' capacity to `MutableBinaryArray::reserve` [\#1277](https://github.com/jorgecarleitao/arrow2/issues/1277)
- Removed `from_data` from all arrays [\#1328](https://github.com/jorgecarleitao/arrow2/pull/1328) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `Offsets` and `OffsetsBuffer` [\#1316](https://github.com/jorgecarleitao/arrow2/pull/1316) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped parquet2 dependency [\#1304](https://github.com/jorgecarleitao/arrow2/pull/1304) ([ritchie46](https://github.com/ritchie46))
- Added data\_pagesize\_limit to write parquet pages [\#1303](https://github.com/jorgecarleitao/arrow2/pull/1303) ([sundy-li](https://github.com/sundy-li))
- Bumped arrow-format to 0.8 [\#1298](https://github.com/jorgecarleitao/arrow2/pull/1298) ([Xuanwo](https://github.com/Xuanwo))
- Improved iterators [\#1270](https://github.com/jorgecarleitao/arrow2/pull/1270) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added `TryExtendFromSelf` [\#1278](https://github.com/jorgecarleitao/arrow2/pull/1278) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for JSON ser/de records layout [\#1275](https://github.com/jorgecarleitao/arrow2/pull/1275) ([AnIrishDuck](https://github.com/AnIrishDuck))

**Fixed bugs:**

- Parquet writes all values of sliced arrays?  [\#1323](https://github.com/jorgecarleitao/arrow2/issues/1323)
- Avro schema: Invalid record names [\#1269](https://github.com/jorgecarleitao/arrow2/issues/1269)
- Fixed writing nested/sliced arrays to parquet [\#1326](https://github.com/jorgecarleitao/arrow2/pull/1326) ([ritchie46](https://github.com/ritchie46))
- Fixed failing to accept dictionary full of nulls [\#1312](https://github.com/jorgecarleitao/arrow2/pull/1312) ([ritchie46](https://github.com/ritchie46))
- Added support for Extension types in ffi [\#1300](https://github.com/jorgecarleitao/arrow2/pull/1300) ([jondo2010](https://github.com/jondo2010))
- Fixed error in memory usage of sliced binary/list/utf8arrays [\#1293](https://github.com/jorgecarleitao/arrow2/pull/1293) ([ritchie46](https://github.com/ritchie46))
- Fixed descending ordering when specify nulls first [\#1286](https://github.com/jorgecarleitao/arrow2/pull/1286) ([sandflee](https://github.com/sandflee))
- Added avro record names when converting arrow schema to avro [\#1279](https://github.com/jorgecarleitao/arrow2/pull/1279) ([Samrose-Ahmed](https://github.com/Samrose-Ahmed))

**Enhancements:**

- Fixed clippy [\#1336](https://github.com/jorgecarleitao/arrow2/pull/1336) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `UnionArray` [\#1331](https://github.com/jorgecarleitao/arrow2/pull/1331) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped json-deserializer version [\#1321](https://github.com/jorgecarleitao/arrow2/pull/1321) ([universalmind303](https://github.com/universalmind303))
- Removed flushing during arrow IPC writing to improve performance when using a buffered writer [\#1318](https://github.com/jorgecarleitao/arrow2/pull/1318) ([cyr](https://github.com/cyr))
- Improved performance of check\_indexes [\#1313](https://github.com/jorgecarleitao/arrow2/pull/1313) ([ritchie46](https://github.com/ritchie46))
- Improved performance of checking offsets `~-64-73%` [\#1305](https://github.com/jorgecarleitao/arrow2/pull/1305) ([ritchie46](https://github.com/ritchie46))
- Added `reserve` to pushable containers in parquet extend\_from\_decoder [\#1301](https://github.com/jorgecarleitao/arrow2/pull/1301) ([ritchie46](https://github.com/ritchie46))
- Optimized slicing [\#1285](https://github.com/jorgecarleitao/arrow2/pull/1285) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved ZipValidity iterators [\#1284](https://github.com/jorgecarleitao/arrow2/pull/1284) ([ritchie46](https://github.com/ritchie46))
- Added `MutableBinaryValuesArray` [\#1276](https://github.com/jorgecarleitao/arrow2/pull/1276) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Fixed link from the API to the guide [\#1290](https://github.com/jorgecarleitao/arrow2/pull/1290) ([datapythonista](https://github.com/datapythonista))

## [v0.15.0](https://github.com/jorgecarleitao/arrow2/tree/v0.15.0) (2022-12-18)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.14.2...v0.15.0)

**Breaking changes:**

- Added values' capacity to `MutableBinaryArray::reserve` [\#1277](https://github.com/jorgecarleitao/arrow2/issues/1277)
- Removed `from_data` from all arrays [\#1328](https://github.com/jorgecarleitao/arrow2/pull/1328) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `Offsets` and `OffsetsBuffer` [\#1316](https://github.com/jorgecarleitao/arrow2/pull/1316) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped parquet2 dependency [\#1304](https://github.com/jorgecarleitao/arrow2/pull/1304) ([ritchie46](https://github.com/ritchie46))
- Added data\_pagesize\_limit to write parquet pages [\#1303](https://github.com/jorgecarleitao/arrow2/pull/1303) ([sundy-li](https://github.com/sundy-li))
- Bumped arrow-format to 0.8 [\#1298](https://github.com/jorgecarleitao/arrow2/pull/1298) ([Xuanwo](https://github.com/Xuanwo))
- Improved iterators [\#1270](https://github.com/jorgecarleitao/arrow2/pull/1270) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added `TryExtendFromSelf` [\#1278](https://github.com/jorgecarleitao/arrow2/pull/1278) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for JSON ser/de records layout [\#1275](https://github.com/jorgecarleitao/arrow2/pull/1275) ([AnIrishDuck](https://github.com/AnIrishDuck))

**Fixed bugs:**

- Parquet writes all values of sliced arrays?  [\#1323](https://github.com/jorgecarleitao/arrow2/issues/1323)
- Avro schema: Invalid record names [\#1269](https://github.com/jorgecarleitao/arrow2/issues/1269)
- Fixed writing nested/sliced arrays to parquet [\#1326](https://github.com/jorgecarleitao/arrow2/pull/1326) ([ritchie46](https://github.com/ritchie46))
- Fixed failing to accept dictionary full of nulls [\#1312](https://github.com/jorgecarleitao/arrow2/pull/1312) ([ritchie46](https://github.com/ritchie46))
- Added support for Extension types in ffi [\#1300](https://github.com/jorgecarleitao/arrow2/pull/1300) ([jondo2010](https://github.com/jondo2010))
- Fixed error in memory usage of sliced binary/list/utf8arrays [\#1293](https://github.com/jorgecarleitao/arrow2/pull/1293) ([ritchie46](https://github.com/ritchie46))
- Fixed descending ordering when specify nulls first [\#1286](https://github.com/jorgecarleitao/arrow2/pull/1286) ([sandflee](https://github.com/sandflee))
- Added avro record names when converting arrow schema to avro [\#1279](https://github.com/jorgecarleitao/arrow2/pull/1279) ([Samrose-Ahmed](https://github.com/Samrose-Ahmed))

**Enhancements:**

- Fixed clippy [\#1336](https://github.com/jorgecarleitao/arrow2/pull/1336) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `UnionArray` [\#1331](https://github.com/jorgecarleitao/arrow2/pull/1331) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped json-deserializer version [\#1321](https://github.com/jorgecarleitao/arrow2/pull/1321) ([universalmind303](https://github.com/universalmind303))
- Removed flushing during arrow IPC writing to improve performance when using a buffered writer [\#1318](https://github.com/jorgecarleitao/arrow2/pull/1318) ([cyr](https://github.com/cyr))
- Improved performance of check\_indexes [\#1313](https://github.com/jorgecarleitao/arrow2/pull/1313) ([ritchie46](https://github.com/ritchie46))
- Improved performance of checking offsets `~-64-73%` [\#1305](https://github.com/jorgecarleitao/arrow2/pull/1305) ([ritchie46](https://github.com/ritchie46))
- Added `reserve` to pushable containers in parquet extend\_from\_decoder [\#1301](https://github.com/jorgecarleitao/arrow2/pull/1301) ([ritchie46](https://github.com/ritchie46))
- Optimized slicing [\#1285](https://github.com/jorgecarleitao/arrow2/pull/1285) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved ZipValidity iterators [\#1284](https://github.com/jorgecarleitao/arrow2/pull/1284) ([ritchie46](https://github.com/ritchie46))
- Added `MutableBinaryValuesArray` [\#1276](https://github.com/jorgecarleitao/arrow2/pull/1276) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Fixed link from the API to the guide [\#1290](https://github.com/jorgecarleitao/arrow2/pull/1290) ([datapythonista](https://github.com/datapythonista))

## [v0.14.2](https://github.com/jorgecarleitao/arrow2/tree/v0.14.2) (2022-10-05)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.14.1...v0.14.2)

**New features:**

- Added MutableUtf8ValuesArray [\#1260](https://github.com/jorgecarleitao/arrow2/pull/1260) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Unnecessary println in library code [\#1263](https://github.com/jorgecarleitao/arrow2/issues/1263)

**Testing updates:**

- Added test for `MutableUtf8Array::as_box` [\#1266](https://github.com/jorgecarleitao/arrow2/pull/1266) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.14.1](https://github.com/jorgecarleitao/arrow2/tree/v0.14.1) (2022-09-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.14.0...v0.14.1)

**Fixed bugs:**

- Potentially unneeded call in Parquet repetition-level encoding [\#1254](https://github.com/jorgecarleitao/arrow2/issues/1254)
- Potential bug in reading lists from avro? [\#1252](https://github.com/jorgecarleitao/arrow2/issues/1252)
- Removed un-used code [\#1258](https://github.com/jorgecarleitao/arrow2/pull/1258) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error reading unbounded Avro list [\#1253](https://github.com/jorgecarleitao/arrow2/pull/1253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add missing call to `try_push_valid` for nested avro deserialization [\#1248](https://github.com/jorgecarleitao/arrow2/pull/1248) ([shaeqahmed](https://github.com/shaeqahmed))

**Enhancements:**

- Bump json\_deserializer version to 0.4.1 [\#1261](https://github.com/jorgecarleitao/arrow2/pull/1261) ([cjermain](https://github.com/cjermain))
- Fixed clippy for 1.60 [\#1259](https://github.com/jorgecarleitao/arrow2/pull/1259) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `BinaryArray::into_mut` and double-ended support for its iterator [\#1255](https://github.com/jorgecarleitao/arrow2/pull/1255) ([ozgrakkurt](https://github.com/ozgrakkurt))

**Testing updates:**

- Improved test for nullable struct read from Avro [\#1250](https://github.com/jorgecarleitao/arrow2/pull/1250) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.14.0](https://github.com/jorgecarleitao/arrow2/tree/v0.14.0) (2022-09-12)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.13.0...v0.14.0)

**Breaking changes:**

- Removed `Count` \(parquet statistics\) [\#1217](https://github.com/jorgecarleitao/arrow2/pull/1217) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Exposed parquet indexed page filtering to `FileReader` [\#1216](https://github.com/jorgecarleitao/arrow2/pull/1216) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simpler IPC API [\#1208](https://github.com/jorgecarleitao/arrow2/pull/1208) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated Avro code to avro-schema repo [\#1199](https://github.com/jorgecarleitao/arrow2/pull/1199) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for decimal 256 [\#1194](https://github.com/jorgecarleitao/arrow2/pull/1194) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support for decoding delta-length-encoded binary \(parquet\) [\#1228](https://github.com/jorgecarleitao/arrow2/pull/1228) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and write Parquet's delta-bitpacked \(integer encoding\) [\#1226](https://github.com/jorgecarleitao/arrow2/pull/1226) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for parquet sidecar to `FileReader` [\#1215](https://github.com/jorgecarleitao/arrow2/pull/1215) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Write 64bit aligned IPC files [\#1201](https://github.com/jorgecarleitao/arrow2/pull/1201) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to mmap IPC format [\#1197](https://github.com/jorgecarleitao/arrow2/pull/1197) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutableStructArray` [\#1196](https://github.com/jorgecarleitao/arrow2/pull/1196) ([hohav](https://github.com/hohav))

**Fixed bugs:**

- Stack overflow in parquet RowGroupReader with groups\_filter  [\#1206](https://github.com/jorgecarleitao/arrow2/issues/1206)
- fixed comparisson and validity kernels [\#1243](https://github.com/jorgecarleitao/arrow2/pull/1243) ([ritchie46](https://github.com/ritchie46))
- Fixed reading nested stats [\#1240](https://github.com/jorgecarleitao/arrow2/pull/1240) ([jorgecarleitao](https://github.com/jorgecarleitao))
- `FileSink` now closes the underlying writer. [\#1213](https://github.com/jorgecarleitao/arrow2/pull/1213) ([samkaufman](https://github.com/samkaufman))
- Fixed JSON infer order [\#1212](https://github.com/jorgecarleitao/arrow2/pull/1212) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed StackOverflow in skipping many parquet row groups [\#1210](https://github.com/jorgecarleitao/arrow2/pull/1210) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fix escaped like wildcards [\#1204](https://github.com/jorgecarleitao/arrow2/pull/1204) ([daniel-martinez-maqueda-sap](https://github.com/daniel-martinez-maqueda-sap))
- Removed println :\( [\#1203](https://github.com/jorgecarleitao/arrow2/pull/1203) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added schema to FileReader [\#1246](https://github.com/jorgecarleitao/arrow2/pull/1246) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simpler nested parquet read [\#1241](https://github.com/jorgecarleitao/arrow2/pull/1241) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed unneeded code [\#1229](https://github.com/jorgecarleitao/arrow2/pull/1229) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutableStruct::push` [\#1223](https://github.com/jorgecarleitao/arrow2/pull/1223) ([hohav](https://github.com/hohav))
- Reduced binary size [\#1221](https://github.com/jorgecarleitao/arrow2/pull/1221) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added utf8 \<\> binary cast [\#1220](https://github.com/jorgecarleitao/arrow2/pull/1220) ([jorgecarleitao](https://github.com/jorgecarleitao))
- split parquet compression backend features [\#1207](https://github.com/jorgecarleitao/arrow2/pull/1207) ([ritchie46](https://github.com/ritchie46))
- Improved API of `mmap` [\#1205](https://github.com/jorgecarleitao/arrow2/pull/1205) ([ritchie46](https://github.com/ritchie46))
- Added `MutableArray::reserve` [\#1202](https://github.com/jorgecarleitao/arrow2/pull/1202) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Delayed dict [\#1185](https://github.com/jorgecarleitao/arrow2/pull/1185) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Fixed guide and improved examples [\#1247](https://github.com/jorgecarleitao/arrow2/pull/1247) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added documentation on parquet compatibility under `TimeUnit`. [\#1238](https://github.com/jorgecarleitao/arrow2/pull/1238) ([TurnOfACard](https://github.com/TurnOfACard))
- Fixed typo in error message for impl StructArray [\#1237](https://github.com/jorgecarleitao/arrow2/pull/1237) ([knil-sama](https://github.com/knil-sama))
- Fixed incorrect command in doc for generating ORC files [\#1234](https://github.com/jorgecarleitao/arrow2/pull/1234) ([poga](https://github.com/poga))
- Improved github page generation [\#1233](https://github.com/jorgecarleitao/arrow2/pull/1233) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fix a typo in the docs [\#1225](https://github.com/jorgecarleitao/arrow2/pull/1225) ([teymour-aldridge](https://github.com/teymour-aldridge))
- Fix some doc links/typos [\#1211](https://github.com/jorgecarleitao/arrow2/pull/1211) ([AnIrishDuck](https://github.com/AnIrishDuck))

**Testing updates:**

- Fixed clippy warnings [\#1227](https://github.com/jorgecarleitao/arrow2/pull/1227) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Updated integration test [\#1214](https://github.com/jorgecarleitao/arrow2/pull/1214) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.13.0](https://github.com/jorgecarleitao/arrow2/tree/v0.13.0) (2022-07-31)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.12.0...v0.13.0)

**Breaking changes:**

- Made `nested` argument of `array_to_pages` non-owning [\#1174](https://github.com/jorgecarleitao/arrow2/issues/1174)
- Replaced `Result` by `panic` in boolean comparison [\#1159](https://github.com/jorgecarleitao/arrow2/pull/1159) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved dictionary invariants [\#1137](https://github.com/jorgecarleitao/arrow2/pull/1137) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Change signature of PrimitiveScalar::value to return reference [\#1129](https://github.com/jorgecarleitao/arrow2/pull/1129) ([ncpenke](https://github.com/ncpenke))
- Removed need to pass encodings by value [\#1123](https://github.com/jorgecarleitao/arrow2/pull/1123) ([ritchie46](https://github.com/ritchie46))
- Removed unused `NativeType::to_ne_bytes` [\#1112](https://github.com/jorgecarleitao/arrow2/pull/1112) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid clone in `with_validity` [\#1104](https://github.com/jorgecarleitao/arrow2/pull/1104) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced need of `unsafe` in FFI [\#1100](https://github.com/jorgecarleitao/arrow2/pull/1100) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `Buffer::into_mut` and `make_mut` functions [\#1089](https://github.com/jorgecarleitao/arrow2/pull/1089) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Renamed `Bitmap::null_count` to `Bitmap::unset_bits` [\#1087](https://github.com/jorgecarleitao/arrow2/pull/1087) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `chunk_size` optional in parquet's `column_iter_to_arrays` [\#1055](https://github.com/jorgecarleitao/arrow2/pull/1055) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated from `Arc<dyn Array>` to `Box<dyn Array>` [\#1042](https://github.com/jorgecarleitao/arrow2/pull/1042) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support to read ORC [\#1189](https://github.com/jorgecarleitao/arrow2/pull/1189) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for limit pushdown to IPC reading [\#1135](https://github.com/jorgecarleitao/arrow2/pull/1135) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write and read Intervals from and to parquet [\#1122](https://github.com/jorgecarleitao/arrow2/pull/1122) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write `FixedSizeBinary` to Avro [\#1118](https://github.com/jorgecarleitao/arrow2/pull/1118) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for projections in reading IPC streams [\#1097](https://github.com/jorgecarleitao/arrow2/pull/1097) ([joshuataylor](https://github.com/joshuataylor))
- Added support to write parquet `_metadata` sidecar [\#1063](https://github.com/jorgecarleitao/arrow2/pull/1063) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added cow APIs \(2x-10x vs non-cow\) [\#1061](https://github.com/jorgecarleitao/arrow2/pull/1061) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and write f16 [\#1051](https://github.com/jorgecarleitao/arrow2/pull/1051) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error not implemented error when reading plain, after-dict pages for fix-len-binary from parquet [\#1192](https://github.com/jorgecarleitao/arrow2/pull/1192) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in decoding nested multi-page columns from parquet [\#1188](https://github.com/jorgecarleitao/arrow2/pull/1188) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in counting items in nested parquet [\#1182](https://github.com/jorgecarleitao/arrow2/pull/1182) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed reading stats from int96 parquet [\#1181](https://github.com/jorgecarleitao/arrow2/pull/1181) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed limit pushdown in parquet [\#1180](https://github.com/jorgecarleitao/arrow2/pull/1180) ([jorgecarleitao](https://github.com/jorgecarleitao))
- use `FnOnce` for `PrimitiveArray::apply_validity` [\#1176](https://github.com/jorgecarleitao/arrow2/pull/1176) ([ritchie46](https://github.com/ritchie46))
- release memory on predicate with 0% selectivity [\#1163](https://github.com/jorgecarleitao/arrow2/pull/1163) ([ritchie46](https://github.com/ritchie46))
- Fixed error in reading `Struct<List<...>>` from parquet [\#1150](https://github.com/jorgecarleitao/arrow2/pull/1150) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed IPC projection [\#1149](https://github.com/jorgecarleitao/arrow2/pull/1149) ([ritchie46](https://github.com/ritchie46))
- Fixed casting dictionary keys [\#1143](https://github.com/jorgecarleitao/arrow2/pull/1143) ([ritchie46](https://github.com/ritchie46))
- Fixed reading arrays from parquet with required children [\#1140](https://github.com/jorgecarleitao/arrow2/pull/1140) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed panic in deserializing nested statistics [\#1139](https://github.com/jorgecarleitao/arrow2/pull/1139) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Aligned name of `FixedSizeBinaryArray::values_iter` [\#1117](https://github.com/jorgecarleitao/arrow2/pull/1117) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in `FixedSizeListArray::new_null` [\#1114](https://github.com/jorgecarleitao/arrow2/pull/1114) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed panic in writing dictionaries to parquet [\#1113](https://github.com/jorgecarleitao/arrow2/pull/1113) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in reading chunked parquet [\#1108](https://github.com/jorgecarleitao/arrow2/pull/1108) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Raise error when invalid fields are passed to flight [\#1093](https://github.com/jorgecarleitao/arrow2/pull/1093) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IPC projection not sort projection [\#1082](https://github.com/jorgecarleitao/arrow2/pull/1082) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in chunked\_mut bitmap [\#1081](https://github.com/jorgecarleitao/arrow2/pull/1081) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed panic in bitmap assign\_mut [\#1078](https://github.com/jorgecarleitao/arrow2/pull/1078) ([ritchie46](https://github.com/ritchie46))
- Panic-free read of IPC files [\#1075](https://github.com/jorgecarleitao/arrow2/pull/1075) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped parquet2 \(minor\) requirement [\#1071](https://github.com/jorgecarleitao/arrow2/pull/1071) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed divide by zero on reading empty row group [\#1062](https://github.com/jorgecarleitao/arrow2/pull/1062) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed missing validation of number of encodings passed when writing to parquet [\#1057](https://github.com/jorgecarleitao/arrow2/pull/1057) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Improved performance of reading Binary from parquet [\#1190](https://github.com/jorgecarleitao/arrow2/pull/1190) ([ritchie46](https://github.com/ritchie46))
- Bumped to latest nightly [\#1186](https://github.com/jorgecarleitao/arrow2/pull/1186) ([gyscos](https://github.com/gyscos))
- Improved error message [\#1179](https://github.com/jorgecarleitao/arrow2/pull/1179) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and write nested dictionaries to parquet [\#1175](https://github.com/jorgecarleitao/arrow2/pull/1175) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutableUtf8Array::into_data` [\#1170](https://github.com/jorgecarleitao/arrow2/pull/1170) ([ritchie46](https://github.com/ritchie46))
- Added `Default` for `Utf8Array` [\#1169](https://github.com/jorgecarleitao/arrow2/pull/1169) ([ritchie46](https://github.com/ritchie46))
- fix\(parquet\): allow to read other logical types from parquet [\#1168](https://github.com/jorgecarleitao/arrow2/pull/1168) ([sundy-li](https://github.com/sundy-li))
- fix\(parquet\): enforce to use ParquetTimeUnit::Nanoseconds  for PhysicalType::Int96 [\#1167](https://github.com/jorgecarleitao/arrow2/pull/1167) ([sundy-li](https://github.com/sundy-li))
- Added constructor `MutableFixedSizeListArray::new_from` [\#1161](https://github.com/jorgecarleitao/arrow2/pull/1161) ([hohav](https://github.com/hohav))
- Removed unneeded `Default` constraint [\#1157](https://github.com/jorgecarleitao/arrow2/pull/1157) ([hohav](https://github.com/hohav))
- Improved checks to safety invariants in FFI [\#1154](https://github.com/jorgecarleitao/arrow2/pull/1154) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed un-needed indirection [\#1153](https://github.com/jorgecarleitao/arrow2/pull/1153) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Soften generic constraint of `Buffer` [\#1152](https://github.com/jorgecarleitao/arrow2/pull/1152) ([sundy-li](https://github.com/sundy-li))
- Use ahash by default [\#1148](https://github.com/jorgecarleitao/arrow2/pull/1148) ([ritchie46](https://github.com/ritchie46))
- Reduced bound checks [\#1142](https://github.com/jorgecarleitao/arrow2/pull/1142) ([ritchie46](https://github.com/ritchie46))
- Moved `Bytes` to own crate [\#1141](https://github.com/jorgecarleitao/arrow2/pull/1141) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed clippy for 1.62 [\#1134](https://github.com/jorgecarleitao/arrow2/pull/1134) ([Xuanwo](https://github.com/Xuanwo))
- Cleaned example [\#1130](https://github.com/jorgecarleitao/arrow2/pull/1130) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `O(N)` clone in writing CSV [\#1128](https://github.com/jorgecarleitao/arrow2/pull/1128) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid zeroed allocation in reading avro [\#1127](https://github.com/jorgecarleitao/arrow2/pull/1127) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced allocations of reading bitmaps from IPC [\#1126](https://github.com/jorgecarleitao/arrow2/pull/1126) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of reading from IPC [\#1125](https://github.com/jorgecarleitao/arrow2/pull/1125) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved parquet read performance [\#1124](https://github.com/jorgecarleitao/arrow2/pull/1124) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Optimized write nulls to Avro [\#1119](https://github.com/jorgecarleitao/arrow2/pull/1119) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `row_group::get_field_columns` public [\#1110](https://github.com/jorgecarleitao/arrow2/pull/1110) ([ritchie46](https://github.com/ritchie46))
- Removed some panics reading invalid parquet files [\#1106](https://github.com/jorgecarleitao/arrow2/pull/1106) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced reallocations when reading from IPC \(`~12%`\) [\#1105](https://github.com/jorgecarleitao/arrow2/pull/1105) ([ritchie46](https://github.com/ritchie46))
- Exposed utilities in `io::flight` [\#1094](https://github.com/jorgecarleitao/arrow2/pull/1094) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Accept decoding parquet's `i64` into `u32` written by `pyarrow` [\#1090](https://github.com/jorgecarleitao/arrow2/pull/1090) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code [\#1088](https://github.com/jorgecarleitao/arrow2/pull/1088) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed un-necessary allocation in `assign_ops` [\#1085](https://github.com/jorgecarleitao/arrow2/pull/1085) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced some macros by generics [\#1084](https://github.com/jorgecarleitao/arrow2/pull/1084) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of `Bitmap::make_mut` with offset [\#1079](https://github.com/jorgecarleitao/arrow2/pull/1079) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Implemented `Default` for `PrimitiveArray` [\#1073](https://github.com/jorgecarleitao/arrow2/pull/1073) ([ritchie46](https://github.com/ritchie46))
- Expose share counts in `Buffer` [\#1072](https://github.com/jorgecarleitao/arrow2/pull/1072) ([ritchie46](https://github.com/ritchie46))
- Added `compute::arity_assign` [\#1070](https://github.com/jorgecarleitao/arrow2/pull/1070) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance in lexical write \(~5%\) [\#1067](https://github.com/jorgecarleitao/arrow2/pull/1067) ([ritchie46](https://github.com/ritchie46))
- Added cast to/from `Null` from/to every type [\#1066](https://github.com/jorgecarleitao/arrow2/pull/1066) ([jorgecarleitao](https://github.com/jorgecarleitao))
- prevent unneeded offset check [\#1059](https://github.com/jorgecarleitao/arrow2/pull/1059) ([ritchie46](https://github.com/ritchie46))

**Documentation updates:**

- Fixed parquet write example [\#1193](https://github.com/jorgecarleitao/arrow2/pull/1193) ([rajasekarv](https://github.com/rajasekarv))
- Improved docs [\#1164](https://github.com/jorgecarleitao/arrow2/pull/1164) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Minor cleanup of internal namings [\#1160](https://github.com/jorgecarleitao/arrow2/pull/1160) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added example reading Avro produced by Kafka [\#1151](https://github.com/jorgecarleitao/arrow2/pull/1151) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Updated license wording [\#1138](https://github.com/jorgecarleitao/arrow2/pull/1138) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed wrong package name in examples [\#1133](https://github.com/jorgecarleitao/arrow2/pull/1133) ([Xuanwo](https://github.com/Xuanwo))
- Improved example [\#1131](https://github.com/jorgecarleitao/arrow2/pull/1131) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#1111](https://github.com/jorgecarleitao/arrow2/pull/1111) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved examples [\#1109](https://github.com/jorgecarleitao/arrow2/pull/1109) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved internal docs [\#1107](https://github.com/jorgecarleitao/arrow2/pull/1107) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added notes about creating parquet files and submodules in the development documentation [\#1096](https://github.com/jorgecarleitao/arrow2/pull/1096) ([joshuataylor](https://github.com/joshuataylor))
- Improved docs for `BooleanArray` [\#1083](https://github.com/jorgecarleitao/arrow2/pull/1083) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added missing link to guide [\#1065](https://github.com/jorgecarleitao/arrow2/pull/1065) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improve Docs Readability [\#1054](https://github.com/jorgecarleitao/arrow2/pull/1054) ([ryanrussell](https://github.com/ryanrussell))

**Testing updates:**

- Temporary skip decimal256 integration tests [\#1198](https://github.com/jorgecarleitao/arrow2/pull/1198) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code [\#1183](https://github.com/jorgecarleitao/arrow2/pull/1183) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made kafka schema\_id `u32` in example [\#1162](https://github.com/jorgecarleitao/arrow2/pull/1162) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#1158](https://github.com/jorgecarleitao/arrow2/pull/1158) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped MIRI [\#1156](https://github.com/jorgecarleitao/arrow2/pull/1156) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code in flight integration tests [\#1136](https://github.com/jorgecarleitao/arrow2/pull/1136) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests for nested parquet [\#1121](https://github.com/jorgecarleitao/arrow2/pull/1121) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests for reading and writing CSV [\#1120](https://github.com/jorgecarleitao/arrow2/pull/1120) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added test for scalar division [\#1115](https://github.com/jorgecarleitao/arrow2/pull/1115) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#1103](https://github.com/jorgecarleitao/arrow2/pull/1103) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Enabled more integration tests with pyarrow [\#1102](https://github.com/jorgecarleitao/arrow2/pull/1102) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `Bytes` \(internal\) [\#1099](https://github.com/jorgecarleitao/arrow2/pull/1099) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Updated patch to arrow integration tests [\#1068](https://github.com/jorgecarleitao/arrow2/pull/1068) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#1064](https://github.com/jorgecarleitao/arrow2/pull/1064) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.12.0](https://github.com/jorgecarleitao/arrow2/tree/v0.12.0) (2022-06-05)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.11.2...v0.12.0)

**Breaking changes:**

- Require one encoding per parquet column on write [\#1012](https://github.com/jorgecarleitao/arrow2/issues/1012)
- Bumped parquet2 [\#1035](https://github.com/jorgecarleitao/arrow2/pull/1035) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of deserializing JSON \(2x\) [\#1024](https://github.com/jorgecarleitao/arrow2/pull/1024) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Remove `from_trusted_len_*` from `Buffer` [\#1020](https://github.com/jorgecarleitao/arrow2/pull/1020) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped arrow-format [\#1011](https://github.com/jorgecarleitao/arrow2/pull/1011) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replace `fn Offset::is_large()` as `const Offset::IS_LARGE` [\#1002](https://github.com/jorgecarleitao/arrow2/pull/1002) ([HaoYang670](https://github.com/HaoYang670))
- Renamed `ArrowError` to `Error` [\#993](https://github.com/jorgecarleitao/arrow2/pull/993) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support to deserialize `MapArray` from parquet [\#1045](https://github.com/jorgecarleitao/arrow2/pull/1045) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for random access reads from IPC [\#1034](https://github.com/jorgecarleitao/arrow2/pull/1034) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for custom sort `build_compare_fn` [\#1016](https://github.com/jorgecarleitao/arrow2/pull/1016) ([b41sh](https://github.com/b41sh))
- Added support to write nested parquet [\#1007](https://github.com/jorgecarleitao/arrow2/pull/1007) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for deserializing JSON from iterator [\#989](https://github.com/jorgecarleitao/arrow2/pull/989) ([cjermain](https://github.com/cjermain))

**Fixed bugs:**

- Writing of `ListArray` does not preserve all values [\#1008](https://github.com/jorgecarleitao/arrow2/issues/1008)
- Write a two-dimensional list to parquet file failed [\#992](https://github.com/jorgecarleitao/arrow2/issues/992)
- Writing to Parquet fails for extension types that contain lists [\#830](https://github.com/jorgecarleitao/arrow2/issues/830)
- Fixed using lower limit than size of first parquet row group [\#1046](https://github.com/jorgecarleitao/arrow2/pull/1046) ([arxra](https://github.com/arxra))
- Fixed error in consuming sliced `FixedSizedBinary` from c data interface \(FFI\) [\#1026](https://github.com/jorgecarleitao/arrow2/pull/1026) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed lexsort limit equal or greater than row\_count [\#1021](https://github.com/jorgecarleitao/arrow2/pull/1021) ([b41sh](https://github.com/b41sh))
- Fixed error in reading nested parquet structs [\#1015](https://github.com/jorgecarleitao/arrow2/pull/1015) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed panic on debug print of invalid timezones [\#1013](https://github.com/jorgecarleitao/arrow2/pull/1013) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Treat empty timezone string as no-timezone [\#1009](https://github.com/jorgecarleitao/arrow2/pull/1009) ([dbr](https://github.com/dbr))
- Fixed encoding of `NaN` to json [\#990](https://github.com/jorgecarleitao/arrow2/pull/990) ([SimonSchneider](https://github.com/SimonSchneider))
- Fixed error in writing `ListArray` to parquet [\#984](https://github.com/jorgecarleitao/arrow2/pull/984) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed decoding Binary Plain pages with dictionary pages [\#982](https://github.com/jorgecarleitao/arrow2/pull/982) ([aptr322](https://github.com/aptr322))

**Enhancements:**

- Added `Debug` and `PartialEq` for `MapArray` [\#1043](https://github.com/jorgecarleitao/arrow2/pull/1043) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Exposed compression levels for parquet [\#1041](https://github.com/jorgecarleitao/arrow2/pull/1041) ([ritchie46](https://github.com/ritchie46))
- Added `.arced`/`.boxed` to arrays [\#1040](https://github.com/jorgecarleitao/arrow2/pull/1040) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added utility to create encodings [\#1018](https://github.com/jorgecarleitao/arrow2/pull/1018) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `parquet_to_arrow_schema` public [\#1006](https://github.com/jorgecarleitao/arrow2/pull/1006) ([martingallagher](https://github.com/martingallagher))
- Speeded up `min_max_boolean` for the case where all values are null [\#1005](https://github.com/jorgecarleitao/arrow2/pull/1005) ([HaoYang670](https://github.com/HaoYang670))
- Simplified `min_max_string` and `min_max_binary` [\#1004](https://github.com/jorgecarleitao/arrow2/pull/1004) ([HaoYang670](https://github.com/HaoYang670))
- Added support for Decimal in `build_compare` [\#998](https://github.com/jorgecarleitao/arrow2/pull/998) ([GPSnoopy](https://github.com/GPSnoopy))
- remove accidental quadratic null\_count [\#991](https://github.com/jorgecarleitao/arrow2/pull/991) ([ritchie46](https://github.com/ritchie46))
- Aligns MutableDictionaryArray's with MutablePrimitiveArrays with TryPush [\#981](https://github.com/jorgecarleitao/arrow2/pull/981) ([TurnOfACard](https://github.com/TurnOfACard))

**Documentation updates:**

- Cleaned docs for BinaryArray [\#1047](https://github.com/jorgecarleitao/arrow2/pull/1047) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved API docs for `MutableBitmap` [\#1025](https://github.com/jorgecarleitao/arrow2/pull/1025) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved docs for `bitmap` [\#1022](https://github.com/jorgecarleitao/arrow2/pull/1022) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved API docs for `PrimitiveArray` and `Utf8Array` [\#1017](https://github.com/jorgecarleitao/arrow2/pull/1017) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed dev guide [\#1003](https://github.com/jorgecarleitao/arrow2/pull/1003) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Added more tests [\#1029](https://github.com/jorgecarleitao/arrow2/pull/1029) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved coverage reporting to `cargo-llvm-cov` [\#1028](https://github.com/jorgecarleitao/arrow2/pull/1028) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests \(increase coverage\) [\#1027](https://github.com/jorgecarleitao/arrow2/pull/1027) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved tests from lib to `tests` [\#1001](https://github.com/jorgecarleitao/arrow2/pull/1001) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Allowed feature-specific test runs [\#985](https://github.com/jorgecarleitao/arrow2/pull/985) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.11.2](https://github.com/jorgecarleitao/arrow2/tree/v0.11.2) (2022-05-05)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.11.1...v0.11.2)

**New features:**

- Added support to append to existing IPC Arrow file [\#972](https://github.com/jorgecarleitao/arrow2/pull/972) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added pop to utf8/binary/fixedSize MutableArray [\#966](https://github.com/jorgecarleitao/arrow2/pull/966) ([ygf11](https://github.com/ygf11))
- Added support for union scalars [\#930](https://github.com/jorgecarleitao/arrow2/pull/930) ([ncpenke](https://github.com/ncpenke))

**Fixed bugs:**

- Added support to read nested binary from parquet [\#978](https://github.com/jorgecarleitao/arrow2/pull/978) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed empty reader panic for NDJSON type infer [\#974](https://github.com/jorgecarleitao/arrow2/pull/974) ([Roberto-XY](https://github.com/Roberto-XY))
- Prevented SO in large parquet files [\#973](https://github.com/jorgecarleitao/arrow2/pull/973) ([ritchie46](https://github.com/ritchie46))
- Fixed API bug in `async` read of IPC metadata [\#969](https://github.com/jorgecarleitao/arrow2/pull/969) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed writing required list to parquet [\#968](https://github.com/jorgecarleitao/arrow2/pull/968) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added support Parquet deserialize LargeList and Uint data types [\#979](https://github.com/jorgecarleitao/arrow2/pull/979) ([b41sh](https://github.com/b41sh))
- Made reading of IPC dictionaries lazy [\#971](https://github.com/jorgecarleitao/arrow2/pull/971) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Allowed creating IPC `FileWriter` without writing to the file [\#970](https://github.com/jorgecarleitao/arrow2/pull/970) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.11.1](https://github.com/jorgecarleitao/arrow2/tree/v0.11.1) (2022-04-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.11.0...v0.11.1)

**Fixed bugs:**

- Fixed bug in writing csv with buffer resizing [\#965](https://github.com/jorgecarleitao/arrow2/pull/965) ([ritchie46](https://github.com/ritchie46))

## [v0.11.0](https://github.com/jorgecarleitao/arrow2/tree/v0.11.0) (2022-04-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.10.1...v0.11.0)

**Breaking changes:**

- Refactored parquet statistics deserialization [\#962](https://github.com/jorgecarleitao/arrow2/pull/962) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made GroupFilter `Send + Sync` [\#947](https://github.com/jorgecarleitao/arrow2/pull/947) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support for non-ordered projections to IPC reading [\#961](https://github.com/jorgecarleitao/arrow2/pull/961) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for reading indexed parquet pages [\#923](https://github.com/jorgecarleitao/arrow2/pull/923) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Parquet regression: `exceptions.ArrowErrorException: NotYetImplemented("Can't read Dictionary(UInt32, LargeUtf8, false) from parquet")` [\#955](https://github.com/jorgecarleitao/arrow2/issues/955)
- Reading Parquet binary column panics during deserialization 'attempt to subtract with overflow` [\#944](https://github.com/jorgecarleitao/arrow2/issues/944)
- Reading Parquet file written by pyarrow with `lz4` compression fails with `OutOfSpec("Thrift out of range")` [\#940](https://github.com/jorgecarleitao/arrow2/issues/940)
- Issues when trying to create a parquet file with FixedSizedListArray [\#691](https://github.com/jorgecarleitao/arrow2/issues/691)
- Fixed bug in writing csv with buffer resizing [\#965](https://github.com/jorgecarleitao/arrow2/pull/965) ([ritchie46](https://github.com/ritchie46))
- Fixed bug in reading binary parquet [\#945](https://github.com/jorgecarleitao/arrow2/pull/945) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in writing fixedSizeListArray to parquet [\#941](https://github.com/jorgecarleitao/arrow2/pull/941) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed support to read dict nested binary parquet  [\#924](https://github.com/jorgecarleitao/arrow2/pull/924) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Reduced memory usage in reading parquet [\#964](https://github.com/jorgecarleitao/arrow2/pull/964) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simpler IPC code [\#939](https://github.com/jorgecarleitao/arrow2/pull/939) ([jorgecarleitao](https://github.com/jorgecarleitao))
- don't allocate string when writing to csv [\#935](https://github.com/jorgecarleitao/arrow2/pull/935) ([ritchie46](https://github.com/ritchie46))
- Removed un-needed generic parameter [\#927](https://github.com/jorgecarleitao/arrow2/pull/927) ([jorgecarleitao](https://github.com/jorgecarleitao))
- update to odbc-api 0.36.0 [\#925](https://github.com/jorgecarleitao/arrow2/pull/925) ([pacman82](https://github.com/pacman82))

**Documentation updates:**

- Fixed example of parallel read via rayon [\#958](https://github.com/jorgecarleitao/arrow2/pull/958) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed guide deployment [\#931](https://github.com/jorgecarleitao/arrow2/pull/931) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Typo fix [\#919](https://github.com/jorgecarleitao/arrow2/pull/919) ([bkmgit](https://github.com/bkmgit))

**Testing updates:**

- Fixed patch of integration tests [\#960](https://github.com/jorgecarleitao/arrow2/pull/960) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added test for MapArray [\#942](https://github.com/jorgecarleitao/arrow2/pull/942) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed wrong clippy warning [\#938](https://github.com/jorgecarleitao/arrow2/pull/938) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.10.1](https://github.com/jorgecarleitao/arrow2/tree/v0.10.1) (2022-03-16)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.10.0...v0.10.1)

**New features:**

- Added support to write `StructArray` to Avro [\#909](https://github.com/jorgecarleitao/arrow2/pull/909) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write `ListArray` to Avro [\#908](https://github.com/jorgecarleitao/arrow2/pull/908) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in `FixedSizeBinaryArray::new_null` [\#914](https://github.com/jorgecarleitao/arrow2/pull/914) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- remove csv dependency for csv-write [\#917](https://github.com/jorgecarleitao/arrow2/pull/917) ([ritchie46](https://github.com/ritchie46))
- Added `capacity` to some mutable arrays and tests [\#913](https://github.com/jorgecarleitao/arrow2/pull/913) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Support `sum`, `min` and `max` for extension and decimal [\#907](https://github.com/jorgecarleitao/arrow2/pull/907) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Added more tests [\#910](https://github.com/jorgecarleitao/arrow2/pull/910) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.10.0](https://github.com/jorgecarleitao/arrow2/tree/v0.10.0) (2022-03-12)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.9.1...v0.10.0)

**Breaking changes:**

- Renamed `Ffi_ArrowArray` and `Ffi_ArrowSchema` [\#859](https://github.com/jorgecarleitao/arrow2/issues/859)
- Improved performance and stability of writing to CSV [\#866](https://github.com/jorgecarleitao/arrow2/pull/866) ([ritchie46](https://github.com/ritchie46))
- Simplified API for writing to JSON [\#864](https://github.com/jorgecarleitao/arrow2/pull/864) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified API to import from FFI [\#854](https://github.com/jorgecarleitao/arrow2/pull/854) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified compute \(lower/upper\) [\#847](https://github.com/jorgecarleitao/arrow2/pull/847) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified infering arrow schema from a parquet schema [\#819](https://github.com/jorgecarleitao/arrow2/pull/819) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped parquet and aligned API to fit into it [\#795](https://github.com/jorgecarleitao/arrow2/pull/795) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added `GrowableUnion` [\#902](https://github.com/jorgecarleitao/arrow2/pull/902) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added cast to `months_days_ns` [\#900](https://github.com/jorgecarleitao/arrow2/pull/900) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `hash` of `month_day_ns` arrays [\#899](https://github.com/jorgecarleitao/arrow2/pull/899) ([jorgecarleitao](https://github.com/jorgecarleitao))
- IPC sink types and IPC file stream [\#878](https://github.com/jorgecarleitao/arrow2/pull/878) ([dexterduck](https://github.com/dexterduck))
- implemented `futures::Sink` for parquet async writer [\#877](https://github.com/jorgecarleitao/arrow2/pull/877) ([dexterduck](https://github.com/dexterduck))
- Added `try_new` and `new` to all arrays [\#873](https://github.com/jorgecarleitao/arrow2/pull/873) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for datatypes serde [\#858](https://github.com/jorgecarleitao/arrow2/pull/858) ([houqp](https://github.com/houqp))
- Added support to the Arrow C stream interface \(read and write\) [\#857](https://github.com/jorgecarleitao/arrow2/pull/857) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Support to read/write from/to ODBC [\#849](https://github.com/jorgecarleitao/arrow2/pull/849) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added operators that include validities in comparisons [\#846](https://github.com/jorgecarleitao/arrow2/pull/846) ([ritchie46](https://github.com/ritchie46))
- Added support to read and write `Decimal128` to Avro [\#837](https://github.com/jorgecarleitao/arrow2/pull/837) ([potter420](https://github.com/potter420))
- Added support to read Arrow streams asynchronously [\#832](https://github.com/jorgecarleitao/arrow2/pull/832) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write `LargeUtf8` and `LargeBinary` to Avro [\#828](https://github.com/jorgecarleitao/arrow2/pull/828) ([illumination-k](https://github.com/illumination-k))
- Added support for pushdown projection in reading Avro [\#827](https://github.com/jorgecarleitao/arrow2/pull/827) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read Avro's structs [\#826](https://github.com/jorgecarleitao/arrow2/pull/826) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write largeUtf8/Binary to Avro [\#825](https://github.com/jorgecarleitao/arrow2/pull/825) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added json serialization of timestamp/date32/date64 [\#814](https://github.com/jorgecarleitao/arrow2/pull/814) ([ritchie46](https://github.com/ritchie46))
- Added `BooleanArray::from_trusted_len_values_iter_unchecked` [\#799](https://github.com/jorgecarleitao/arrow2/pull/799) ([ritchie46](https://github.com/ritchie46))
- Added `MutableUtf8Array::extend_values` [\#798](https://github.com/jorgecarleitao/arrow2/pull/798) ([ritchie46](https://github.com/ritchie46))
- Added COW semantics to `Buffer`, `Bitmap` and some arrays [\#794](https://github.com/jorgecarleitao/arrow2/pull/794) ([ritchie46](https://github.com/ritchie46))
- Added support to read parquet row groups in chunks [\#789](https://github.com/jorgecarleitao/arrow2/pull/789) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added scalar bitwise ops [\#788](https://github.com/jorgecarleitao/arrow2/pull/788) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated to portable simd [\#747](https://github.com/jorgecarleitao/arrow2/pull/747) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed edge case in reading multiple parquet pages [\#904](https://github.com/jorgecarleitao/arrow2/pull/904) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bug fix in offset for sliced unions [\#891](https://github.com/jorgecarleitao/arrow2/pull/891) ([ncpenke](https://github.com/ncpenke))
- Fix edge case in reading nested parquet [\#884](https://github.com/jorgecarleitao/arrow2/pull/884) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed unsoundness of `#derive(Clone)` for FFI structs [\#882](https://github.com/jorgecarleitao/arrow2/pull/882) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed json writing of dates and datetimes [\#867](https://github.com/jorgecarleitao/arrow2/pull/867) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed reading parquet with timezone [\#862](https://github.com/jorgecarleitao/arrow2/pull/862) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in writing compressed IPC arrow [\#855](https://github.com/jorgecarleitao/arrow2/pull/855) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed wrong null\_count when slicing a sliced Bitmap [\#848](https://github.com/jorgecarleitao/arrow2/pull/848) ([satlank](https://github.com/satlank))
- Fixed error in writing compressed IPC files [\#840](https://github.com/jorgecarleitao/arrow2/pull/840) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed float to i128 cast [\#817](https://github.com/jorgecarleitao/arrow2/pull/817) ([houqp](https://github.com/houqp))
- fix unescaped '"' in json writing [\#812](https://github.com/jorgecarleitao/arrow2/pull/812) ([ritchie46](https://github.com/ritchie46))
- Fixed reading parquet binary dict page [\#791](https://github.com/jorgecarleitao/arrow2/pull/791) ([danburkert](https://github.com/danburkert))

**Enhancements:**

- Add `FixedSizeBinaryScalar` [\#782](https://github.com/jorgecarleitao/arrow2/issues/782)
- Use more idiomatic versions [\#898](https://github.com/jorgecarleitao/arrow2/pull/898) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for min/max for decimal [\#897](https://github.com/jorgecarleitao/arrow2/pull/897) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `FixedSizeList::try_push_valid` public and added `new_with_field` [\#887](https://github.com/jorgecarleitao/arrow2/pull/887) ([ncpenke](https://github.com/ncpenke))
- Added `MutableFixedList::mut_values` [\#886](https://github.com/jorgecarleitao/arrow2/pull/886) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IPC IO use `try_new` [\#879](https://github.com/jorgecarleitao/arrow2/pull/879) ([jorgecarleitao](https://github.com/jorgecarleitao))
- expose `ListValuesIter` [\#874](https://github.com/jorgecarleitao/arrow2/pull/874) ([ritchie46](https://github.com/ritchie46))
- Bumped crc [\#856](https://github.com/jorgecarleitao/arrow2/pull/856) ([jorgecarleitao](https://github.com/jorgecarleitao))
- DRY parquet reading [\#845](https://github.com/jorgecarleitao/arrow2/pull/845) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored \(internal\) fmt [\#842](https://github.com/jorgecarleitao/arrow2/pull/842) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped zstd [\#841](https://github.com/jorgecarleitao/arrow2/pull/841) ([jorgecarleitao](https://github.com/jorgecarleitao))
- inline push [\#835](https://github.com/jorgecarleitao/arrow2/pull/835) ([ritchie46](https://github.com/ritchie46))
- Increased API consistency for COW and respective docs [\#833](https://github.com/jorgecarleitao/arrow2/pull/833) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved flexibility of reading parquet [\#820](https://github.com/jorgecarleitao/arrow2/pull/820) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Small improvement to deserializing fixed-len parquet statistics. [\#818](https://github.com/jorgecarleitao/arrow2/pull/818) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for other timestamp units from parquet [\#803](https://github.com/jorgecarleitao/arrow2/pull/803) ([jorgecarleitao](https://github.com/jorgecarleitao))
- More to `into_mut` implementations [\#801](https://github.com/jorgecarleitao/arrow2/pull/801) ([ritchie46](https://github.com/ritchie46))
- Added `FixedSizeListScalar` and `FixedSizeBinaryScalar` [\#786](https://github.com/jorgecarleitao/arrow2/pull/786) ([illumination-k](https://github.com/illumination-k))
- DRY parquet module [\#785](https://github.com/jorgecarleitao/arrow2/pull/785) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Improved documentation [\#860](https://github.com/jorgecarleitao/arrow2/pull/860) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made crate `deny(missing_docs)` [\#808](https://github.com/jorgecarleitao/arrow2/pull/808) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed doc for `Bitmap::set_bit` [\#802](https://github.com/jorgecarleitao/arrow2/pull/802) ([yjshen](https://github.com/yjshen))
- Fixed `dyn Array::slice` docstring [\#792](https://github.com/jorgecarleitao/arrow2/pull/792) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Simpler code \(DRY\) [\#901](https://github.com/jorgecarleitao/arrow2/pull/901) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed integration test [\#885](https://github.com/jorgecarleitao/arrow2/pull/885) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code to generate parquet files for tests [\#883](https://github.com/jorgecarleitao/arrow2/pull/883) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed un-needed `unsafe` [\#843](https://github.com/jorgecarleitao/arrow2/pull/843) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#810](https://github.com/jorgecarleitao/arrow2/pull/810) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced code duplication [\#805](https://github.com/jorgecarleitao/arrow2/pull/805) ([jorgecarleitao](https://github.com/jorgecarleitao))
- upgrade to clap 3.0 [\#797](https://github.com/jorgecarleitao/arrow2/pull/797) ([Jimexist](https://github.com/Jimexist))
- Simplified avro reading and added more tests [\#737](https://github.com/jorgecarleitao/arrow2/pull/737) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.9.1](https://github.com/jorgecarleitao/arrow2/tree/v0.9.1) (2022-01-19)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.9.0...v0.9.1)

**New features:**

- Added support for compare dictionary-encoded with scalar [\#686](https://github.com/jorgecarleitao/arrow2/pull/686) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Allowed passing `None` as ipc\_fields in flight API [\#780](https://github.com/jorgecarleitao/arrow2/pull/780) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Read dict binary from parquet [\#781](https://github.com/jorgecarleitao/arrow2/pull/781) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and write float dict from parquet [\#778](https://github.com/jorgecarleitao/arrow2/pull/778) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Fixed CI for SIMD [\#779](https://github.com/jorgecarleitao/arrow2/pull/779) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.9.0](https://github.com/jorgecarleitao/arrow2/tree/v0.9.0) (2022-01-14)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.8.1...v0.9.0)

**Breaking changes:**

- Added number of rows read in CSV inference [\#765](https://github.com/jorgecarleitao/arrow2/pull/765) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored `nullif` [\#753](https://github.com/jorgecarleitao/arrow2/pull/753) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated to latest parquet2 [\#752](https://github.com/jorgecarleitao/arrow2/pull/752) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replace flatbuffers dependency by Planus [\#732](https://github.com/jorgecarleitao/arrow2/pull/732) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `Schema` and `Field` [\#728](https://github.com/jorgecarleitao/arrow2/pull/728) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced `RecordBatch` by `Chunk` [\#717](https://github.com/jorgecarleitao/arrow2/pull/717) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `Option` from fields' metadata [\#715](https://github.com/jorgecarleitao/arrow2/pull/715) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved dict\_id to IPC-specific IO [\#713](https://github.com/jorgecarleitao/arrow2/pull/713) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved is\_ordered from `Field` to `DataType::Dictionary` [\#711](https://github.com/jorgecarleitao/arrow2/pull/711) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored JSON writing \(5-10x\) [\#709](https://github.com/jorgecarleitao/arrow2/pull/709) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made Avro read API use `Block` and `CompressedBlock` [\#698](https://github.com/jorgecarleitao/arrow2/pull/698) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified most traits [\#696](https://github.com/jorgecarleitao/arrow2/pull/696) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced `Display` by `Debug` for `Array` [\#694](https://github.com/jorgecarleitao/arrow2/pull/694) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced `MutableBuffer` by `std::Vec` [\#693](https://github.com/jorgecarleitao/arrow2/pull/693) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `Utf8Scalar` and `BinaryScalar` [\#660](https://github.com/jorgecarleitao/arrow2/pull/660) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified Primitive and Boolean scalar [\#648](https://github.com/jorgecarleitao/arrow2/pull/648) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Add `and_scalar` and `or_scalar` for boolean\_kleene [\#662](https://github.com/jorgecarleitao/arrow2/issues/662)
- Add `lower` and `upper` support for string [\#635](https://github.com/jorgecarleitao/arrow2/issues/635)
- Added support to cast decimal [\#761](https://github.com/jorgecarleitao/arrow2/pull/761) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to deserialize JSON \(!= NDJSON\) [\#758](https://github.com/jorgecarleitao/arrow2/pull/758) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to infer nested json structs [\#750](https://github.com/jorgecarleitao/arrow2/pull/750) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to compare intervals [\#746](https://github.com/jorgecarleitao/arrow2/pull/746) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `any` and `all` kernel [\#739](https://github.com/jorgecarleitao/arrow2/pull/739) ([ritchie46](https://github.com/ritchie46))
- Added support to write Avro async [\#736](https://github.com/jorgecarleitao/arrow2/pull/736) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write interval to Avro [\#734](https://github.com/jorgecarleitao/arrow2/pull/734) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `and_scalar` and `or_scalar` for boolean kleene [\#723](https://github.com/jorgecarleitao/arrow2/pull/723) ([silathdiir](https://github.com/silathdiir))
- Added `and_scalar` and `or_scalar` for boolean [\#707](https://github.com/jorgecarleitao/arrow2/pull/707) ([silathdiir](https://github.com/silathdiir))
- Refactored JSON read to split IO-bounded from CPU-bounded tasks [\#706](https://github.com/jorgecarleitao/arrow2/pull/706) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more conversions from parquet [\#701](https://github.com/jorgecarleitao/arrow2/pull/701) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for compressed Avro write [\#699](https://github.com/jorgecarleitao/arrow2/pull/699) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write to Avro [\#690](https://github.com/jorgecarleitao/arrow2/pull/690) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added dynamic version of negation [\#685](https://github.com/jorgecarleitao/arrow2/pull/685) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read dictionary-encoded required parquet pages [\#683](https://github.com/jorgecarleitao/arrow2/pull/683) ([mdrach](https://github.com/mdrach))
- Added `upper` [\#664](https://github.com/jorgecarleitao/arrow2/pull/664) ([Xuanwo](https://github.com/Xuanwo))
- Added `lower` [\#641](https://github.com/jorgecarleitao/arrow2/pull/641) ([Xuanwo](https://github.com/Xuanwo))
- Added support for `async` read of Avro [\#620](https://github.com/jorgecarleitao/arrow2/pull/620) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Pyarrow and Arrow2 don't agree on Timestamp resolution [\#700](https://github.com/jorgecarleitao/arrow2/issues/700)
- Writing compressed dictionary in parquet corrupts the files [\#667](https://github.com/jorgecarleitao/arrow2/issues/667)
- Replaced assert by error in IPC read [\#748](https://github.com/jorgecarleitao/arrow2/pull/748) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made all panics in IPC read errors [\#722](https://github.com/jorgecarleitao/arrow2/pull/722) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in compare booleans [\#721](https://github.com/jorgecarleitao/arrow2/pull/721) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in dispatching scalar arithmetics [\#682](https://github.com/jorgecarleitao/arrow2/pull/682) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in reading negative decimals from parquet [\#679](https://github.com/jorgecarleitao/arrow2/pull/679) ([mdrach](https://github.com/mdrach))
- Made IPC reader less restrictive [\#678](https://github.com/jorgecarleitao/arrow2/pull/678) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in trait constraint in compute [\#665](https://github.com/jorgecarleitao/arrow2/pull/665) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed performance regression of CSV reading [\#657](https://github.com/jorgecarleitao/arrow2/pull/657) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed filter of predicate with validity [\#653](https://github.com/jorgecarleitao/arrow2/pull/653) ([ritchie46](https://github.com/ritchie46))
- Made `Scalar: Send+Sync` [\#644](https://github.com/jorgecarleitao/arrow2/pull/644) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Feature: JSON IO? [\#712](https://github.com/jorgecarleitao/arrow2/issues/712)
- Simplified code [\#760](https://github.com/jorgecarleitao/arrow2/pull/760) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added iterator of values of `FixedBinaryArray` [\#757](https://github.com/jorgecarleitao/arrow2/pull/757) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Remove un-needed `unsafe` [\#756](https://github.com/jorgecarleitao/arrow2/pull/756) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced un-needed `unsafe` [\#755](https://github.com/jorgecarleitao/arrow2/pull/755) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IO `#[forbid(unsafe)]` [\#749](https://github.com/jorgecarleitao/arrow2/pull/749) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved reading nullable Avro arrays [\#727](https://github.com/jorgecarleitao/arrow2/pull/727) ([Igosuki](https://github.com/Igosuki))
- Allow to create primitive array by vec without extra memcopy [\#710](https://github.com/jorgecarleitao/arrow2/pull/710) ([sundy-li](https://github.com/sundy-li))
- Removed requirement of `use Array` to access primitives' `data_type` [\#697](https://github.com/jorgecarleitao/arrow2/pull/697) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Cleaned up trait usage and added forbid\_unsafe to parts [\#695](https://github.com/jorgecarleitao/arrow2/pull/695) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated from `avro-rs` to `avro-schema` [\#692](https://github.com/jorgecarleitao/arrow2/pull/692) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutablePrimitiveArray::extend_constant` [\#689](https://github.com/jorgecarleitao/arrow2/pull/689) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Do not write validity without nulls in IPC [\#688](https://github.com/jorgecarleitao/arrow2/pull/688) ([jorgecarleitao](https://github.com/jorgecarleitao))
- DRY code via macro [\#681](https://github.com/jorgecarleitao/arrow2/pull/681) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `dyn Array` and `Scalar` usable in `#[derive(PartialEq)]` [\#680](https://github.com/jorgecarleitao/arrow2/pull/680) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IPC ZSTD-compressed consumable by pyarrow [\#675](https://github.com/jorgecarleitao/arrow2/pull/675) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified trait bounds in arithmetics [\#671](https://github.com/jorgecarleitao/arrow2/pull/671) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of reading utf8 required from parquet \(-15%\) [\#670](https://github.com/jorgecarleitao/arrow2/pull/670) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid double utf8 checks on MutableUtf8 -\> Utf8 [\#655](https://github.com/jorgecarleitao/arrow2/pull/655) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `Buffer::offset` public [\#652](https://github.com/jorgecarleitao/arrow2/pull/652) ([ritchie46](https://github.com/ritchie46))
- Improved performance in cast Primitive to Binary/String \(2x\) [\#646](https://github.com/jorgecarleitao/arrow2/pull/646) ([sundy-li](https://github.com/sundy-li))
- Made `Filter: Send+Sync` [\#645](https://github.com/jorgecarleitao/arrow2/pull/645) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made API to create field accept `String` [\#643](https://github.com/jorgecarleitao/arrow2/pull/643) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Fixed clippy \(coming from 1.58\) [\#763](https://github.com/jorgecarleitao/arrow2/pull/763) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Described how to run part of the tests [\#762](https://github.com/jorgecarleitao/arrow2/pull/762) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved README [\#735](https://github.com/jorgecarleitao/arrow2/pull/735) ([jorgecarleitao](https://github.com/jorgecarleitao))
- clarify boolean value in DataType::Dictionary [\#718](https://github.com/jorgecarleitao/arrow2/pull/718) ([ritchie46](https://github.com/ritchie46))
- readme typo [\#687](https://github.com/jorgecarleitao/arrow2/pull/687) ([max-sixty](https://github.com/max-sixty))
- Added example to read parquet in parallel with rayon [\#658](https://github.com/jorgecarleitao/arrow2/pull/658) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added documentation to `Bitmap::as_slice` [\#654](https://github.com/jorgecarleitao/arrow2/pull/654) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Improved json tests [\#742](https://github.com/jorgecarleitao/arrow2/pull/742) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added integration tests for writing compressed parquet [\#740](https://github.com/jorgecarleitao/arrow2/pull/740) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Updated patch for integration test [\#731](https://github.com/jorgecarleitao/arrow2/pull/731) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added cargo check to benchmarks [\#730](https://github.com/jorgecarleitao/arrow2/pull/730) ([sundy-li](https://github.com/sundy-li))
- More tests to CSV writing [\#724](https://github.com/jorgecarleitao/arrow2/pull/724) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added integration tests for other compressions with parquet from pyarrow [\#674](https://github.com/jorgecarleitao/arrow2/pull/674) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped nightly in CI [\#672](https://github.com/jorgecarleitao/arrow2/pull/672) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Invalidate caches from CI. [\#656](https://github.com/jorgecarleitao/arrow2/pull/656) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.8.1](https://github.com/jorgecarleitao/arrow2/tree/v0.8.1) (2021-11-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.8.0...v0.8.1)

**Fixed bugs:**

- Fixed compilation with individual features activated [\#642](https://github.com/jorgecarleitao/arrow2/pull/642) ([ritchie46](https://github.com/ritchie46))

## [v0.8.0](https://github.com/jorgecarleitao/arrow2/tree/v0.8.0) (2021-11-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.7.0...v0.8.0)

**Breaking changes:**

- Made CSV write options use chrono formatting by default [\#624](https://github.com/jorgecarleitao/arrow2/issues/624)
- Add `compression` to `IpcWriteOptions` [\#570](https://github.com/jorgecarleitao/arrow2/issues/570)
- Made `cast` accept `CastOptions` parameter [\#569](https://github.com/jorgecarleitao/arrow2/issues/569)
- Simplified `ArrowError` [\#640](https://github.com/jorgecarleitao/arrow2/pull/640) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Use `DynComparator` for `lexsort` and `partition` [\#637](https://github.com/jorgecarleitao/arrow2/pull/637) ([yjshen](https://github.com/yjshen))
- Split "compute" feature [\#634](https://github.com/jorgecarleitao/arrow2/pull/634) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed unneeded trait. [\#628](https://github.com/jorgecarleitao/arrow2/pull/628) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Sealed 2 traits to forbid downstream implementations [\#621](https://github.com/jorgecarleitao/arrow2/pull/621) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified arithmetics compute [\#607](https://github.com/jorgecarleitao/arrow2/pull/607) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored comparison `Operator` [\#604](https://github.com/jorgecarleitao/arrow2/pull/604) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified dictionary indexes [\#584](https://github.com/jorgecarleitao/arrow2/pull/584) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified IPC APIs [\#576](https://github.com/jorgecarleitao/arrow2/pull/576) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified IPC stream writer / remove finish on drop from stream writer [\#575](https://github.com/jorgecarleitao/arrow2/pull/575) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified trait in compute. [\#572](https://github.com/jorgecarleitao/arrow2/pull/572) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Compute: add partial option into CastOptions [\#561](https://github.com/jorgecarleitao/arrow2/pull/561) ([sundy-li](https://github.com/sundy-li))
- Introduced `UnionMode` enum [\#557](https://github.com/jorgecarleitao/arrow2/pull/557) ([simonvandel](https://github.com/simonvandel))
- Changed DataType::FixedSize\*\(i32\) to DataType::FixedSize\*\(usize\) [\#556](https://github.com/jorgecarleitao/arrow2/pull/556) ([simonvandel](https://github.com/simonvandel))

**New features:**

- Added support to write timestamps with timezones for CSV [\#623](https://github.com/jorgecarleitao/arrow2/pull/623) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read Avro files' metadata asynchronously [\#614](https://github.com/jorgecarleitao/arrow2/pull/614) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added iterator for `StructArray` [\#613](https://github.com/jorgecarleitao/arrow2/pull/613) ([illumination-k](https://github.com/illumination-k))
- Added support to read snappy-compressed Avro [\#612](https://github.com/jorgecarleitao/arrow2/pull/612) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read decimal from csv [\#602](https://github.com/jorgecarleitao/arrow2/pull/602) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to cast `NullArray` to all other types [\#589](https://github.com/jorgecarleitao/arrow2/pull/589) ([flaneur2020](https://github.com/flaneur2020))
- Added support dictionaries in nested types over IPC [\#587](https://github.com/jorgecarleitao/arrow2/pull/587) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write Arrow IPC streams asynchronously [\#577](https://github.com/jorgecarleitao/arrow2/pull/577) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write compressed Arrow IPC \(feather v2\) [\#566](https://github.com/jorgecarleitao/arrow2/pull/566) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for ffi for `FixedSizeList` and `FixedSizeBinary` [\#565](https://github.com/jorgecarleitao/arrow2/pull/565) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `async` csv reading. [\#562](https://github.com/jorgecarleitao/arrow2/pull/562) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `bitwise` operations [\#553](https://github.com/jorgecarleitao/arrow2/pull/553) ([1aguna](https://github.com/1aguna))
- Added support to read `StructArray` from parquet [\#547](https://github.com/jorgecarleitao/arrow2/pull/547) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in reading nullable from Avro. [\#631](https://github.com/jorgecarleitao/arrow2/pull/631) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in union FFI [\#625](https://github.com/jorgecarleitao/arrow2/pull/625) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in computing projection in `io::ipc::read::reader::FileReader` [\#596](https://github.com/jorgecarleitao/arrow2/pull/596) ([illumination-k](https://github.com/illumination-k))
- Fixed error in compressing IPC LZ4 [\#593](https://github.com/jorgecarleitao/arrow2/pull/593) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed growable of dictionaries negative keys [\#582](https://github.com/jorgecarleitao/arrow2/pull/582) ([ritchie46](https://github.com/ritchie46))
- Made substring kernel on utf8 take chars into account. [\#568](https://github.com/jorgecarleitao/arrow2/pull/568) ([ritchie46](https://github.com/ritchie46))
- Fixed error in passing sliced arrays via FFI [\#564](https://github.com/jorgecarleitao/arrow2/pull/564) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Faster `take` with null values \(2-3x\) [\#633](https://github.com/jorgecarleitao/arrow2/pull/633) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved error message for missing feature in compressed parquet [\#632](https://github.com/jorgecarleitao/arrow2/pull/632) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `to` conversion to `FixedSizeBinary` [\#622](https://github.com/jorgecarleitao/arrow2/pull/622) ([ritchie46](https://github.com/ritchie46))
- Bumped `confy-table` [\#618](https://github.com/jorgecarleitao/arrow2/pull/618) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `MutableArray` `Send + Sync` [\#617](https://github.com/jorgecarleitao/arrow2/pull/617) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed most of allocations in IPC reading [\#611](https://github.com/jorgecarleitao/arrow2/pull/611) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Speed up boolean comparison kernels \(~3x\) [\#610](https://github.com/jorgecarleitao/arrow2/pull/610) ([Dandandan](https://github.com/Dandandan))
- Improved performance of decimal arithmetics [\#605](https://github.com/jorgecarleitao/arrow2/pull/605) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified traits and added documentation [\#603](https://github.com/jorgecarleitao/arrow2/pull/603) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of `is_not_null`. [\#600](https://github.com/jorgecarleitao/arrow2/pull/600) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `len` to every array [\#599](https://github.com/jorgecarleitao/arrow2/pull/599) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `NullArray` at FFI. [\#598](https://github.com/jorgecarleitao/arrow2/pull/598) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Optimized `MutableBinaryArray` [\#597](https://github.com/jorgecarleitao/arrow2/pull/597) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Speedup/simplify bitwise operations \(avoid extra allocation\) [\#586](https://github.com/jorgecarleitao/arrow2/pull/586) ([Dandandan](https://github.com/Dandandan))
- Improved performance of `bitmap::from_trusted` \(3x\) [\#578](https://github.com/jorgecarleitao/arrow2/pull/578) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made bitmap not cache null count [\#563](https://github.com/jorgecarleitao/arrow2/pull/563) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoided redundant checks in creating an `Utf8Array` from `MutableUtf8Array` [\#560](https://github.com/jorgecarleitao/arrow2/pull/560) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid unnecessary allocations [\#559](https://github.com/jorgecarleitao/arrow2/pull/559) ([simonvandel](https://github.com/simonvandel))
- Surfaced errors in reading from avro [\#558](https://github.com/jorgecarleitao/arrow2/pull/558) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Simplified example [\#619](https://github.com/jorgecarleitao/arrow2/pull/619) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made example of parallel parquet write be over multiple batches [\#544](https://github.com/jorgecarleitao/arrow2/pull/544) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Cleaned up benches [\#636](https://github.com/jorgecarleitao/arrow2/pull/636) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Ignored tests code in coverage report [\#615](https://github.com/jorgecarleitao/arrow2/pull/615) ([yjhmelody](https://github.com/yjhmelody))
- Added more tests [\#601](https://github.com/jorgecarleitao/arrow2/pull/601) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Mitigated `RUSTSEC-2020-0159` [\#595](https://github.com/jorgecarleitao/arrow2/pull/595) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#591](https://github.com/jorgecarleitao/arrow2/pull/591) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.7.0](https://github.com/jorgecarleitao/arrow2/tree/v0.7.0) (2021-10-29)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.6.2...v0.7.0)

**Breaking changes:**

- Simplified reading parquet [\#532](https://github.com/jorgecarleitao/arrow2/pull/532) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Change IPC `FileReader` to own the underlying reader [\#518](https://github.com/jorgecarleitao/arrow2/pull/518) ([blakesmith](https://github.com/blakesmith))
- Migrate to `arrow_format` crate [\#517](https://github.com/jorgecarleitao/arrow2/pull/517) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added read of 2-level nested lists from parquet [\#548](https://github.com/jorgecarleitao/arrow2/pull/548) ([jorgecarleitao](https://github.com/jorgecarleitao))
- add dictionary serialization for csv-writer [\#515](https://github.com/jorgecarleitao/arrow2/pull/515) ([ritchie46](https://github.com/ritchie46))
- Added `checked_negate` and `wrapping_negate` for `PrimitiveArray` [\#506](https://github.com/jorgecarleitao/arrow2/pull/506) ([yjhmelody](https://github.com/yjhmelody))

**Fixed bugs:**

- Fixed error in reading fixed len binary from parquet [\#549](https://github.com/jorgecarleitao/arrow2/pull/549) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed ffi of sliced arrays [\#540](https://github.com/jorgecarleitao/arrow2/pull/540) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed s3 example [\#536](https://github.com/jorgecarleitao/arrow2/pull/536) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in writing compressed parquet dict pages [\#523](https://github.com/jorgecarleitao/arrow2/pull/523) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Validity taken into account when writing `StructArray` to json [\#511](https://github.com/jorgecarleitao/arrow2/pull/511) ([VasanthakumarV](https://github.com/VasanthakumarV))

**Enhancements:**

- Bumped Prost and Tonic [\#550](https://github.com/jorgecarleitao/arrow2/pull/550) ([PsiACE](https://github.com/PsiACE))
- Speedup scalar boolean operations [\#546](https://github.com/jorgecarleitao/arrow2/pull/546) ([Dandandan](https://github.com/Dandandan))
- Added fast path for validating ASCII text \(~1.12-1.89x improvement on reading ASCII parquet data\) [\#542](https://github.com/jorgecarleitao/arrow2/pull/542) ([Dandandan](https://github.com/Dandandan))
- Exposed missing APIs to write parquet in parallel [\#539](https://github.com/jorgecarleitao/arrow2/pull/539) ([jorgecarleitao](https://github.com/jorgecarleitao))
- improve utf8 init validity [\#530](https://github.com/jorgecarleitao/arrow2/pull/530) ([ritchie46](https://github.com/ritchie46))
- export missing `BinaryValueIter` [\#526](https://github.com/jorgecarleitao/arrow2/pull/526) ([yjhmelody](https://github.com/yjhmelody))

**Documentation updates:**

- Added more IPC documentation [\#534](https://github.com/jorgecarleitao/arrow2/pull/534) ([HagaiHargil](https://github.com/HagaiHargil))
- Fixed clippy and fmt [\#521](https://github.com/jorgecarleitao/arrow2/pull/521) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Added more tests for `utf8` [\#543](https://github.com/jorgecarleitao/arrow2/pull/543) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Ignored RUSTSEC-2020-0071 and RUSTSEC-2020-0159 [\#537](https://github.com/jorgecarleitao/arrow2/pull/537) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved parquet read benches [\#533](https://github.com/jorgecarleitao/arrow2/pull/533) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added fmt and clippy checks to CI. [\#522](https://github.com/jorgecarleitao/arrow2/pull/522) ([xudong963](https://github.com/xudong963))

## [v0.6.2](https://github.com/jorgecarleitao/arrow2/tree/v0.6.2) (2021-10-09)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.6.1...v0.6.2)

**New features:**

- Added wrapping version arithmetics for `PrimitiveArray` [\#496](https://github.com/jorgecarleitao/arrow2/pull/496) ([yjhmelody](https://github.com/yjhmelody))

**Fixed bugs:**

- Do not check offsets or utf8 validity in ffi \(\#505\) [\#510](https://github.com/jorgecarleitao/arrow2/pull/510) ([NilsBarlaug](https://github.com/NilsBarlaug))
- Made `try_push_valid` public again [\#509](https://github.com/jorgecarleitao/arrow2/pull/509) ([ritchie46](https://github.com/ritchie46))

**Enhancements:**

- Use static-typed equal functions directly [\#507](https://github.com/jorgecarleitao/arrow2/pull/507) ([yjhmelody](https://github.com/yjhmelody))

## [v0.6.1](https://github.com/jorgecarleitao/arrow2/tree/v0.6.1) (2021-10-07)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.5.3...v0.6.1)

**Breaking changes:**

- Bring `MutableFixedSizeListArray` to the spec used by the rest of the Mutable API [\#475](https://github.com/jorgecarleitao/arrow2/issues/475)
- Removed `ALIGNMENT` invariant from `[Mutable]Buffer` [\#449](https://github.com/jorgecarleitao/arrow2/issues/449)
- Un-nested `compute::arithemtics::basic` [\#461](https://github.com/jorgecarleitao/arrow2/pull/461) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more serialization options for csv writer. [\#453](https://github.com/jorgecarleitao/arrow2/pull/453) ([ritchie46](https://github.com/ritchie46))
- Changed validity from `&Option<Bitmap>` to `Option<&Bitmap>`. [\#431](https://github.com/jorgecarleitao/arrow2/pull/431) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped parquet2 [\#422](https://github.com/jorgecarleitao/arrow2/pull/422) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Changed IPC `FileWriter` to own the `writer`. [\#420](https://github.com/jorgecarleitao/arrow2/pull/420) ([yjshen](https://github.com/yjshen))
- Made `DynComparator` `Send+Sync` [\#414](https://github.com/jorgecarleitao/arrow2/pull/414) ([yjshen](https://github.com/yjshen))

**New features:**

- Read Decimal from Parquet File [\#444](https://github.com/jorgecarleitao/arrow2/issues/444)
- Add IO read for Avro [\#401](https://github.com/jorgecarleitao/arrow2/issues/401)
- Added support to read Avro logical types, `List`,`Enum`, `Duration` and `Fixed`. [\#493](https://github.com/jorgecarleitao/arrow2/pull/493) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added read `Decimal` from parquet [\#489](https://github.com/jorgecarleitao/arrow2/pull/489) ([potter420](https://github.com/potter420))
- Implement `BitXor` trait for `Bitmap` [\#485](https://github.com/jorgecarleitao/arrow2/pull/485) ([houqp](https://github.com/houqp))
- Added `extend`/`extend_unchecked` for `MutableBooleanArray` [\#478](https://github.com/jorgecarleitao/arrow2/pull/478) ([VasanthakumarV](https://github.com/VasanthakumarV))
- expose `shrink_to_fit` to mutable arrays [\#467](https://github.com/jorgecarleitao/arrow2/pull/467) ([ritchie46](https://github.com/ritchie46))
- Added support for `DataType::Map` and `MapArray` [\#464](https://github.com/jorgecarleitao/arrow2/pull/464) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Extract parts of datetime [\#433](https://github.com/jorgecarleitao/arrow2/pull/433) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Added support to add an interval to a timestamp [\#417](https://github.com/jorgecarleitao/arrow2/pull/417) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read Avro. [\#406](https://github.com/jorgecarleitao/arrow2/pull/406) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced own allocator by `std::Vec`. [\#385](https://github.com/jorgecarleitao/arrow2/pull/385) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- crash in parquet read [\#459](https://github.com/jorgecarleitao/arrow2/issues/459)
- Made writing stream to parquet require a non-static lifetime [\#471](https://github.com/jorgecarleitao/arrow2/pull/471) ([GrandChaman](https://github.com/GrandChaman))
- Made importing from FFI `unsafe` [\#458](https://github.com/jorgecarleitao/arrow2/pull/458) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed panic in division using nulls. [\#438](https://github.com/jorgecarleitao/arrow2/pull/438) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error writing dictionary extension to IPC [\#397](https://github.com/jorgecarleitao/arrow2/pull/397) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in extending `MutableBitmap` [\#393](https://github.com/jorgecarleitao/arrow2/pull/393) ([jorgecarleitao](https://github.com/jorgecarleitao))


**Enhancements:**

- Some `compare` function are not exported [\#349](https://github.com/jorgecarleitao/arrow2/issues/349)
- Investigate how to add support for timezones in timestamp [\#23](https://github.com/jorgecarleitao/arrow2/issues/23)
- Made `hash` work for extension type [\#487](https://github.com/jorgecarleitao/arrow2/pull/487) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `extend`/`extend_unchecked` for `MutableBinaryArray` [\#486](https://github.com/jorgecarleitao/arrow2/pull/486) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Improved inference and deserialization of CSV [\#483](https://github.com/jorgecarleitao/arrow2/pull/483) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `GrowableFixedSizeList` and improved `MutableFixedSizeListArray` [\#470](https://github.com/jorgecarleitao/arrow2/pull/470) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutableBitmap::shrink_to_fit` [\#468](https://github.com/jorgecarleitao/arrow2/pull/468) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutableArray::as_box` [\#450](https://github.com/jorgecarleitao/arrow2/pull/450) ([sd2k](https://github.com/sd2k))
- Improved performance of sum aggregation via aligned loads \(-10%\) [\#445](https://github.com/jorgecarleitao/arrow2/pull/445) ([ritchie46](https://github.com/ritchie46))
- Removed `assert` from `MutableBuffer::set_len` [\#443](https://github.com/jorgecarleitao/arrow2/pull/443) ([ritchie46](https://github.com/ritchie46))
- Optimized `null_count` [\#442](https://github.com/jorgecarleitao/arrow2/pull/442) ([ritchie46](https://github.com/ritchie46))
- Improved performance of list iterator \(- 10-20%\) [\#441](https://github.com/jorgecarleitao/arrow2/pull/441) ([ritchie46](https://github.com/ritchie46))
- Improved performance of `PrimitiveGrowable` for nulls \(-10%\) [\#434](https://github.com/jorgecarleitao/arrow2/pull/434) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Allowed accessing validity without importing `Array` [\#432](https://github.com/jorgecarleitao/arrow2/pull/432) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Optimize hashing using `ahash` and `multiversion` \(-30%\) [\#428](https://github.com/jorgecarleitao/arrow2/pull/428) ([Dandandan](https://github.com/Dandandan))
- Improved performance of iterator of `Utf8Array` and `BinaryArray` \(3-4x\) [\#427](https://github.com/jorgecarleitao/arrow2/pull/427) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of utf8 validation of large strings via `simdutf8` \(-40%\) [\#426](https://github.com/jorgecarleitao/arrow2/pull/426) ([Dandandan](https://github.com/Dandandan))
- Added reading of parquet required dictionary-encoded binary. [\#419](https://github.com/jorgecarleitao/arrow2/pull/419) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add `extend`/`extend_unchecked` for `MutableUtf8Array` [\#413](https://github.com/jorgecarleitao/arrow2/pull/413) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Added support to extract hours and years from timestamps with timezone [\#412](https://github.com/jorgecarleitao/arrow2/pull/412) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `io_csv_read` and `io_csv_write` feature [\#408](https://github.com/jorgecarleitao/arrow2/pull/408) ([ritchie46](https://github.com/ritchie46))
- Improve `comparison` docs and re-export the array-comparing function [\#404](https://github.com/jorgecarleitao/arrow2/pull/404) ([HagaiHargil](https://github.com/HagaiHargil))
- Added support to read dict-encoded required primitive types from parquet [\#402](https://github.com/jorgecarleitao/arrow2/pull/402) ([Dandandan](https://github.com/Dandandan))
- Added `Array::with_validity` [\#399](https://github.com/jorgecarleitao/arrow2/pull/399) ([ritchie46](https://github.com/ritchie46))

**Documentation updates:**

- Improved documentation [\#491](https://github.com/jorgecarleitao/arrow2/pull/491) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more API docs. [\#479](https://github.com/jorgecarleitao/arrow2/pull/479) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more documentation [\#476](https://github.com/jorgecarleitao/arrow2/pull/476) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved documentation [\#462](https://github.com/jorgecarleitao/arrow2/pull/462) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added example showing parallel writes to parquet \(x num\_cores\) [\#436](https://github.com/jorgecarleitao/arrow2/pull/436) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved documentation [\#430](https://github.com/jorgecarleitao/arrow2/pull/430) ([jorgecarleitao](https://github.com/jorgecarleitao))
- \[0.5\] The docs `io` module has no submodules [\#390](https://github.com/jorgecarleitao/arrow2/issues/390)
- Made docs be compiled with feature `full` [\#391](https://github.com/jorgecarleitao/arrow2/pull/391) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- DRY via macro. [\#477](https://github.com/jorgecarleitao/arrow2/pull/477) ([jorgecarleitao](https://github.com/jorgecarleitao))
- DRY of type check and len check code in `compute` [\#474](https://github.com/jorgecarleitao/arrow2/pull/474) ([yjhmelody](https://github.com/yjhmelody))
- Added property testing [\#460](https://github.com/jorgecarleitao/arrow2/pull/460) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added fmt to CI. [\#455](https://github.com/jorgecarleitao/arrow2/pull/455) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified CI [\#452](https://github.com/jorgecarleitao/arrow2/pull/452) ([jorgecarleitao](https://github.com/jorgecarleitao))
- fix filter kernels bench [\#440](https://github.com/jorgecarleitao/arrow2/pull/440) ([ritchie46](https://github.com/ritchie46))
- Reduced number of combinations in feature tests. [\#429](https://github.com/jorgecarleitao/arrow2/pull/429) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Move tests from `src/compute/` to `tests/` [\#423](https://github.com/jorgecarleitao/arrow2/pull/423) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Skipped some feature permutations. [\#411](https://github.com/jorgecarleitao/arrow2/pull/411) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added tests to some invariants of `unsafe` [\#403](https://github.com/jorgecarleitao/arrow2/pull/403) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and write extension types to and from parquet [\#396](https://github.com/jorgecarleitao/arrow2/pull/396) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fix testing of SIMD [\#394](https://github.com/jorgecarleitao/arrow2/pull/394) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.3](https://github.com/jorgecarleitao/arrow2/tree/v0.5.3) (2021-09-14)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.5.2...v0.5.3)

**New features:**

- Added support to read and write extension types to and from parquet [\#396](https://github.com/jorgecarleitao/arrow2/pull/396) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error writing dictionary extension to IPC [\#397](https://github.com/jorgecarleitao/arrow2/pull/397) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in extending `MutableBitmap` [\#393](https://github.com/jorgecarleitao/arrow2/pull/393) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added support to read dict-encoded required primitive types from parquet [\#402](https://github.com/jorgecarleitao/arrow2/pull/402) ([Dandandan](https://github.com/Dandandan))
- Added `Array::with_validity` [\#399](https://github.com/jorgecarleitao/arrow2/pull/399) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Fix testing of SIMD [\#394](https://github.com/jorgecarleitao/arrow2/pull/394) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.1](https://github.com/jorgecarleitao/arrow2/tree/v0.5.1) (2021-09-09)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.5.0...v0.5.1)

**Documentation updates:**

- \[0.5\] The docs `io` module has no submodules [\#390](https://github.com/jorgecarleitao/arrow2/issues/390)
- Made docs be compiled with feature `full` [\#391](https://github.com/jorgecarleitao/arrow2/pull/391) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.0](https://github.com/jorgecarleitao/arrow2/tree/v0.5.0) (2021-09-07)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.4.0...v0.5.0)

**Breaking changes:**

- Added `Extension` to `DataType` [\#361](https://github.com/jorgecarleitao/arrow2/issues/361)
- `MonthDayNano` added to enum `IntervalUnit` [\#360](https://github.com/jorgecarleitao/arrow2/issues/360)
- Make `io::parquet::write::write_*` return size of file in bytes [\#354](https://github.com/jorgecarleitao/arrow2/issues/354)
- Renamed `bitmap::utils::null_count` to `bitmap::utils::count_zeros` [\#342](https://github.com/jorgecarleitao/arrow2/issues/342)
- Made `GroupFilter` optional in parquet's`RecordReader` and added method to set it. [\#386](https://github.com/jorgecarleitao/arrow2/pull/386) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `PartialOrd` and `Ord` of all enums in `datatypes` [\#379](https://github.com/jorgecarleitao/arrow2/pull/379) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `cargo` features not default [\#369](https://github.com/jorgecarleitao/arrow2/pull/369) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Prepare APIs for extension types [\#357](https://github.com/jorgecarleitao/arrow2/pull/357) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support for `async` parquet write [\#372](https://github.com/jorgecarleitao/arrow2/pull/372) ([GrandChaman](https://github.com/GrandChaman))
- Add support to extension types in FFI [\#363](https://github.com/jorgecarleitao/arrow2/pull/363) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for field's metadata via FFI [\#362](https://github.com/jorgecarleitao/arrow2/pull/362) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `Extension` \(logical\) type [\#359](https://github.com/jorgecarleitao/arrow2/pull/359) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for compute to `BinaryArray` [\#346](https://github.com/jorgecarleitao/arrow2/pull/346) ([zhyass](https://github.com/zhyass))
- Added support for reading binary from CSV [\#337](https://github.com/jorgecarleitao/arrow2/pull/337) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `MONTH_DAY_NANO` interval type [\#268](https://github.com/jorgecarleitao/arrow2/pull/268) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Parquet read skips a few rows at the end of the page [\#373](https://github.com/jorgecarleitao/arrow2/issues/373)
- `parquet_read` fails when a column has too many rows with string values [\#366](https://github.com/jorgecarleitao/arrow2/issues/366)
- `parquet_read` panics with `index_out_of_bounds` [\#351](https://github.com/jorgecarleitao/arrow2/issues/351)
- Fixed error in `MutableBitmap::push_unchecked` [\#384](https://github.com/jorgecarleitao/arrow2/pull/384) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed display of timestamp with tz. [\#375](https://github.com/jorgecarleitao/arrow2/pull/375) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added `extend_*values` to `MutablePrimitiveArray` [\#383](https://github.com/jorgecarleitao/arrow2/pull/383) ([ritchie46](https://github.com/ritchie46))
- Improved performance of writing to CSV \(20-25%\) [\#382](https://github.com/jorgecarleitao/arrow2/pull/382) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped `lexical-core` [\#378](https://github.com/jorgecarleitao/arrow2/pull/378) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed casting of utf8 \<\> Timestamp with and without timezone [\#376](https://github.com/jorgecarleitao/arrow2/pull/376) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `Send+Sync` to `MutableBuffer` [\#368](https://github.com/jorgecarleitao/arrow2/pull/368) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of unary \_not\_ for aligned bitmaps \(3x\) [\#365](https://github.com/jorgecarleitao/arrow2/pull/365) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced dependencies within `num` [\#353](https://github.com/jorgecarleitao/arrow2/pull/353) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped to parquet2 v0.4 [\#352](https://github.com/jorgecarleitao/arrow2/pull/352) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped tonic and prost in flight [\#344](https://github.com/jorgecarleitao/arrow2/pull/344) ([PsiACE](https://github.com/PsiACE))
- Improved null count calculation \(5x\) [\#343](https://github.com/jorgecarleitao/arrow2/pull/343) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved perf of deserializing integers from json \(30%\) [\#340](https://github.com/jorgecarleitao/arrow2/pull/340) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code of json schema inference [\#339](https://github.com/jorgecarleitao/arrow2/pull/339) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Moved guide examples to examples/ [\#387](https://github.com/jorgecarleitao/arrow2/pull/387) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more docs. [\#358](https://github.com/jorgecarleitao/arrow2/pull/358) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved API docs. [\#355](https://github.com/jorgecarleitao/arrow2/pull/355) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Moved tests to `tests/` [\#389](https://github.com/jorgecarleitao/arrow2/pull/389) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved compute tests to tests/ [\#388](https://github.com/jorgecarleitao/arrow2/pull/388) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests. [\#380](https://github.com/jorgecarleitao/arrow2/pull/380) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Pinned nightly in SIMD tests [\#364](https://github.com/jorgecarleitao/arrow2/pull/364) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved benches for take [\#348](https://github.com/jorgecarleitao/arrow2/pull/348) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IPC integration tests run tests that are not run by arrow-rs [\#278](https://github.com/jorgecarleitao/arrow2/pull/278) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.4.0](https://github.com/jorgecarleitao/arrow2/tree/v0.4.0) (2021-08-24)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.3.0...v0.4.0)

**Breaking changes:**

- Change dictionary iterator of values from `Array`s of one element to `Scalar`s [\#335](https://github.com/jorgecarleitao/arrow2/issues/335)
- Align FFI API with arrow's C++ API [\#328](https://github.com/jorgecarleitao/arrow2/issues/328)
- Make `*_compare_scalar` not return `Result` [\#316](https://github.com/jorgecarleitao/arrow2/issues/316)
- Make `io::print`, `get_value_display` and `get_display` not return `Result` [\#286](https://github.com/jorgecarleitao/arrow2/issues/286)
- Add `MetadataVersion` to IPC interfaces [\#282](https://github.com/jorgecarleitao/arrow2/issues/282)
- Change `DataType::Union` to enable round trips in IPC [\#281](https://github.com/jorgecarleitao/arrow2/issues/281)
- Removed clone requirement in `StructArray -> RecordBatch` [\#307](https://github.com/jorgecarleitao/arrow2/pull/307) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in reading a non-finished IPC stream. [\#302](https://github.com/jorgecarleitao/arrow2/pull/302) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Generalized ZipIterator to accept a `BitmapIter` [\#296](https://github.com/jorgecarleitao/arrow2/pull/296) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added API to FFI `Field` [\#321](https://github.com/jorgecarleitao/arrow2/pull/321) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `compare_scalar` [\#317](https://github.com/jorgecarleitao/arrow2/pull/317) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add `UnionArray` [\#283](https://github.com/jorgecarleitao/arrow2/pull/283) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- SliceIterator of last bytes is not correct [\#292](https://github.com/jorgecarleitao/arrow2/issues/292)
- Fixed error in displaying dictionaries with nulls in values [\#334](https://github.com/jorgecarleitao/arrow2/pull/334) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in dict equality [\#333](https://github.com/jorgecarleitao/arrow2/pull/333) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed small inconsistencies between `compute::cast` and `compute::can_cast` [\#295](https://github.com/jorgecarleitao/arrow2/pull/295) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed order implementation for `days_ms` / `Interval(DayTime)` [\#285](https://github.com/jorgecarleitao/arrow2/pull/285) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added support for remaining non-nested datatypes [\#336](https://github.com/jorgecarleitao/arrow2/pull/336) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `multiversion` and `lexical-core` optional [\#324](https://github.com/jorgecarleitao/arrow2/pull/324) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of utf8 comparison \(1.7x-4x\) [\#322](https://github.com/jorgecarleitao/arrow2/pull/322) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of boolean comparison \(5x-14x\) [\#318](https://github.com/jorgecarleitao/arrow2/pull/318) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added trait `TryPush` [\#314](https://github.com/jorgecarleitao/arrow2/pull/314) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added cast `date32 -> i64` and `date64 -> i32` [\#308](https://github.com/jorgecarleitao/arrow2/pull/308) ([ritchie46](https://github.com/ritchie46))
- Improved performance of comparison with SIMD feature flag \(2x-3.5x\) [\#305](https://github.com/jorgecarleitao/arrow2/pull/305) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read json to `BinaryArray` [\#304](https://github.com/jorgecarleitao/arrow2/pull/304) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutableFixedSizeBinaryArray` [\#303](https://github.com/jorgecarleitao/arrow2/pull/303) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutablePrimitiveArray` and `MutableUtf8Array` [\#299](https://github.com/jorgecarleitao/arrow2/pull/299) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutableBooleanArray` [\#297](https://github.com/jorgecarleitao/arrow2/pull/297) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of concatenating non-aligned validities \(15x\) [\#291](https://github.com/jorgecarleitao/arrow2/pull/291) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for timestamps with tz and interval to `io::print::write` [\#287](https://github.com/jorgecarleitao/arrow2/pull/287) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved debug repr of buffers and bitmaps. [\#284](https://github.com/jorgecarleitao/arrow2/pull/284) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Cleaned up internals of json integration [\#280](https://github.com/jorgecarleitao/arrow2/pull/280) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `serde_derive` dependency [\#279](https://github.com/jorgecarleitao/arrow2/pull/279) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified IPC code. [\#277](https://github.com/jorgecarleitao/arrow2/pull/277) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced dependencies from confi-table and enabled `wasm` on `io_print` feature. [\#276](https://github.com/jorgecarleitao/arrow2/pull/276) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improve performance of `rem_scalar/div_scalar` for integer types \(4x-10x\) [\#275](https://github.com/jorgecarleitao/arrow2/pull/275) ([ritchie46](https://github.com/ritchie46))

**Documentation updates:**

- Cleaned examples and docs from old API. [\#330](https://github.com/jorgecarleitao/arrow2/pull/330) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved documentation [\#306](https://github.com/jorgecarleitao/arrow2/pull/306) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Improved naming of testing workflows [\#315](https://github.com/jorgecarleitao/arrow2/pull/315) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added tests to scalar API [\#300](https://github.com/jorgecarleitao/arrow2/pull/300) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made CSV and JSON tests not use files. [\#290](https://github.com/jorgecarleitao/arrow2/pull/290) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved tests to integration tests [\#289](https://github.com/jorgecarleitao/arrow2/pull/289) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Closed issues:**

- Make parquet\_read\_record support async [\#331](https://github.com/jorgecarleitao/arrow2/issues/331)
- Panic due to SIMD comparison [\#312](https://github.com/jorgecarleitao/arrow2/issues/312)
- Bitmap::mutable line 155 may Panic/segfault [\#309](https://github.com/jorgecarleitao/arrow2/issues/309)
- IPC's `StreamReader` may abort due to excessive memory by overflowing a `usize`d variable [\#301](https://github.com/jorgecarleitao/arrow2/issues/301)
- Improve performance of `rem_scalar/div_scalar` for integer types \(4x-10x\) [\#259](https://github.com/jorgecarleitao/arrow2/issues/259)

## [v0.3.0](https://github.com/jorgecarleitao/arrow2/tree/v0.3.0) (2021-08-11)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.2.0...v0.3.0)

**Breaking changes:**

- Renamed `sum` to `sum_primitive` [\#273](https://github.com/jorgecarleitao/arrow2/issues/273)
- Moved trait `Index` from `array::Index` to `types::Index` [\#272](https://github.com/jorgecarleitao/arrow2/issues/272)
- Added optional `projection` to IPC FileReader [\#271](https://github.com/jorgecarleitao/arrow2/issues/271)
- Added optional `page_filter` to parquet's `RecordReader` and `get_page_iterator` [\#270](https://github.com/jorgecarleitao/arrow2/issues/270)
- Renamed parquets' `CompressionCodec` to `Compression` [\#269](https://github.com/jorgecarleitao/arrow2/issues/269)

**New features:**

- Added support for FFI of dictionary-encoded arrays [\#267](https://github.com/jorgecarleitao/arrow2/pull/267) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for projection pushdown on IPC files [\#264](https://github.com/jorgecarleitao/arrow2/pull/264) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read parquet asynchronously [\#260](https://github.com/jorgecarleitao/arrow2/pull/260) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to filter parquet pages. [\#256](https://github.com/jorgecarleitao/arrow2/pull/256) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added wrapping\_cast to cast kernels [\#254](https://github.com/jorgecarleitao/arrow2/pull/254) ([sundy-li](https://github.com/sundy-li))
- Added support to parquet IO on wasm32 [\#239](https://github.com/jorgecarleitao/arrow2/pull/239) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to round-trip dictionary arrays on parquet [\#232](https://github.com/jorgecarleitao/arrow2/pull/232) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added Scalar API [\#56](https://github.com/jorgecarleitao/arrow2/pull/56) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in computing remainder of chunk iterator [\#262](https://github.com/jorgecarleitao/arrow2/pull/262) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in slicing bitmap. [\#250](https://github.com/jorgecarleitao/arrow2/pull/250) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Improve the performance in cast kernel using AsPrimitive trait in generic dispatch [\#252](https://github.com/jorgecarleitao/arrow2/issues/252)
- Poor performance in `sort::sort_to_indices`  with limit option in arrow2 [\#245](https://github.com/jorgecarleitao/arrow2/issues/245)
- Support loading Feather v2 \(IPC\) files with more than 1 million tables [\#231](https://github.com/jorgecarleitao/arrow2/issues/231)
- Migrated to parquet2 v0.3 [\#265](https://github.com/jorgecarleitao/arrow2/pull/265) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests to cast and min/max [\#253](https://github.com/jorgecarleitao/arrow2/pull/253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Prettytable is unmaintained. Change to comfy-table [\#251](https://github.com/jorgecarleitao/arrow2/pull/251) ([PsiACE](https://github.com/PsiACE))
- Added IndexRange to remove checks in hot loops [\#247](https://github.com/jorgecarleitao/arrow2/pull/247) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Make merge\_sort\_slices MergeSortSlices public [\#243](https://github.com/jorgecarleitao/arrow2/pull/243) ([sundy-li](https://github.com/sundy-li))

**Documentation updates:**

- Added example and guide section on compute [\#242](https://github.com/jorgecarleitao/arrow2/pull/242) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Closed issues:**

- Allow projection pushdown to IPC files [\#261](https://github.com/jorgecarleitao/arrow2/issues/261)
- Add support to write dictionary-encoded pages [\#211](https://github.com/jorgecarleitao/arrow2/issues/211)
- Make IpcWriteOptions easier to find. [\#120](https://github.com/jorgecarleitao/arrow2/issues/120)

## [v0.2.0](https://github.com/jorgecarleitao/arrow2/tree/v0.2.0) (2021-07-30)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.1.0...v0.2.0)

**Breaking changes:**

- Simplified `new` signature of growable API [\#238](https://github.com/jorgecarleitao/arrow2/pull/238) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add support to merge sort with a limit [\#222](https://github.com/jorgecarleitao/arrow2/pull/222) ([sundy-li](https://github.com/sundy-li))
- Generalized sort to accept indices other than i32. [\#220](https://github.com/jorgecarleitao/arrow2/pull/220) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for limited sort [\#218](https://github.com/jorgecarleitao/arrow2/pull/218) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Merge sort support limit option [\#221](https://github.com/jorgecarleitao/arrow2/issues/221)
- Introduce limit option to sort [\#215](https://github.com/jorgecarleitao/arrow2/issues/215)
- Added support for take of interval of days\_ms [\#219](https://github.com/jorgecarleitao/arrow2/pull/219) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added FFI for remaining types [\#213](https://github.com/jorgecarleitao/arrow2/pull/213) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Filter operation on sliced utf8 arrays are incorrect [\#233](https://github.com/jorgecarleitao/arrow2/issues/233)
- Fixed error in slicing bitmap. [\#237](https://github.com/jorgecarleitao/arrow2/pull/237) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed nested FFI. [\#212](https://github.com/jorgecarleitao/arrow2/pull/212) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Avoid materialization of indices in filter\_record\_batch for single arrays [\#234](https://github.com/jorgecarleitao/arrow2/issues/234)
- Add integration tests for writing to parquet [\#80](https://github.com/jorgecarleitao/arrow2/issues/80)
- Short-circuited boolean evaluation in GrowableList [\#228](https://github.com/jorgecarleitao/arrow2/pull/228) ([ritchie46](https://github.com/ritchie46))
- Add extra inlining to speed up take [\#226](https://github.com/jorgecarleitao/arrow2/pull/226) ([Dandandan](https://github.com/Dandandan))
- Removed un-needed `unsafe` [\#225](https://github.com/jorgecarleitao/arrow2/pull/225) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Add documentation to guide [\#96](https://github.com/jorgecarleitao/arrow2/issues/96)
- Add git submodule command to correct the test doc [\#223](https://github.com/jorgecarleitao/arrow2/pull/223) ([sundy-li](https://github.com/sundy-li))
- Added badges to README [\#216](https://github.com/jorgecarleitao/arrow2/pull/216) ([sundy-li](https://github.com/sundy-li))
- Clarified differences with arrow crate [\#210](https://github.com/jorgecarleitao/arrow2/pull/210) ([alamb](https://github.com/alamb))
- Clarified differences with arrow crate [\#209](https://github.com/jorgecarleitao/arrow2/pull/209) ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
