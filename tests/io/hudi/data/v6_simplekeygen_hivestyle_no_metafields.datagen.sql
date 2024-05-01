CREATE TABLE v6_simplekeygen_hivestyle_no_metafields (
    id INT,
    name STRING,
    isActive BOOLEAN,
    shortField SHORT,
    intField INT,
    longField LONG,
    floatField FLOAT,
    doubleField DOUBLE,
    decimalField DECIMAL(10,5),
    dateField DATE,
    timestampField TIMESTAMP,
    binaryField BINARY,
    arrayField ARRAY<STRUCT<arr_struct_f1: STRING, arr_struct_f2: INT>>,  -- Array of structs
    mapField MAP<STRING, STRUCT<map_field_value_struct_f1: DOUBLE, map_field_value_struct_f2: BOOLEAN>>,  -- Map with struct values
    structField STRUCT<
        field1: STRING,
        field2: INT,
        child_struct: STRUCT<
            child_field1: DOUBLE,
            child_field2: BOOLEAN
        >
    >,
    byteField BYTE
)
USING HUDI
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'longField',
    'hoodie.metadata.enable' = 'false',
    'hoodie.datasource.write.hive_style_partitioning' = 'true',
    'hoodie.datasource.write.drop.partition.columns' = 'false',
    'hoodie.populate.meta.fields' = 'false'
)
PARTITIONED BY (byteField);

INSERT INTO v6_simplekeygen_hivestyle_no_metafields VALUES
(1, 'Alice', false, 300, 15000, 1234567890, 1.0, 3.14159, 12345.67890, CAST('2023-04-01' AS DATE), CAST('2023-04-01 12:01:00' AS TIMESTAMP), CAST('binary data' AS BINARY),
    ARRAY(STRUCT('red', 100), STRUCT('blue', 200), STRUCT('green', 300)),
    MAP('key1', STRUCT(123.456, true), 'key2', STRUCT(789.012, false)),
    STRUCT('Alice', 30, STRUCT(123.456, true)),
    10
),
(2, 'Bob', false, 100, 25000, 9876543210, 2.0, 2.71828, 67890.12345, CAST('2023-04-02' AS DATE), CAST('2023-04-02 13:02:00' AS TIMESTAMP), CAST('more binary data' AS BINARY),
    ARRAY(STRUCT('yellow', 400), STRUCT('purple', 500)),
    MAP('key3', STRUCT(234.567, true), 'key4', STRUCT(567.890, false)),
    STRUCT('Bob', 40, STRUCT(789.012, false)),
    20
),
(3, 'Carol', true, 200, 35000, 1928374650, 3.0, 1.41421, 11111.22222, CAST('2023-04-03' AS DATE), CAST('2023-04-03 14:03:00' AS TIMESTAMP), CAST('even more binary data' AS BINARY),
    ARRAY(STRUCT('black', 600), STRUCT('white', 700), STRUCT('pink', 800)),
    MAP('key5', STRUCT(345.678, true), 'key6', STRUCT(654.321, false)),
    STRUCT('Carol', 25, STRUCT(456.789, true)),
    10
),
(4, 'Diana', true, 500, 45000, 987654321, 4.0, 2.468, 65432.12345, CAST('2023-04-04' AS DATE), CAST('2023-04-04 15:04:00' AS TIMESTAMP), CAST('new binary data' AS BINARY),
    ARRAY(STRUCT('orange', 900), STRUCT('gray', 1000)),
    MAP('key7', STRUCT(456.789, true), 'key8', STRUCT(123.456, false)),
    STRUCT('Diana', 50, STRUCT(987.654, true)),
    30
);
