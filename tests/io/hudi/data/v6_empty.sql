create table v6_empty (
                          id INT,
                          name STRING,
                          isActive BOOLEAN
)
    USING HUDI
    TBLPROPERTIES (
        type = 'cow',
        primaryKey = 'id',
        'hoodie.metadata.enable' = 'false'
);
