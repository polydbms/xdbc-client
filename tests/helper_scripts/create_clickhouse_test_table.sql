DROP TABLE IF EXISTS test_10000000;
CREATE TABLE IF NOT EXISTS test_10000000
(
    l_orderkey      Int64,
    l_partkey       Int32,
    l_suppkey       Int32,
    l_linenumber    Int32,
    l_quantity      Decimal(12, 2),
    l_extendedprice Decimal(12, 2),
    l_discount      Decimal(12, 2),
    l_tax           Decimal(12, 2)
)
ENGINE = MergeTree
PRIMARY KEY(l_orderkey);

INSERT INTO test_10000000 FROM INFILE '/tmp/test_10000000.csv' FORMAT CSV;

