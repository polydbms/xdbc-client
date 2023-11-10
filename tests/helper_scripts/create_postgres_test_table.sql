DROP TABLE IF EXISTS test_10000000;
CREATE TABLE test_10000000
(
    l_orderkey      BIGINT,
    l_partkey       INTEGER,
    l_suppkey       INTEGER,
    l_linenumber    INTEGER,
    l_quantity      DECIMAL(12, 2),
    l_extendedprice DECIMAL(12, 2),
    l_discount      DECIMAL(12, 2),
    l_tax           DECIMAL(12, 2)
);
COPY test_10000000 FROM '/tmp/test_10000000.csv' DELIMITER ',' CSV;
