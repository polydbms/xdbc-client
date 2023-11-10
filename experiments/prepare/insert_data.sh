#!/bin/bash

#docker cp /home/harry-ldap/test_10000000.csv pg1:/tmp/

#docker cp /home/harry-ldap/xdbc/xdbc-client/tests/helper_scripts/create_postgres_test_table.sql pg1:/tmp/

#docker exec pg1 bash -c "psql -U postgres -d db1 -a -f /tmp/create_test_table.sql"

docker cp /home/harry-ldap/test_10000000.csv ch:/tmp/
docker cp /home/harry-ldap/xdbc/xdbc-client/tests/helper_scripts/create_clickhouse_test_table.sql ch:/tmp/

docker exec ch bash -c "clickhouse-client --multiquery < /tmp/create_clickhouse_test_table.sql"