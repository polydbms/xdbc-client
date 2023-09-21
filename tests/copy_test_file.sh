#!/bin/bash

docker cp pg1:/tmp/test_10000000.csv /tmp/ && docker cp /tmp/test_10000000.csv xdbcserver:/tmp/ && docker exec -it xdbcserver bash -c "mv /tmp/test_10000000.csv /dev/shm"
