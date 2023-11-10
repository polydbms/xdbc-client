#!/bin/bash
cd xdbc/xdbc-client/ && docker-compose -f docker-xdbc.yml restart && docker cp ~/test_10000000.csv xdbcserver:/tmp/ && docker exec xdbcserver bash -c "mv /tmp/test_10000000.csv /dev/shm"