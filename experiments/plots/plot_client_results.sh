#!/bin/bash
#work in progress
docker cp xdbcclient:/xdbc-client/tests/build/xdbcclient.txt .
python plot_xdbc_client_log.py xdbcclient.txt