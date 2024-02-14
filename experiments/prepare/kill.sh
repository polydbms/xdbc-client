#!/bin/bash
set -x

docker exec xdbcserver bash -c "pids=\$(pgrep xdbc-server); if [ \"\$pids\" ]; then kill \$pids; fi"
docker exec xdbcclient bash -c "pids=\$(pgrep xdbc-client); if [ \"\$pids\" ]; then kill \$pids; fi"

cd ~/xdbc/xdbc-client/ && docker-compose -f docker-xdbc.yml down
cd ~/xdbc/xdbc-client/ && docker-compose -f docker-tc.yml down