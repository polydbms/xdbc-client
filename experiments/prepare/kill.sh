#!/bin/bash
set -x

docker exec xdbcserver bash -c "pids=\$(pgrep xdbc-server); if [ \"\$pids\" ]; then kill \$pids; fi"
docker exec xdbcclient bash -c "pids=\$(pgrep xdbc-client); if [ \"\$pids\" ]; then kill \$pids; fi"
