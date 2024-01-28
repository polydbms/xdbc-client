#!/bin/bash

#filename="test_10000000.csv"
filename="lineitem_sf10.csv"
#parameter: $1 rebuild containers
#pkill -f "bash /home/harry-ldap/xdbc/xdbc-client/experiments/prepare/restart.sh 1"
if [ "$1" = "1" ]; then
  cd ~/xdbc/xdbc-client/ && docker-compose -f docker-xdbc.yml down
  docker container prune -f
  dangling_image_ids=$(docker images -q --filter "dangling=true")
  for image_id in $dangling_image_ids; do
    docker rmi "$image_id"
  done
  echo "Rebuilding..."
  cd ~/xdbc/xdbc-client && make &
  cd ~/xdbc/xdbc-server && make &
  wait
  cd ~/xdbc/xdbc-client/ && docker-compose -f docker-xdbc.yml up -d
  docker exec xdbcclient bash -c "yes | apt-get install plocate && updatedb"
else
  echo "Not rebuilding."
  cd ~/xdbc/xdbc-client/
  docker-compose -f docker-xdbc.yml down
  docker-compose -f docker-xdbc.yml up -d
  docker exec xdbcclient bash -c "yes | apt-get install plocate && updatedb"
fi

cd ~/xdbc/xdbc-client/ && docker-compose -f docker-tc.yml down
cd ~/xdbc/xdbc-client/ && docker-compose -f docker-tc.yml up -d

rm -f /dev/shm/lineitem_sf10_*.csv
rm -f /dev/shm/test_10000000_*.csv
#docker exec xdbcserver bash -c "rm -f /dev/shm/lineitem_sf10_*.csv"
docker exec xdbcclient bash -c "rm -f /dev/shm/output*.csv"
cp ~/lineitem_sf10.csv /dev/shm/lineitem_sf10.csv
cp ~/test_10000000.csv /dev/shm/test_10000000.csv