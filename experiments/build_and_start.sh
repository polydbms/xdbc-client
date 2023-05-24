#!/bin/bash
#set -x
#params
#$1 docker container name
CONTAINER=$1
#$2 1: build & run, 2: only run, 3: only build
OPTION=$2
#$3 run params (for now compression library)
RUNPARAMS=$3

if [ $OPTION == 1 ] || [ $OPTION == 3 ]; then
  DIR=$(dirname $(dirname "$(realpath -- "$0")"))
  docker exec $CONTAINER bash -c "rm -rf xdbc-client && mkdir xdbc-client"
  #copy dirs
  for file in xdbc tests; do
    docker cp ${DIR}/$file/ $CONTAINER:/xdbc-client/
  done
  docker cp ${DIR}/CMakeLists.txt $CONTAINER:/xdbc-client/

  #build & install
  docker exec -it $CONTAINER bash -c "cd xdbc-client/ && rm -rf build/ && mkdir build && cd build && cmake .. && make -j8 && make install"
  docker exec -it $CONTAINER bash -c "cd xdbc-client/tests && rm -rf build/ && mkdir build && cd build && cmake .. && make -j8"

fi

# start
if [[ $OPTION != 3 ]]; then
  docker exec -it $CONTAINER bash -c "cd xdbc-client/tests/build && ./test_xclient ${RUNPARAMS}"
fi
