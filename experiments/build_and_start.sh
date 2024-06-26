#!/bin/bash
#set -x
#params
#$1 docker container name
CONTAINER=$1
#$2 1: build & run, 2: only run, 3: only build
OPTION=$2
#$3 run params (for now compression library)
RUNPARAMS=$3
INTERACTIVE=$4
IT=""

if [[ $INTERACTIVE == "" ]]; then
  IT="-it"
fi

#copy files & build
if [ $OPTION == 1 ] || [ $OPTION == 3 ]; then
  DIR=$(dirname $(dirname "$(realpath -- "$0")"))
  docker exec $CONTAINER bash -c "rm -rf xdbc-client && mkdir xdbc-client"
  #copy dirs
  for file in xdbc tests; do
    docker cp ${DIR}/$file/ $CONTAINER:/xdbc-client/
  done
  docker cp ${DIR}/CMakeLists.txt $CONTAINER:/xdbc-client/

  #build & install
  docker exec $IT $CONTAINER bash -c "cd xdbc-client/ && rm -rf build/ && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j8 && make install"
  docker exec $IT $CONTAINER bash -c "cd xdbc-client/tests && rm -rf build/ && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j8"

fi

rm /tmp/client_exec.log
# run
if [[ $OPTION != 3 ]]; then
  #docker exec $IT $CONTAINER bash -c "cd xdbc-client/tests/build && ./test_xclient ${RUNPARAMS}"
  docker exec $IT $CONTAINER bash -c "cd xdbc-client/tests/build && ./test_xclient ${RUNPARAMS}" 2>&1 | tee /tmp/client_exec.log
fi
