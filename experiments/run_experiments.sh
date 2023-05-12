#!/bin/bash
set -x

#$1 absolute server path dir
SERVER_PATH=$1
#$2 absolute client path dir
CLIENT_PATH=$2
#$3 rebuild
BUILD_OPT=$3

if [[ $BUILD_OPT == 1 ]]; then
  bash $SERVER_PATH/build_and_start.sh xdbcserver 3
  bash $CLIENT_PATH/build_and_start.sh xdbcclient 3
fi

#mkdir -p ${EXECLOG_DIR}
TS=$(date +%s)
EXECLOG=measurements/${TS}_runs_comp.csv
echo "date,comp,parallelism,network,time" >$EXECLOG

#for COMP in nocomp zstd snappy lzo lz4; do
for NETWORK in 25; do
  for COMP in nocomp snappy lzo lz4; do
    for PAR in 4; do

      echo "Running compression: $COMP, parallelism: $PAR, network: $NETWORK"

      curl -d'rate='$NETWORK'mbps' localhost:4080/xdbcclient

      bash $SERVER_PATH/build_and_start.sh xdbcserver 2 -c$COMP -P$PAR &
      SERVER_PID=$!
      sleep 2
      SECONDS=0
      bash $CLIENT_PATH/build_and_start.sh xdbcclient 2
      ELAPSED_SEC=$SECONDS
      echo "$(date +%F),$COMP,$PAR,$NETWORK,$ELAPSED_SEC" >>$EXECLOG
      echo "$(date +%F),$COMP,$PAR,$NETWORK,$ELAPSED_SEC"
      kill $SERVER_PID
    done
  done
done
