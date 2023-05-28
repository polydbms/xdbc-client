#!/bin/bash
#set -x

#$1 absolute server path dir
SERVER_PATH=$1
#$2 absolute client path dir
CLIENT_PATH=$2
#$3, $4 rebuild
BUILD_OPT_SERVER=$3
BUILD_OPT_CLIENT=$4

if [[ $BUILD_OPT_SERVER == 1 ]]; then
  bash $SERVER_PATH/build_and_start.sh xdbcserver 3
fi
if [[ $BUILD_OPT_CLIENT == 1 ]]; then
  bash $CLIENT_PATH/build_and_start.sh xdbcclient 3

fi

#mkdir -p ${EXECLOG_DIR}
TS=$(date +%s)
EXECLOG=measurements/${TS}_runs_comp.csv
echo "date,comp,parallelism,cpu,network,time" >$EXECLOG

for CPUS in 7; do
  docker update --cpus $CPUS xdbcclient
  for NETWORK in 100; do
    #for COMP in nocomp snappy lzo lz4 zlib; do
    for COMP in zlib; do
      for RPAR in 16; do
        for NPAR in 16; do
          for FORMAT in 2; do
            for BUFFPOOLSIZE in 10000; do
              for BUFFSIZE in 10000; do

                echo "Running cpus: $CPUS, network: $NETWORK, compression: $COMP, rparallelism: $RPAR, nparallelism: $NPAR, format: $FORMAT, bufferpool_size $BUFFPOOLSIZE, buffer_size: $BUFFSIZE"

                curl -d'rate='$NETWORK'mbps' localhost:4080/xdbcclient

                bash $SERVER_PATH/build_and_start.sh xdbcserver 2 "-c$COMP --read-parallelism=$RPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s2" &
                SERVER_PID=$!

                sleep 1
                SECONDS=0
                bash $CLIENT_PATH/build_and_start.sh xdbcclient 2 "-f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -P$NPAR -s2"
                ELAPSED_SEC=$SECONDS
                echo "$(date +%F),$COMP,$PAR,$CPUS,$NETWORK,$ELAPSED_SEC" >>$EXECLOG
                echo "$(date +%F),$COMP,$PAR,$CPUS,$NETWORK,$ELAPSED_SEC"
                kill $SERVER_PID
              done
            done
          done
        done
      done
    done
  done
done
