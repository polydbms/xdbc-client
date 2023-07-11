#!/bin/bash
#set -x

#$1 absolute server path dir
SERVER_PATH=$1
#$2 absolute client path dir
CLIENT_PATH=$2
#$3, $4 rebuild
BUILD_OPT_SERVER=$3
BUILD_OPT_CLIENT=$4

#reset container cpu
docker update --cpus 7 xdbcclient
docker update --cpus 7 xdbcserver

if [[ $BUILD_OPT_SERVER == 1 ]]; then
  bash $SERVER_PATH/build_and_start.sh xdbcserver 3
fi
if [[ $BUILD_OPT_CLIENT == 1 ]]; then
  bash $CLIENT_PATH/build_and_start.sh xdbcclient 3

fi

#mkdir -p ${EXECLOG_DIR}
TS=$(date +%s)
EXECLOG=measurements/${TS}_runs_comp.csv
echo "date,comp,rparallelism,npar,cpu,network,format,bpsize,bsize,time,datasize" >$EXECLOG

#systems=("postgres" "clickhouse")
systems=("postgres")
#tables=("test_10000000" "test_1000000")
tables=("test_10000000")
#cpus=(.2 7)
cpus=(7)
#networks=(100 50 25 13 6)
networks=(100)
#comps=("nocomp" "zstd" "snappy" "lzo" "lz4" "zlib" "cols")
comps=("nocomp" "zstd" "cols")
#rpars=(1 2 4 8 16)
rpars=(4)
#npars=(1 2 4 8 16)
npars=(1)
#formats=(1 2)
formats=(2)
bufpoolsizes=(1000)
#bufpoolsizes=(1000)
#buffsizes=(1000 10000)
buffsizes=(10000)

for SYS in "${systems[@]}"; do
  for TBL in "${tables[@]}"; do
    for CPUS in "${cpus[@]}"; do
      docker update --cpus $CPUS xdbcclient
      for NETWORK in "${networks[@]}"; do
        for COMP in "${comps[@]}"; do
          for RPAR in "${rpars[@]}"; do
            for NPAR in "${npars[@]}"; do
              for FORMAT in "${formats[@]}"; do
                for BUFFPOOLSIZE in "${bufpoolsizes[@]}"; do
                  for BUFFSIZE in "${buffsizes[@]}"; do

                    echo "Running cpus: $CPUS, network: $NETWORK, compression: $COMP, rparallelism: $RPAR, nparallelism: $NPAR, format: $FORMAT, bufferpool_size $BUFFPOOLSIZE, buffer_size: $BUFFSIZE"

                    curl -d'rate='$NETWORK'mbps' localhost:4080/xdbcclient

                    echo "server cmd: ./xdbc-server -c$COMP --read-parallelism=$RPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s2 --system=$SYS"
                    bash $SERVER_PATH/build_and_start.sh xdbcserver 2 "-c$COMP --read-parallelism=$RPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s2 --system=$SYS" &
                    SERVER_PID=$!

                    sleep 1
                    echo "client cmd: ./test_xclient -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -P$NPAR -s2 --table=$TBL"
                    SECONDS=0
                    DATASIZE=$(bash experiments_measure_network.sh "xdbcclient")
                    bash $CLIENT_PATH/build_and_start.sh xdbcclient 2 "-f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -P$NPAR -s2 --table=$TBL"
                    ELAPSED_SEC=$SECONDS
                    DATASIZE="$(($(bash experiments_measure_network.sh "xdbcclient")-$DATASIZE))"

                    echo "$(date +%F),$COMP,$RPAR,$NPAR,$CPUS,$NETWORK,$FORMAT,$BUFFPOOLSIZE,$BUFFSIZE,$ELAPSED_SEC,$DATASIZE" >>$EXECLOG
                    echo "$(date +%F),$COMP,$RPAR,$NPAR,$CPUS,$NETWORK,$FORMAT,$BUFFPOOLSIZE,$BUFFSIZE,$ELAPSED_SEC,$DATASIZE"
                    #TODO: find correct pid
                    #kill $SERVER_PID
                  done
                done
              done
            done
          done
        done
      done
    done
  done
done
