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
echo "date,sys,cpu,network,comp,rparallelism,rparts,deserpar,npar,format,bpsize,bsize,time,datasize" >$EXECLOG

#readmode: 1 analytics, 2 storage
RMODE=1
#systems=("postgres" "clickhouse")
systems=("csv")
#tables=("test_10000000" "test_1000000")
tables=("test_10000000")
#cpus=(.2 7)
cpus=(7)
#networks=(100 50 25 13 6)
networks=(100)
#comps=("nocomp" "zstd" "snappy" "lzo" "lz4" "zlib" "cols")
comps=("nocomp")
#rpars=(1 2 4 8 16)
rpars=(8)
#readpartitions=(1 2 4 8)
readpartitions=(8)
#deserpars=(8)
deserpars=(8)
#npars=(1 2 4 8 16)
npars=(1)
#formats=(1 2)
formats=(1)
bufpoolsizes=(1000)
#bufpoolsizes=(1000)
#buffsizes=(1000 10000)
buffsizes=(1000)

for SYS in "${systems[@]}"; do
  for TBL in "${tables[@]}"; do
    for CPU in "${cpus[@]}"; do
      docker update --cpus $CPUS xdbcclient
      for NETWORK in "${networks[@]}"; do
        for COMP in "${comps[@]}"; do
          for RPAR in "${rpars[@]}"; do
            for RPARTS in "${readpartitions[@]}"; do
              for DESERPAR in "${deserpars[@]}"; do
                if [ $SYS == 'csv' ]; then
                  #docker exec xdbcserver bash -c "cd /tmp/ && split -d -n $DESERPAR ${TBL}.csv -a 2 --additional-suffix=.csv ${TBL}_"
                  total_lines=$(docker exec xdbcserver bash -c "wc -l </dev/shm/${TBL}.csv")
                  lines_per_file=$((($total_lines + $DESERPAR - 1) / $DESERPAR))
                  docker exec xdbcserver bash -c "cd /dev/shm/ && split -d --lines=${lines_per_file} test_10000000.csv --additional-suffix=.csv ${TBL}_"
                fi
                for NPAR in "${npars[@]}"; do
                  for FORMAT in "${formats[@]}"; do
                    for BUFFPOOLSIZE in "${bufpoolsizes[@]}"; do
                      for BUFFSIZE in "${buffsizes[@]}"; do

                        echo "Running cpus: $CPU, network: $NETWORK, compression: $COMP, rparallelism: $RPAR, rpartitions: $RPARTS, nparallelism: $NPAR, format: $FORMAT, bufferpool_size $BUFFPOOLSIZE, buffer_size: $BUFFSIZE"

                        curl -d'rate='$NETWORK'mbps' localhost:4080/xdbcclient

                        echo "server cmd: ./xdbc-server -c$COMP --read-parallelism=$RPAR --read-partitions=$RPARTS --deser-parallelism=$DESERPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s1 --system=$SYS"
                        bash $SERVER_PATH/build_and_start.sh xdbcserver 2 "-c$COMP --read-parallelism=$RPAR --read-partitions=$RPARTS --deser-parallelism=$DESERPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s1 --system=$SYS" &
                        #bash $SERVER_PATH/build_and_start.sh xdbcserver 2 "-c$COMP --read-parallelism=$RPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s1 --system=$SYS" &
                        SERVER_PID=$!

                        sleep 1
                        echo "client cmd: ./test_xclient -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -P$NPAR -s1 -m$RMODE --table=$TBL"
                        SECONDS=0
                        DATASIZE=$(bash experiments_measure_network.sh "xdbcclient")
                        bash $CLIENT_PATH/build_and_start.sh xdbcclient 2 "-f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -P$NPAR -s1 --table=$TBL -m$RMODE"
                        ELAPSED_SEC=$SECONDS
                        DATASIZE="$(($(bash experiments_measure_network.sh "xdbcclient") - $DATASIZE))"

                        echo "$(date +%F),$SYS,$CPU,$NETWORK,$COMP,$RPAR,$RPARTS,$DESERPAR,$NPAR,$FORMAT,$BUFFPOOLSIZE,$BUFFSIZE,$ELAPSED_SEC,$DATASIZE" >>$EXECLOG
                        echo "$(date +%F),$SYS,$CPU,$NETWORK,$COMP,$RPAR,$RPARTS,$DESERPAR,$NPAR,$FORMAT,$BUFFPOOLSIZE,$BUFFSIZE,$ELAPSED_SEC,$DATASIZE"

                        #TODO: find correct pid
                        #kill $SERVER_PID
                      done
                    done
                  done
                done
                if [ $SYS == 'csv' ]; then
                  docker exec xdbcserver bash -c "rm /dev/shm/${TBL}_*.csv"
                fi
              done
            done
          done
        done
      done
    done
  done
done
