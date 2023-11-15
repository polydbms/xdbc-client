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
EXECLOG=local_measurements/${TS}_runs_comp.csv
echo "date,xdbcver,sys,table,scpu,ccpu,network,comp,format,npar,bpsize,bsize,sreadpar,sreadparts,sdeserpar,scomppar,creadpar,cdecomppar,datasize,time,avgcpuserver,avgcpuclient,run" >$EXECLOG

docker exec xdbcserver bash -c "[ ! -f /tmp/xdbc_server_timings.csv ] && echo 'transfer_id,total_time,read_wait_time,read_time,deser_wait_time,deser_time,compression_wait_time,compression_time,network_wait_time,network_time' > /tmp/xdbc_server_timings.csv"
#TODO: the same for xdbcclient

### GENERAL
XDBCVER=2
runs=(1)
#systems=("postgres" "clickhouse")
systems=("csv")
#tables=("test_10000000" "test_1000000")
tables=("test_10000000")
#comps=("nocomp" "zstd" "snappy" "lzo" "lz4" "zlib" "cols")
comps=("snappy")
#formats=(1 2)
formats=(1)
#npars=(1 2 4 8 16)
npars=(1)
#bufpoolsizes=(1000)
bufpoolsizes=(1000)
#buffsizes=(1000 10000)
buffsizes=(1000)
#networks=(100 50 25 13 6)
networks=(100)

### CLIENT
#readmode: 1 analytics, 2 storage
RMODE=2
#cpus=(.2 7)
clientcpus=(8)
#clientrpars=(1 2 4 8 16)
clientrpars=(1)
#clientdecomppars=(8)
clientdecomppars=(1)

### SERVER
#cpus=(.2 7)
servercpus=(8)
#serverrpars=(1 2 4 8 16)
serverrpars=(1)
#serverreadpartitions=(1 2 4 8)
serverreadpartitions=(1)
#serverdeserpars=(8)
serverdeserpars=(1)
#servercomppars=(1 2 4 8)
servercomppars=(1)

for SYS in "${systems[@]}"; do
  for TBL in "${tables[@]}"; do
    for SCPU in "${servercpus[@]}"; do
      docker update --cpus "$SCPU" xdbcserver
      for CCPU in "${clientcpus[@]}"; do
        docker update --cpus "$CCPU" xdbcclient
        for NETWORK in "${networks[@]}"; do
          for COMP in "${comps[@]}"; do
            for SRPAR in "${serverrpars[@]}"; do
              for CRPAR in "${clientrpars[@]}"; do
                for SRPARTS in "${serverreadpartitions[@]}"; do
                  for SDESERPAR in "${serverdeserpars[@]}"; do
                    for SCOMPPAR in "${servercomppars[@]}"; do
                      for CDECOMPPAR in "${clientdecomppars[@]}"; do
                        if [ $SYS == 'csv' ]; then
                          #docker exec xdbcserver bash -c "cd /tmp/ && split -d -n $DESERPAR ${TBL}.csv -a 2 --additional-suffix=.csv ${TBL}_"
                          total_lines=$(docker exec xdbcserver bash -c "wc -l </dev/shm/${TBL}.csv")
                          lines_per_file=$((($total_lines + $SDESERPAR - 1) / $SDESERPAR))
                          docker exec xdbcserver bash -c "cd /dev/shm/ && split -d --lines=${lines_per_file} test_10000000.csv --additional-suffix=.csv ${TBL}_"
                        fi
                        for NPAR in "${npars[@]}"; do
                          for FORMAT in "${formats[@]}"; do
                            for BUFFPOOLSIZE in "${bufpoolsizes[@]}"; do
                              for BUFFSIZE in "${buffsizes[@]}"; do
                                for RUN in "${runs[@]}"; do

                                  current_timestamp=$(date +%s)

                                  echo "Running scpus: $SCPU,ccpus: $CCPU, network: $NETWORK, compression: $COMP, rparallelism: $SRPAR, rpartitions: $SRPARTS, nparallelism: $NPAR, format: $FORMAT, bufferpool_size $BUFFPOOLSIZE, buffer_size: $BUFFSIZE"

                                  curl -d'rate='$NETWORK'mbps' localhost:4080/xdbcclient

                                  echo "server cmd: ./xdbc-server --transfer-id=$current_timestamp -c$COMP --read-parallelism=$SRPAR --read-partitions=$SRPARTS --deser-parallelism=$SDESERPAR --compression-parallelism=$SCOMPPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s1 --system=$SYS"
                                  bash $SERVER_PATH/build_and_start.sh xdbcserver 2 " --transfer-id=$current_timestamp -c$COMP --read-parallelism=$SRPAR --read-partitions=$SRPARTS --deser-parallelism=$SDESERPAR --compression-parallelism=$SCOMPPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s1 --system=$SYS" &
                                  #bash $SERVER_PATH/build_and_start.sh xdbcserver 2 "-c$COMP --read-parallelism=$RPAR --network-parallelism=$NPAR -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -s1 --system=$SYS" &
                                  #SERVER_PID=$!

                                  sleep 1
                                  echo "client cmd: ./test_xclient --transfer-id=$current_timestamp -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -n$NPAR -r$CRPAR -d$CDECOMPPAR -s1 -m$RMODE --table=$TBL"
                                  SECONDS=0
                                  DATASIZE=$(bash experiments_measure_network.sh "xdbcclient")

                                  touch /tmp/start_monitoring
                                  ./experiments_measure_resources.sh xdbcserver xdbcclient &

                                  bash $CLIENT_PATH/build_and_start.sh xdbcclient 2 "--transfer-id=$current_timestamp -f$FORMAT -b$BUFFSIZE -p$BUFFPOOLSIZE -n$NPAR -r$CRPAR -d$CDECOMPPAR -s1 --table=$TBL -m$RMODE"
                                  ELAPSED_SEC=$SECONDS
                                  touch /tmp/stop_monitoring
                                  rm /tmp/start_monitoring
                                  DATASIZE="$(($(bash experiments_measure_network.sh "xdbcclient") - $DATASIZE))"

                                  wait
                                  metrics_json=$(cat /tmp/resource_metrics.json)

                                  CPU_UTIL_SERVER=$(echo "$metrics_json" | jq -r '.["xdbcserver"].average_cpu_usage')
                                  CPU_UTIL_CLIENT=$(echo "$metrics_json" | jq -r '.["xdbcclient"].average_cpu_usage')

                                  # Convert CPU limit to percentage
                                  SCPU_LIMIT_PERCENT=$(echo "$SCPU*100" | bc)
                                  CCPU_LIMIT_PERCENT=$(echo "$CCPU*100" | bc)

                                  # Calculate normalized CPU utilization
                                  SCPU_UTIL_NORM=$(echo "scale=2; ($CPU_UTIL_SERVER / $SCPU_LIMIT_PERCENT) * 100" | bc)
                                  CCPU_UTIL_NORM=$(echo "scale=2; ($CPU_UTIL_CLIENT / $CCPU_LIMIT_PERCENT) * 100" | bc)

                                  echo "$current_timestamp,$XDBCVER,$SYS,$TBL,$SCPU,$CCPU,$NETWORK,$COMP,$FORMAT,$NPAR,$BUFFPOOLSIZE,$BUFFSIZE,$SRPAR,$SRPARTS,$SDESERPAR,$SCOMPPAR,$CRPAR,$CDECOMPPAR,$DATASIZE,$ELAPSED_SEC,$SCPU_UTIL_NORM,$CCPU_UTIL_NORM,$RUN" >>$EXECLOG
                                  echo "$current_timestamp,$XDBCVER,$SYS,$TBL,$SCPU,$CCPU,$NETWORK,$COMP,$FORMAT,$NPAR,$BUFFPOOLSIZE,$BUFFSIZE,$SRPAR,$SRPARTS,$SDESERPAR,$SCOMPPAR,$CRPAR,$CDECOMPPAR,$DATASIZE,$ELAPSED_SEC,$SCPU_UTIL_NORM,$CCPU_UTIL_NORM,$RUN"

                                  #TODO: find correct pid
                                  #kill $SERVER_PID
                                done
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
      done
    done
  done
done

docker cp xdbcserver:/tmp/xdbc_server_timings.csv local_measurements/
