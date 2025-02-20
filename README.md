# xdbc-client

## Build XDBC
To build XDBC you just need `make` and `docker`. Then, to build the XDBC Client image run:
```
make
```
The images can also manually be built running `docker build ...` (the contents of the Makefile). Follow the same instructions to build the XDBC Server on: https://github.com/polydbms/xdbc-server

## Set up the infrastructure
To spawn the XDBC Server and the Client
```
docker compose -f docker-xdbc.yml up -d
docker compose -f docker-tc.yml up -d
```
Docker tc is used for emulating network traffic restrictions on containers.

## Running XDBC

### First start the server
To run a data transfer simply run XDBC with the default options:
```
docker exec -it xdbcserver bash -c "./xdbc-server/build/xdbc-server"
```
The XDBC Server supports multiple options. For example, to transfer from a CSV source with a buffer size of 256 kb, a buffer pool size of 16384, and the parallelism for deserialization at 16, read at 1 and compression at 2 with a row format and snappy, run:
```
docker exec -it xdbcserver bash -c "./xdbc-server/build/xdbc-server \
--system csv -b 1024 -p 32000 --deser-parallelism 4 --read-parallelism 1 \
--compression-parallelism=2 -f1 -csnappy"
```
Currently, XDBC assumes your data is placed in `/dev/shm`, which is also mapped to the containers' `/dev/shm`.
### Then initiate the transfer through a client
```
docker exec -it xdbcserver bash -c "./xdbc-server/tests/build/test --table ss13husallm"
```
The XDBC Client also supports multiple options. For example to transfer the ss13husallm dataset with a buffer size of, a buffer pool size of 16384, and the parallelism for writing at 16, decompression at 1, run:
```
docker exec -it xdbcclient bash -c "/xdbc-client/Sinks/build/xdbcsinks --server-host="xdbcserver" --table lineitem_sf10 \
-f1 -b 1024 -p 32000 -n1 -w1 -d2 -s8 --skip-serializer=0 --target=csv"
```
Please make sure that you have the `ss13husallm.csv` file in your `/dev/shm/` directory. Your output will be located at `/dev/shm/`
## Optimizer
The optimizer is currently implemented in Python, hence a working local Python installation is necessary.
To use XDBC's optimizer run:
```
python3 xdbc-client/optimizer/main.py --env_name=icu_analysis --optimizer=xdbc --optimizer_spec=heuristic
```
This will run the transfer in a predefined environment for benchmarking purposes. To add an environment change `xdbc-client/optimizer/test_envs.py`. If you run XDBC for the first time, it will first do a run to collect runtime statistics, to then run the optimizer for subsequent transfers.
