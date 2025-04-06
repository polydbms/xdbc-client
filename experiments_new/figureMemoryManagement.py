import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file
import copy

repetitions = 1
test_env = next((env for env in test_envs if env['name'] == "test"), None)
# prepare environment
env = test_env['env']
env['network'] = 1000
set_env(env)
print(test_env)

csv_file_path = "res/figureMemoryManagement.csv"

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "timestamp", "env", "repetition",
            "bufferpool_size", "buffer_size",
            "table", "time"
        ])

bpsizes = [2048, 4096, 8192, 16384, 32768, 65536]
bsizes = [16, 32, 64, 128, 256, 512, 1024]

per_queue = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

de_ser_par = 8

min_buffers = de_ser_par * 2 + 1

normal_conf = create_conf(read_par=1, deser_par=de_ser_par, comp_par=4, send_par=1, rcv_par=1, decomp_par=4,
                          ser_par=de_ser_par,
                          write_par=1, format=1, skip_ser=0, skip_deser=0, compression_lib='nocomp')

perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

server_pars = client_pars = de_ser_par + 4
for bpsize in bpsizes:
    for bsize in bsizes:
        available_buffers = (bpsize / bsize)
        consumers = de_ser_par * 2 + 1
        if available_buffers < consumers:
            print(f"Invalid bufferpool: {bpsize}, buffer: {bsize}, available buffers: {available_buffers}/{consumers}")
            continue
        for i in range(repetitions):
            cur_conf = config = copy.deepcopy(normal_conf)
            cur_conf['server_buffpool_size'] = bpsize
            cur_conf['client_buffpool_size'] = bpsize
            cur_conf['buffer_size'] = bsize

            t = run_xdbserver_and_xdbclient(cur_conf, env, perf_dir, show_output=(True, True))
            print(f"xdbc for bufferpool: {bpsize}, buffer: {bsize}: {t} s")

            timestamp = int(datetime.datetime.now().timestamp())

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([timestamp, test_env['name'], i + 1, bpsize, bsize, f"{env['table']}_nocomp", t])

                with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                    json.dump(cur_conf, file, indent=4)
