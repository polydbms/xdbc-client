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

set_env(env)

show_server_output = False
show_client_output = False

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

print(test_env)

csv_file_path = "res/figureACSVCSVOpt.csv"

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "timestamp", "env", "repetition",
            "read_par", "deser_par", "comp_par",
            "send_par", "rcv_par", "decomp_par",
            "ser_par", "write_par",
            "table", "time"
        ])

bpsize = 65536 * 10 * 3
bsize = 65536

normal_conf = create_conf(read_par=1, deser_par=1, comp_par=1, send_par=1, rcv_par=1, decomp_par=1, ser_par=8,
                          write_par=1,
                          buffer_size=bsize, server_buffpool_size=bpsize, client_buffpool_size=bpsize,
                          format=1,
                          compression_lib='snappy', skip_ser=0, skip_deser=0)


def generate_configs(base_conf, scale_keys, scale_values):
    configs = []
    for key in scale_keys:
        for value in scale_values:
            # Copy base config
            conf = base_conf.copy()
            # Scale the current key
            conf[key] = value
            configs.append(conf)
    return configs


parallelism_keys = ["read_par", "deser_par", "comp_par", "decomp_par", "ser_par", "write_par"]
scale_values = [1, 2, 4, 8, 16]

configs = generate_configs(normal_conf, parallelism_keys, scale_values)

# run xdbc
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

de_ser_pars = [1, 2, 4, 8, 16]
read_pars = [1, 2, 4, 8, 16]

for cur_conf in configs:
    for i in range(repetitions):
        t = run_xdbserver_and_xdbclient(cur_conf, env, perf_dir, show_output=(False, False))

        print(f"xdbc for read_par co: {t} s")

        timestamp = int(datetime.datetime.now().timestamp())

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [timestamp, test_env['name'], i + 1, cur_conf['read_par'], cur_conf['deser_par'], cur_conf['comp_par'],
                 cur_conf['send_par'], cur_conf['rcv_par'], cur_conf['decomp_par'], cur_conf['ser_par'],
                 cur_conf['write_par'],
                 env['table'], t])

            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(cur_conf, file, indent=4)
