import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file

repetitions = 1
test_env = next((env for env in test_envs if env['name'] == "test"), None)
# prepare environment
env = test_env['env']
env['src'] = 'postgres'

set_env(env)

show_server_output = False
show_client_output = False

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

print(test_env)

csv_file_path = "res/testPostgres.csv"

create_file(csv_file_path)

bpsize = 65536
bsize = 256

normal_conf = create_conf(read_par=10, deser_par=8, comp_par=1, send_par=2, rcv_par=2, decomp_par=1, ser_par=10,
                          write_par=2, buffer_size=bsize, server_buffpool_size=bpsize, client_buffpool_size=bpsize,
                          format=2, compression_lib='nocomp', skip_ser=0, skip_deser=0)

comp_conf = create_conf(read_par=10, deser_par=8, comp_par=4, send_par=2, rcv_par=2, decomp_par=4, ser_par=10,
                        write_par=2, buffer_size=bsize, server_buffpool_size=bpsize, client_buffpool_size=bpsize,
                        format=2, compression_lib='snappy', skip_ser=0, skip_deser=0)

# run xdbc
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

confs = [normal_conf, comp_conf]
table = "lineitem_sf10"
for j, conf in enumerate(confs):

    for i in range(repetitions):

        t = run_xdbserver_and_xdbclient(conf, env, perf_dir, show_output=(True, True))

        conf_name = "wrong"
        if j == 0:
            conf_name = "[nocomp]"
        elif j == 1:
            conf_name = "[comp]"

        print(f"xdbc for conf {conf_name} and table {table}: {t} s")

        timestamp = int(datetime.datetime.now().timestamp())

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [timestamp, test_env['name'], i + 1, f"xdbc{conf_name}", table, t])

        with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
            json.dump(conf, file, indent=4)
