import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file

repetitions = 1
test_env = next((env for env in test_envs if env['name'] == "figure_parquet"), None)
# prepare environment
env = test_env['env']

set_env(env)

show_server_output = False
show_client_output = False

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

print(test_env)

csv_file_path = "res/figureYParquet.csv"

create_file(csv_file_path)

bpsize = 65536 * 10 * 3
bsize = 65536

normal_conf = create_conf(read_par=4, deser_par=4, comp_par=1, send_par=1, rcv_par=1, decomp_par=1, ser_par=4,
                          write_par=4, buffer_size=bsize, server_buffpool_size=bpsize, client_buffpool_size=bpsize,
                          format=2, compression_lib='nocomp', skip_ser=0, skip_deser=0)

skip_ser_conf = create_conf(read_par=4, deser_par=4, comp_par=1, send_par=1, rcv_par=1, decomp_par=1, ser_par=4,
                            write_par=4, buffer_size=bsize, server_buffpool_size=bpsize, client_buffpool_size=bpsize,
                            format=4, compression_lib='nocomp', skip_ser=1, skip_deser=1)

# run baselines
baselines = ["read_parquet[pyarrow]", "duckdb"]

subprocess.Popen(["docker", "exec", "-it", env['server_container'], "bash", "-c",
                  f"cd /dev/shm && python3 -m RangeHTTPServer 1234"],
                 stdout=show_stdout_server)

for baseline in baselines:
    for table in test_env['env']['tables']:
        for i in range(repetitions):
            a = datetime.datetime.now()

            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"""python /workspace/tests/pandas_parquet.py --table '{table}' --debug=0 --system {baseline}
                                    """], check=True, stdout=show_stdout_client)

            b = datetime.datetime.now()
            c = (b - a).total_seconds()
            print(f"{baseline} : {c} s")

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([int(a.timestamp()), test_env['name'], i + 1, baseline, table, c])

subprocess.run(["docker", "exec", "-it", env['server_container'], "pkill", "-f", "python3 -m RangeHTTPServer 1234"],
               check=True)

# run xdbc
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

confs = [normal_conf, skip_ser_conf]

for j, conf in enumerate(confs):
    for table in test_env['env']['tables']:
        for i in range(repetitions):
            env['table'] = table
            t = run_xdbserver_and_xdbclient(conf, env, perf_dir, show_output=(True, True))

            conf_name = "wrong"
            if j == 0:
                conf_name = "[col]"
            elif j == 1:
                conf_name = "[parquet]"

            print(f"xdbc for conf {conf_name} and table {table}: {t} s")

            timestamp = int(datetime.datetime.now().timestamp())

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [timestamp, test_env['name'], i + 1, f"xdbc{conf_name}", table, t])

            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(conf, file, indent=4)
