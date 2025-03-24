import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from optimizer.optimize import optimize
from experiment_helpers import set_env, create_file

repetitions = 2
test_env = next((env for env in test_envs if env['name'] == "figurePandasPG"), None)
# baselines = ['connectorx', 'duckdb', 'modin', 'turbodbc']
baselines = []
# prepare environment
env = test_env['env']

set_env(env)

show_server_output = False
show_client_output = False

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

print(test_env)

csv_file_path = "res/figurePandasPGCPU.csv"
create_file(csv_file_path)
# run baselines

for table in test_env['env']['tables']:
    for baseline in baselines:
        for i in range(repetitions):
            a = datetime.datetime.now()

            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"""python /workspace/tests/pandas_baselines.py \
                                --parallelism 8 \
                                --table "{table}" \
                                --chunksize 42 \
                                --library "{baseline}"
                                """], check=True, stdout=show_stdout_client)

            b = datetime.datetime.now()
            c = (b - a).total_seconds()
            print(f"{baseline} for {table} : {c} s")

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([int(a.timestamp()), test_env['name'], i + 1, baseline, table, c])

# run xdbc
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
for table in test_env['env']['tables']:
    for i in range(repetitions):
        env['table'] = table

        n, best_config, estimated_thr, opt_time = optimize(env, 'xdbc', 'heuristic', False, 0)
        t = run_xdbserver_and_xdbclient(best_config, env, perf_dir)

        print(f"xdbc for {table}: {t} s")
        timestamp = int(datetime.datetime.now().timestamp())

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, test_env['name'], i + 1, "xdbc", table, t])

        with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
            json.dump(best_config, file, indent=4)
