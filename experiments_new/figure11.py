import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from optimizer.optimize import optimize
from experiment_helpers import set_env, create_file

repetitions = 3
test_env = next((env for env in test_envs if env['name'] == "figure_11"), None)
env = test_env['env']

set_env(env)
csv_file_path = "res/figure11.csv"
create_file(csv_file_path)

baseline = "netcat"

# run baseline
show_server_output = False
show_client_output = False

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

for table in test_env['env']['tables']:
    for i in range(repetitions):
        subprocess.Popen(["docker", "exec", "-it", env['server_container'], "bash", "-c",
                          f"cd /dev/shm && nc -N -l -p 1234 < {table}.csv"],
                         stdout=show_stdout_server)

        a = datetime.datetime.now()

        subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                        f"""cd /dev/shm && nc -d {env['server_container']} 1234 > output_{table}.csv
                                """], check=True, stdout=show_stdout_client)

        b = datetime.datetime.now()
        c = (b - a).total_seconds()
        print(f"{baseline} : {c} s")

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([int(a.timestamp()), test_env['name'], i + 1, baseline, table, c])

perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
for table in test_env['env']['tables']:
    for i in range(repetitions):
        env['table'] = table

        for skip_ser in [0, 1]:
            n, best_config, estimated_thr, opt_time = optimize(env, 'xdbc', 'heuristic', False, skip_ser)
            t = run_xdbserver_and_xdbclient(best_config, env, perf_dir)

            print(f"xdbc for {table}: {t} s")

            timestamp = int(datetime.datetime.now().timestamp())
            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([timestamp, test_env['name'], i + 1, f"xdbc-skip{skip_ser}", table, t])

            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(best_config, file, indent=4)
