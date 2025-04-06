import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from optimizer.optimize import optimize
from experiment_helpers import set_env, create_file

repetitions = 2
test_env = next((env for env in test_envs if env['name'] == "figure_11"), None)
env = test_env['env']

set_env(env)
csv_file_path = "res/figureXArrow.csv"
create_file(csv_file_path)

formats = [1, 2, 3]

perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

for skip_ser in [0, 1]:
    for table in test_env['env']['tables']:
        formats_to_run = formats if skip_ser == 0 else [None]
        for fmt in formats_to_run:
            for i in range(repetitions):
                env['table'] = table

                n, best_config, estimated_thr, opt_time = optimize(env, 'xdbc', 'heuristic', False, skip_ser)
                if skip_ser == 0:
                    best_config['format'] = fmt
                t = run_xdbserver_and_xdbclient(best_config, env, perf_dir)

                print(f"xdbc for {table} (skip_ser={skip_ser}, format={fmt}): {t} s")

                timestamp = int(datetime.datetime.now().timestamp())
                with open(csv_file_path, mode="a", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(
                        [timestamp, test_env['name'], i + 1, f"xdbc-skip{skip_ser}-format{fmt}", table, t])

                with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                    json.dump(best_config, file, indent=4)
