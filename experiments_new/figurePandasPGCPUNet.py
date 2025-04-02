import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf

repetitions = 1
test_env = next((env for env in test_envs if env['name'] == "figurePandasPG"), None)
# prepare environment
env = test_env['env']

set_env(env)

show_server_output = False
show_client_output = False

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

print(test_env)

ashply

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow(["timestamp", "env", "repetition", "client_cpu", "network", "system", "table", "time"])

conf1 = create_conf(read_par=10, deser_par=4, comp_par=4, send_par=1, rcv_par=1, decomp_par=4, ser_par=8, write_par=8,
                    buffer_size=1024, server_buffpool_size=61440, client_buffpool_size=61440, format=2,
                    compression_lib='snappy', skip_ser=0)

conf2 = create_conf(read_par=10, deser_par=4, comp_par=4, send_par=1, rcv_par=1, decomp_par=1, ser_par=1, write_par=1,
                    buffer_size=1024, server_buffpool_size=61440, client_buffpool_size=27648, format=2,
                    compression_lib='nocomp', skip_ser=0)

conf3 = create_conf(read_par=10, deser_par=4, comp_par=1, send_par=1, rcv_par=1, decomp_par=1, ser_par=8, write_par=8,
                    buffer_size=1024, server_buffpool_size=61440, client_buffpool_size=61440, format=2,
                    compression_lib='nocomp', skip_ser=0)

table = "lineitem_sf10"

client_cpus = [1, 2, 4, 8, 16]
networks = [125, 250, 500, 1000, 0]

# run baselines
baseline = "connectorx"
'''
for client_cpu in client_cpus:
    for i in range(repetitions):
        # change client cpu
        env['client_cpu'] = client_cpu
        set_env(env)
        for par in [8, client_cpu]:
            a = datetime.datetime.now()

            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"""python /workspace/tests/pandas_baselines.py \
                                --parallelism {par} \
                                --table "{table}" \
                                --chunksize 42 \
                                --library "{baseline}"
                                """], check=True, stdout=show_stdout_client)

            baselineText = f"{baseline}[conservative]"
            if par == 8:
                baselineText = f"{baseline}[aggressive]"

            b = datetime.datetime.now()
            c = (b - a).total_seconds()
            print(f"{baseline} for {env['client_cpu']} & {table} : {c} s")

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [int(a.timestamp()), test_env['name'], i + 1, env['client_cpu'], env['network'], baselineText,
                     table, c])
'''
env['client_cpu'] = 8

for network in networks:
    # change network
    env['network'] = network
    set_env(env)
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
        print(f"{baseline} for {env['network']}mbps & {table} : {c} s")

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [int(a.timestamp()), test_env['name'], i + 1, env['client_cpu'], env['network'], baseline, table, c])

'''
# run xdbc
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
env['table'] = table
confs = [conf1, conf2]

for client_cpu in client_cpus:
    # change client cpu
    env['client_cpu'] = client_cpu
    set_env(env)
    for j, conf in enumerate(confs):
        for i in range(repetitions):
            t = run_xdbserver_and_xdbclient(conf, env, perf_dir, show_output=(False, False))

            conf_name = "wrong"
            if j == 0:
                conf_name = "[aggressive]"
            elif j == 1:
                conf_name = "[conservative]"

            print(f"xdbc for {table} and client cpu {env['client_cpu']}: {t} s")

            timestamp = int(datetime.datetime.now().timestamp())

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [timestamp, test_env['name'], i + 1, env['client_cpu'], env['network'],
                     f"xdbc{conf_name}", table, t])

            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(conf, file, indent=4)

confs = [conf1, conf3]
env['client_cpu'] = 8

for network in networks:
    # change network
    env['network'] = network
    set_env(env)
    for j, conf in enumerate(confs):
        for i in range(repetitions):
            t = run_xdbserver_and_xdbclient(conf, env, perf_dir)

            conf_name = "wrong"
            if j == 0:
                conf_name = "[comp]"
            elif j == 1:
                conf_name = "[nocomp]"

            print(f"xdbc for {table} and network {network}: {t} s")

            timestamp = int(datetime.datetime.now().timestamp())

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [timestamp, test_env['name'], i + 1, env['client_cpu'], env['network'],
                     f"xdbc{conf_name}", table, t])

            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(conf, file, indent=4)
'''
