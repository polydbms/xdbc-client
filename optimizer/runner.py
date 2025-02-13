import subprocess
import time
import datetime
import csv
import os
from optimizer.config.metrics_client import MetricsClient
from optimizer.config.metrics_server import MetricsServer


def run_xdbserver_and_xdbclient(config, env, perf_dir, sleep=2, show_output=(False, False)):
    show_server_output, show_client_output = show_output
    show_stdout_server = None if show_server_output else subprocess.DEVNULL
    show_stdout_client = None if show_client_output else subprocess.DEVNULL

    # subprocess.run([f"curl -X DELETE localhost:4080/xdbcserver && curl -X PUT localhost:4080/xdbcserver"])
    subprocess.run(['docker', 'update', '--cpus', f'{env["server_cpu"]}', env['server_container']])
    subprocess.run(['docker', 'update', '--cpus', f'{env["client_cpu"]}', env['client_container']])

    subprocess.run(["curl", "-X", "DELETE", f"localhost:4080/{env['client_container']}"])
    subprocess.run(["curl", "-X", "PUT", f"localhost:4080/{env['client_container']}"])

    if env['network'] != 0:
        subprocess.run(["curl", "-s", "-d", f"rate={env['network']}mbps", f"localhost:4080/{env['client_container']}"])

    server_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'xdbc-server', 'experiments'))
    client_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'experiments'))
    measurement_path = os.path.abspath(os.path.join(perf_dir, 'xdbc_general_stats.csv'))
    config['host'] = os.uname().nodename

    config['read_partitions'] = 1
    # config['buffer_size'] = 1024
    # config['bufferpool_size'] = 65536

    config['run'] = 1
    config['date'] = int(time.time_ns())
    config['xdbc_version'] = 11
    config['system_source'] = env["src"]
    config['system_dest'] = env["target"]
    profiling_interval = 1000

    if 'compression_lib' not in config:
        config['compression_lib'] = "nocomp"

    result = {}

    print("----------------------------------------")
    print("XDBC Runner with config:")
    print(config)

    try:

        server_command = (
            f"./xdbc-server/build/xdbc-server "
            f"--network-parallelism={config['send_par']} "
            f"--read-partitions=1 "
            f"--read-parallelism={config['read_par']} "
            f"-c{config['compression_lib']} "
            f"--compression-parallelism={config['comp_par']} "
            f"--buffer-size={config['buffer_size']} "
            f"--dp={config['deser_par']} "
            f"-p{config['server_buffpool_size']} "
            f"-f{config['format']} "
            f"--skip-deserializer={config['skip_deser']} "
            f"--tid=\"{config['date']}\" "
            f"--system={env['src']} "
            f"--profiling-interval={profiling_interval}"
        )

        print("Server command to execute:")
        print(server_command)

        subprocess.run(["docker", "exec", "-d", "xdbcserver", "bash", "-c", server_command],
                       check=True,
                       stdout=show_stdout_server)

        time.sleep(sleep)

        start_data_size = measure_network(client_path, env['client_container'])
        a = datetime.datetime.now()

        if env['target'] == 'csv' or env['target'] == 'parquet':
            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"""./xdbc-client/Sinks/build/xdbcsinks \
                                --server-host={env['server_container']} \
                                --table="{env['table']}" \
                                -f{config['format']} \
                                -b{config['buffer_size']} \
                                -p{config['client_buffpool_size']} \
                                -n{config['rcv_par']} \
                                -d{config['decomp_par']} \
                                -s{config['ser_par']} \
                                -w{config['write_par']} \
                                --skip-serializer={config['skip_ser']} \
                                --target={env['target']} \
                                --tid=\"{config['date']}\" \
                                --profiling-interval={profiling_interval}
                            """], check=True, stdout=show_stdout_client)
        elif env['target'] == 'pandas':
            print("Running pandas")
            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"""python /workspace/tests/pandas_xdbc.py \
                            --env_name "PyXDBC Client" \
                            --table "{env['table']}" \
                            --iformat {config['format']} \
                            --buffer_size {config['buffer_size']} \
                            --bufferpool_size {config['client_buffpool_size']} \
                            --sleep_time 1 \
                            --rcv_par {config['rcv_par']} \
                            --ser_par {config['write_par']} \
                            --write_par {config['write_par']} \
                            --decomp_par {config['decomp_par']} \
                            --transfer_id {config['date']} \
                            --server_host {env['server_container']} \
                            --server_port "1234" \
                            --source {env['src']} \
                            --skip_ser {config['skip_ser']} \
                            """], check=True, stdout=show_stdout_client)

        elif env['target'] == 'spark':
            print("Running spark")
            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"""bash run_xdbc_spark.sh \
                            tableName={env['table']} \
                            iformat={config['format']} \
                            buffer_size={config['buffer_size']} \
                            bufferpool_size={config['client_buffpool_size']} \
                            rcv_par={config['rcv_par']} \
                            write_par={config['write_par']} \
                            decomp_par={config['decomp_par']} \
                            transfer_id={config['date']} \
                            server_host={env['server_container']}
                            """], check=True, stdout=show_stdout_client)

        elif env['target'] == 'postgres':
            print("Running postgres")

            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"cd /pg_xdbc_fdw/experiments/ && bash replace_options.sh -t {env['table']} -s {env['server_container']} -i {config['date']} -b {config['buffer_size']} -p {config['client_buffpool_size']} -n {config['rcv_par']} -d {config['decomp_par']} -r {config['write_par']}",
                            ], check=True, stdout=show_stdout_client)
            subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                            f"bash /pg_xdbc_fdw/experiments/run_pg_xdbc.sh {env['table']} {config['date']}"],
                           check=True, stdout=show_stdout_client)

        b = datetime.datetime.now()
        c = b - a
        result['time'] = c.total_seconds()

        end_data_size = measure_network(client_path, env['client_container'])
        result['size'] = end_data_size - start_data_size
        result['avg_cpu_server'] = 0
        result['avg_cpu_client'] = 0

        print(f"Total Data Transfer Size: {result['size']} bytes")
        write_csv_header(measurement_path)
        write_to_csv(measurement_path, env, config, result)
        copy_metrics(env['server_container'], env['client_container'], perf_dir)

        res = result['time']
    except subprocess.CalledProcessError as e:
        print(f"Error running XDBC: {e}")
        res = -1

    pkill_cmd = ["docker", "exec", env['server_container'], "bash", "-c",
                 "pkill -f './xdbc-server/build/xdbc-server' || true"]
    subprocess.run(pkill_cmd)
    if env['target'] == 'csv':
        subprocess.run(["docker", "exec", env['client_container'], "bash", "-c",
                        "rm -rf /dev/shm/output*"])

    return res


def check_file_exists(container, file_path):
    result = subprocess.run(
        ["docker", "exec", container, "test", "-f", file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return result.returncode == 0  # 0 means the file exists


def copy_metrics(server_container, client_container, perf_dir):
    file_path_server = '/tmp/xdbc_server_timings.csv'
    file_path_client = '/tmp/xdbc_client_timings.csv'

    if (not check_file_exists(server_container, file_path_server) or
            not check_file_exists(client_container, file_path_client)):
        return False

    absolute_perf_dir = os.path.join(os.getcwd(), perf_dir)
    os.makedirs(absolute_perf_dir, exist_ok=True)

    try:
        subprocess.run(["docker", "cp", f"{server_container}:{file_path_server}", absolute_perf_dir], check=True,
                       stdout=subprocess.DEVNULL)
        # subprocess.run(["docker", "cp", f"{client_container}:{file_path_client}", absolute_perf_dir], check=True,
        #               stdout=subprocess.DEVNULL)

        # for the client we need to only grab the last line, as we have different client containers
        # we also need to include the header if our file is empty
        local_file_path = f"{absolute_perf_dir}/xdbc_client_timings.csv"
        if not os.path.exists(local_file_path) or os.stat(local_file_path).st_size == 0:
            copy_command = (
                f"docker exec {client_container} sh -c 'head -n 1 {file_path_client}; tail -n 1 {file_path_client}' >> {local_file_path}"
            )
        else:
            # If the local file is not empty, copy only the last line
            copy_command = f"docker exec {client_container} sh -c 'tail -n 1 {file_path_client}' >> {local_file_path}"

        # Execute the appropriate command
        subprocess.run(copy_command, shell=True, check=True)


    except subprocess.CalledProcessError as e:
        print(f"Error during file copy: {e}")
        return False

    return True


def print_metrics(perf_dir, dict=False):
    absolute_perf_dir = os.path.join(os.getcwd(), perf_dir)

    metrics_server = MetricsServer.from_csv(f"{absolute_perf_dir}/xdbc_server_timings.csv")
    metrics_client = MetricsClient.from_csv(f"{absolute_perf_dir}/xdbc_client_timings.csv")
    if dict:
        return {**metrics_server.get_throughput_metrics(False),
                **metrics_client.get_throughput_metrics(False)}
    print(metrics_server.get_throughput_metrics())
    print(metrics_client.get_throughput_metrics())


def measure_network(client_path, container):
    result = subprocess.run(
        f"bash {client_path}/experiments_measure_network.sh '{container}'",
        shell=True,
        capture_output=True,
        text=True
    )
    return int(result.stdout.strip())


def write_csv_header(filename, config=None):
    header = ['date', 'xdbc_version', 'host', 'run',
              'server_cpu', 'client_cpu', 'network', 'network_latency', 'network_loss',
              'source_system', 'target_system', 'table', 'server_bufferpool_size', 'client_bufferpool_size',
              'buffer_size', 'compression', 'format', 'skip_ser', 'skip_deser',
              'read_par', 'read_partitions', 'deser_par', 'comp_par', 'send_par',
              'rcv_par', 'decomp_par', 'ser_par', 'write_par',
              'time', 'datasize', 'avg_cpu_server', 'avg_cpu_client']

    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(filename):
        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header)


def write_to_csv(filename, env, config, result):
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                config['date'], config['xdbc_version'], config['host'], config['run'],
                env['server_cpu'], env['client_cpu'], env['network'], env['network_latency'], env['network_loss'],
                env['src'], env['target'], env['table'], config['server_buffpool_size'], config['client_buffpool_size'],
                config['buffer_size'], config['compression_lib'], config['format'], config['skip_ser'],
                config['skip_deser'], config['read_par'], config['read_partitions'], config['deser_par'],
                config['comp_par'], config['send_par'], config['rcv_par'], config['decomp_par'], config['ser_par'],
                config['write_par'], result['time'], result['size'], result['avg_cpu_server'], result['avg_cpu_client']
            ]
        )
