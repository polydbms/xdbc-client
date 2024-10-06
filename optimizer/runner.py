import subprocess
import time
import datetime
import csv
import os
from config.metrics_client import MetricsClient
from config.metrics_server import MetricsServer


def run_xdbserver_and_xdbclient(config, systems, mode, cpus, sleep=2, network=0):
    # subprocess.run([f"curl -X DELETE localhost:4080/xdbcserver && curl -X PUT localhost:4080/xdbcserver"])
    subprocess.run(["curl", "-X", "DELETE", "localhost:4080/xdbcclient"])
    subprocess.run(["curl", "-X", "PUT", "localhost:4080/xdbcclient"])
    subprocess.run(["curl", "-X", "DELETE", "localhost:4080/xdbcpython"])
    subprocess.run(["curl", "-X", "PUT", "localhost:4080/xdbcpython"])

    subprocess.run(['docker', 'update', '--cpus', f'{cpus["server_cpu"]}', 'xdbcserver'])
    subprocess.run(['docker', 'update', '--cpus', f'{cpus["client_cpu"]}', 'xdbcclient'])
    subprocess.run(['docker', 'update', '--cpus', f'{cpus["client_cpu"]}', 'xdbcpython'])

    if network != 0:
        subprocess.run(["curl", "-s", "-d", f"rate={network}mbps", "localhost:4080/xdbcclient"])
        subprocess.run(["curl", "-s", "-d", f"rate={network}mbps", "localhost:4080/xdbcpython"])
        # subprocess.run(["bash"])

    server_path = "/home/harry-ldap/xdbc/xdbc-server/experiments"
    client_path = "/home/harry-ldap/xdbc/xdbc-client/experiments"
    measurement_path = "local_measurements/xdbc_local.csv"
    config['host'] = os.uname().nodename
    if config['host'] == 'poodle-2':
        server_path = "~/workspace/xdbc-server/experiments"
        client_path = "~/workspace/xdbc-client/experiments"

    dest_source = systems.split('_')
    if len(dest_source) > 1:
        source = dest_source[1]
        dest = dest_source[0]
    else:
        print("Systems config error")

    config['client_readmode'] = mode
    config['buffer_size'] = 1024
    #config['bufferpool_size'] = 65536
    config['bufferpool_size'] = 131072

    config['run'] = 1
    config['date'] = 0
    config['xdbc_version'] = 10
    config['system_source'] = source
    config['system_dest'] = dest
    config['format'] = 1
    config['network'] = config['network_latency'] = config['network_loss'] = 0
    config['read_partitions'] = 1
    config['client_cpu'] = config['server_cpu'] = 10
    # config['bufferpool_size'] = 131072
    result = {}

    result['date'] = int(time.time_ns())

    try:
        if 'compression_lib' not in config:
            config['compression_lib'] = "nocomp"

        subprocess.run(["docker", "exec", "-d", "xdbcserver", "bash", "-c",
                        f"""./xdbc-server/build/xdbc-server \
                        --network-parallelism={config['send_par']} \
                         --read-partitions=1 \
                         --read-parallelism={config['read_par']} \
                          -c{config['compression_lib']} \
                          --compression-parallelism={config['comp_par']} \
                          --buffer-size={config['buffer_size']} \
                          --dp={config['deser_par']} \
                          -p{config['bufferpool_size']} \
                          -f{config['format']} \
                          --tid="{result['date']}" \
                          --system={source}
                        """], check=True)

        time.sleep(sleep)

        start_data_size = measure_network(server_path, client_path)
        a = datetime.datetime.now()

        if dest == 'csv':
            subprocess.run(["docker", "exec", "-it", "xdbcclient", "bash", "-c",
                            f"""./xdbc-client/tests/build/test_xclient \
                                --server-host="xdbcserver" \
                                --table="{config['table']}" \
                                -f{config['format']} \
                                -b{config['buffer_size']} \
                                -p{config['bufferpool_size']} \
                                -n{config['rcv_par']} \
                                -r{config['write_par']} \
                                -d{config['decomp_par']} \
                                -s1 \
                                --tid="{result['date']}" \
                                -m{mode}
                            """], check=True)
        elif dest == 'pandas':
            print("Running pandas")
            subprocess.run(["docker", "exec", "-it", "xdbcpython", "bash", "-c",
                            f"""python /workspace/tests/pandas_xdbc.py \
                            --env_name "PyXDBC Client" \
                            --table "{config['table']}" \
                            --iformat {config['format']} \
                            --buffer_size {config['buffer_size']} \
                            --bufferpool_size {config['bufferpool_size']} \
                            --sleep_time 1 \
                            --rcv_par {config['rcv_par']} \
                            --write_par {config['write_par']} \
                            --decomp_par {config['decomp_par']} \
                            --transfer_id {result['date']} \
                            --server_host "xdbcserver" \
                            --server_port "1234"
                            """], check=True)

        b = datetime.datetime.now()
        c = b - a
        result['time'] = c.total_seconds() - sleep

        end_data_size = measure_network(server_path, client_path)
        result['size'] = end_data_size - start_data_size
        result['avg_cpu_server'] = 0
        result['avg_cpu_client'] = 0

        print(f"Total Data Transfer: {result['size']}")
        write_csv_header(measurement_path)
        write_to_csv(measurement_path, config['host'], config, result)

        return result['time']
    except subprocess.CalledProcessError as e:
        print(f"Error running XDBC: {e}")
        return 0.1


def check_file_exists(container, file_path):
    result = subprocess.run(
        ["docker", "exec", container, "test", "-f", file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return result.returncode == 0  # 0 means the file exists


def print_metrics(dict=False, client_container="xdbcclient"):
    file_path_server = '/tmp/xdbc_server_timings.csv'
    file_path_client = '/tmp/xdbc_client_timings.csv'

    if (not check_file_exists("xdbcserver", file_path_server) or
            not check_file_exists(client_container, file_path_client)):
        return {}

    subprocess.run(["docker", "cp", f"xdbcserver:{file_path_server}", file_path_server])
    subprocess.run(["docker", "cp", f"{client_container}:{file_path_client}", file_path_client])

    metrics_server = MetricsServer.from_csv(file_path_server)
    metrics_client = MetricsClient.from_csv(file_path_client)
    if dict:
        return {**metrics_server.get_throughput_metrics(False),
                **metrics_client.get_throughput_metrics(False)}
    print(metrics_server.get_throughput_metrics())
    print(metrics_client.get_throughput_metrics())


def measure_network(server_path, client_path):
    result = subprocess.run(
        f"bash {client_path}/experiments_measure_network.sh 'xdbcclient'",
        shell=True,
        capture_output=True,
        text=True
    )
    return int(result.stdout.strip())


def write_csv_header(filename, config=None):
    header = ['date', 'xdbc_version', 'host', 'run', 'source_system', 'target_system', 'table', 'compression',
              'format', 'send_par',
              'rcv_par', 'bufferpool_size', 'buffer_size', 'network', 'network_latency', 'network_loss',
              'client_readmode', 'client_cpu', 'write_par', 'decomp_par', 'server_cpu', 'read_par',
              'read_partitions', 'deser_par', 'comp_par', 'time', 'datasize', 'avg_cpu_server',
              'avg_cpu_client']

    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(filename):
        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header)


def write_to_csv(filename, host, config, result):
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [result['date']] + [config['xdbc_version'], host] +
            [config['run'], config['system_source'], config['system_dest'], config['table'],
             config['compression_lib'],
             config['format'], config['send_par'], config['rcv_par'],
             config['bufferpool_size'],
             config['buffer_size'], config['network'], config['network_latency'], config['network_loss'],
             config['client_readmode'],
             config['client_cpu'], config['write_par'],
             config['decomp_par'],
             config['server_cpu'], config['read_par'],
             config['read_partitions'],
             config['deser_par'],
             config['comp_par']] +
            [result['time'], result['size'], result['avg_cpu_server'], result['avg_cpu_client']])
