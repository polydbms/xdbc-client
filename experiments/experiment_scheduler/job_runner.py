import time
import json
from ssh_handler import execute_ssh_cmd, SSHExecutionError


def run_job(ssh, host, config):
    """
    Run an experiment on a host with a given configuration.

    Args:
        ssh (ssh): The ssh connection to use
        host (str): The hostname or identifier of the host.
        config (dict): The experiment configuration.

    Returns:
        dict: The result of the experiment.
    """
    try:
        current_timestamp = int(time.time_ns())

        server_path = "~/xdbc/xdbc-server/experiments"
        client_path = "~/xdbc/xdbc-client/experiments"

        if config['system'] == 'csv':
            total_lines = execute_ssh_cmd(ssh,
                                          f"echo $(docker exec xdbcserver bash -c 'wc -l </dev/shm/{config['table']}.csv')")

            lines_per_file = execute_ssh_cmd(ssh,
                                             f"echo $((({total_lines} + {config['server_deser_par']} - 1) / {config['server_deser_par']}))")

            execute_ssh_cmd(ssh,
                            f"docker exec xdbcserver bash -c 'cd /dev/shm/ && split -d --lines={lines_per_file} {config['table']}.csv --additional-suffix=.csv {config['table']}_'")

        execute_ssh_cmd(ssh, f"docker update --cpus {config['server_cpu']} xdbcserver")
        execute_ssh_cmd(ssh, f"docker update --cpus {config['client_cpu']} xdbcclient")

        execute_ssh_cmd(ssh, f"curl -s -d 'rate={config['network']}mbps' localhost:4080/xdbcclient")

        execute_ssh_cmd(ssh,
                        f"bash {server_path}/build_and_start.sh xdbcserver 2 \"--transfer-id={current_timestamp} -c{config['compression']} --read-parallelism={config['server_read_par']} --read-partitions={config['server_read_partitions']} --deser-parallelism={config['server_deser_par']} --compression-parallelism={config['server_comp_par']} --network-parallelism={config['network_parallelism']} -f{config['format']} -b{config['buff_size']} -p{config['bufpool_size']} -s1 --system={config['system']}\"",
                        True)
        # TODO: fix? maybe check when server has started instead of sleeping
        time.sleep(2)

        start_data_size = execute_ssh_cmd(ssh,
                                          f"echo $(bash {client_path}/experiments_measure_network.sh 'xdbcclient')")

        execute_ssh_cmd(ssh, f"rm -rf /tmp/stop_monitoring")
        execute_ssh_cmd(ssh, f"touch /tmp/start_monitoring")
        execute_ssh_cmd(ssh, f"bash {client_path}/experiments_measure_resources.sh xdbcserver xdbcclient", True)

        start_time = time.time()
        execute_ssh_cmd(ssh,
                        f"bash {client_path}/build_and_start.sh xdbcclient 2 '--transfer-id={current_timestamp} -f{config['format']} -b{config['buff_size']} -p{config['bufpool_size']} -n{config['network_parallelism']} -r{config['client_write_par']} -d{config['client_decomp_par']} -s1 --table={config['table']} -m{config['client_readmode']}' 1")

        elapsed_time = time.time() - start_time
        formatted_time = "{:.2f}".format(elapsed_time)

        execute_ssh_cmd(ssh, f"touch /tmp/stop_monitoring")
        # server_pid = execute_ssh_cmd(ssh, "docker exec xdbcserver pgrep xdbc-server")
        # if server_pid.strip():
        execute_ssh_cmd(ssh,
                        """docker exec xdbcserver bash -c 'pids=$(pgrep xdbc-server); if [ "$pids" ]; then kill $pids; fi'""")

        time.sleep(3)
        resource_metrics_json = execute_ssh_cmd(ssh,
                                                '[ -f /tmp/resource_metrics.json ] && cat /tmp/resource_metrics.json || echo "{}" 2>/dev/null')

        avg_cpu_server = -1
        avg_cpu_client = -1

        try:
            resource_metrics = json.loads(resource_metrics_json)
            if resource_metrics_json and resource_metrics_json != "{}":
                scpu_limit_percent = config['server_cpu'] * 100
                ccpu_limit_percent = config['client_cpu'] * 100

                # Extract and normalize CPU utilization
                avg_cpu_server = round(
                    (resource_metrics.get("xdbcserver", {}).get("average_cpu_usage", 0) / scpu_limit_percent) * 100, 2)
                avg_cpu_client = round(
                    (resource_metrics.get("xdbcclient", {}).get("average_cpu_usage", 0) / ccpu_limit_percent) * 100, 2)
        except json.JSONDecodeError as e:
            print(f"host {host} JSON decoding failed: {e}")
            print(f"Invalid JSON content: {resource_metrics_json}")

        data_size = int(
            execute_ssh_cmd(ssh, f"echo $(bash {client_path}/experiments_measure_network.sh 'xdbcclient')")) - int(
            start_data_size)
        execute_ssh_cmd(ssh, "rm -f /tmp/resource_metrics.json")
        # print(config)

        result = {
            "date": current_timestamp,
            "time": formatted_time,
            "size": data_size,
            "avg_cpu_server": avg_cpu_server,
            "avg_cpu_client": avg_cpu_client
        }

        # print(f"Complete run on host: {host}, config: {config}, result {result}")

        return result
    except SSHExecutionError as e:
        print(f"Error: {e}")
        return None
# Handle the error or break the loop
