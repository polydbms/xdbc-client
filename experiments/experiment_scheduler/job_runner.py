import time
import random
import paramiko
from ssh_handler import execute_ssh_cmd, SSHExecutionError


def run_job(ssh, host, config):
    """
    Simulate running an experiment on a host with a given configuration.

    Args:
        ssh (ssh): The ssh connection to use
        host (str): The hostname or identifier of the host.
        config (dict): The experiment configuration.

    Returns:
        dict: The result of the experiment.
    """
    try:
        current_timestamp = int(time.time())

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
                        f"bash {server_path}/build_and_start.sh xdbcserver 2 \"-c{config['compression']} --read-parallelism={config['server_read_par']} --read-partitions={config['server_read_partitions']} --deser-parallelism={config['server_deser_par']} --network-parallelism={config['network_parallelism']} -f{config['format']} -b{config['buff_size']} -p{config['bufpool_size']} -s1 --system={config['system']}\"",
                        True)
        # TODO: fix? maybe check when server has started instead of sleeping
        time.sleep(2)

        start_data_size = execute_ssh_cmd(ssh,
                                          f"echo $(bash {client_path}/experiments_measure_network.sh 'xdbcclient')")
        start_time = time.time()
        execute_ssh_cmd(ssh,
                        f"bash {client_path}/build_and_start.sh xdbcclient 2 '-f{config['format']} -b{config['buff_size']} -p{config['bufpool_size']} -n{config['network_parallelism']} -r{config['client_read_par']} -d{config['client_decomp_par']} -s1 --table={config['table']} -m{config['client_readmode']}' 1")

        elapsed_time = time.time() - start_time
        formatted_time = "{:.2f}".format(elapsed_time)

        # server_pid = execute_ssh_cmd(ssh, "docker exec xdbcserver pgrep xdbc-server")
        # if server_pid.strip():
        execute_ssh_cmd(ssh,
                        """docker exec xdbcserver bash -c 'pids=$(pgrep xdbc-server); if [ "$pids" ]; then kill $pids; fi'""")

        data_size = int(
            execute_ssh_cmd(ssh, f"echo $(bash {client_path}/experiments_measure_network.sh 'xdbcclient')")) - int(
            start_data_size)

        # print(config)

        # Generate a mock result (replace with actual experiment result)
        result = {
            "date": current_timestamp,
            "time": formatted_time,
            "size": data_size,
        }

        # print(f"Complete run on host: {host}, config: {config}, result {result}")

        return result
    except SSHExecutionError as e:
        print(f"Error: {e}")
        return None
# Handle the error or break the loop
