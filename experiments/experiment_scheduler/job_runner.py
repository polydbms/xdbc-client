import time
import json
from ssh_handler import SSHConnectionError


def run_job(ssh, config):
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

        # if config['system'] == 'csv':
        # total_lines = execute_ssh_cmd(ssh,
        # f"echo $(docker exec xdbcserver bash -c 'wc -l </dev/shm/{config['table']}.csv')")
        # f"echo $(wc -l </dev/shm/{config['table']}.csv)")

        # lines_per_file = execute_ssh_cmd(ssh,
        #                                 f"echo $((({total_lines} + {config['server_read_par']} - 1) / {config['server_read_par']}))")

        # execute_ssh_cmd(ssh,
        # f"docker exec xdbcserver bash -c 'cd /dev/shm/ && split -d --lines={lines_per_file} {config['table']}.csv --additional-suffix=.csv {config['table']}_'")
        # f"cd /dev/shm/ && split -d --lines={lines_per_file} {config['table']}.csv --additional-suffix=.csv {config['table']}_")

        ssh.execute_cmd(f"docker update --cpus {config['server_cpu']} xdbcserver")
        ssh.execute_cmd(f"docker update --cpus {config['client_cpu']} xdbcclient")

        ssh.execute_cmd(f"curl -X DELETE localhost:4080/xdbcserver && curl -X PUT localhost:4080/xdbcserver")
        ssh.execute_cmd(f"curl -X DELETE localhost:4080/xdbcclient && curl -X PUT localhost:4080/xdbcclient")

        # ssh.execute_cmd(
        #    f"curl -s -d 'rate={config['network']}mbps&delay={config['network_latency']}ms&loss={config['network_loss']}%' localhost:4080/xdbcserver")

        ssh.execute_cmd(
            #    f"curl -s -d'limit={config['network']}mbps&delay={config['network_latency']}ms&loss={config['network_loss']}%' localhost:4080/xdbcclient")
            f"curl -s -d 'rate={config['network']}mbps' localhost:4080/xdbcclient")
        # f"curl -s -d 'rate={config['network']}mbps&delay={config['network_latency']}ms' localhost:4080/xdbcclient")
        #   f"curl -s -d 'rate={config['network']}mbps&loss={config['network_loss']}%' localhost:4080/xdbcclient")
        # ssh.execute_cmd("docker exec -it xdbcclient /bin/bash -c 'tc qdisc del dev eth0 root'")

        #ssh.execute_cmd(f"curl -s -d 'rate={config['network']}mbps' localhost:4080/xdbcserver")
        # ssh.execute_cmd(
        #    f"docker exec -it xdbcclient /bin/bash -c 'tc qdisc add dev eth0 root handle 1: htb default 10 && tc class add dev eth0 parent 1: classid 1:1 htb rate ${config['network']}mbps && tc qdisc add dev eth0 parent 1:1 handle 10: netem delay ${config['network_latency']}ms loss ${config['network_loss']}%'"
        # )

        ssh.execute_cmd(
            f"bash {server_path}/build_and_start.sh xdbcserver 2 \"--transfer-id={current_timestamp} -c{config['compression']} --read-parallelism={config['server_read_par']} --read-partitions={config['server_read_partitions']} --deser-parallelism={config['server_deser_par']} --compression-parallelism={config['server_comp_par']} --network-parallelism={config['network_parallelism']} -f{config['format']} -b{config['buff_size']} -p{config['bufpool_size']} -s1 --system={config['system']}\""
            # ,True
        )
        # TODO: fix? maybe check when server has started instead of sleeping

        sleep_time = 2
        if config['server_cpu'] == 0.2:
            sleep_time *= 8
        time.sleep(sleep_time)

        start_data_size = ssh.execute_cmd(
            f"echo $(bash {client_path}/experiments_measure_network.sh 'xdbcclient')")

        ssh.execute_cmd(f"rm -rf /tmp/stop_monitoring")
        ssh.execute_cmd(f"touch /tmp/start_monitoring")
        ssh.execute_cmd(f"bash {client_path}/experiments_measure_resources.sh xdbcserver xdbcclient", True)

        start_time = time.time()
        ssh.execute_cmd(
            f"bash {client_path}/build_and_start.sh xdbcclient 2 '--transfer-id={current_timestamp} -f{config['format']} -b{config['buff_size']} -p{config['bufpool_size']} -n{config['network_parallelism']} -r{config['client_write_par']} -d{config['client_decomp_par']} -s1 --table={config['table']} -m{config['client_readmode']}' 1")

        elapsed_time = time.time() - start_time
        formatted_time = "{:.2f}".format(elapsed_time)

        ssh.execute_cmd(f"touch /tmp/stop_monitoring")
        # Polling for the JSON file
        json_file_path = "/tmp/resource_metrics.json"
        timeout = 10
        poll_interval = 1  # second
        total_time = 0

        while total_time < timeout:
            time.sleep(poll_interval)
            total_time += poll_interval
            stdout = ssh.execute_cmd(f"test -f {json_file_path} && echo 'exists' || echo 'not exists'")
            if 'exists' == stdout:
                break

        ssh.execute_cmd(f"pkill -f experiments_measure_resources.sh")
        ssh.execute_cmd(f"rm -rf /tmp/stop_monitoring")
        ssh.execute_cmd(f"rm -rf /tmp/start_monitoring")

        ssh.execute_cmd(
            """docker exec xdbcserver bash -c 'pids=$(pgrep xdbc-server); if [ "$pids" ]; then kill $pids; fi'""")
        ssh.execute_cmd(
            """docker exec xdbcclient bash -c 'pids=$(pgrep xdbc-client); if [ "$pids" ]; then kill $pids; fi'""")
        # execute_ssh_cmd(ssh, "cd ~/xdbc/xdbc-client && docker-compose -f docker-xdbc.yml restart xdbc-server")
        # execute_ssh_cmd(ssh, "cd ~/xdbc/xdbc-client && docker-compose -f docker-xdbc.yml restart xdbc-client")

        # time.sleep(3)
        resource_metrics_json = ssh.execute_cmd(
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
            print(f"host {ssh.hostname} JSON decoding failed: {e}")
            print(f"Invalid JSON content: {resource_metrics_json}")

        data_size = int(
            ssh.execute_cmd(f"echo $(bash {client_path}/experiments_measure_network.sh 'xdbcclient')")) - int(
            start_data_size)
        ssh.execute_cmd("rm -f /tmp/resource_metrics.json")
        # print(config)

        result = {
            "date": current_timestamp,
            "time": formatted_time,
            "size": data_size,
            "avg_cpu_server": avg_cpu_server,
            "avg_cpu_client": avg_cpu_client
        }

        # clean up input/output files

        ssh.execute_cmd(f"rm -f /dev/shm/{config['table']}_*.csv")
        ssh.execute_cmd(f"docker exec xdbcclient bash -c 'rm -f /dev/shm/output*.csv'")

        # print(f"Complete run on host: {host}, config: {config}, result {result}")

        return result
    except SSHConnectionError as e:
        print(f"Error: {e}")
        return None
# Handle the error or break the loop
