import sys
from configuration import get_experiment_queue
from configuration import params
from configuration import hosts
from job_runner import run_job
from ssh_handler import create_ssh_connections, execute_ssh_cmd
import queue
import threading
import csv
import time
import signal
from tqdm import tqdm
import os
import glob
import pandas as pd


def close_ssh_connections(signum, frame):
    global ssh_connections
    for host, ssh in ssh_connections.items():
        transport = ssh.get_transport()
        if transport is not None and transport.is_active():
            print(f"Closing ssh connection to: {host}")
            ssh.close()
        else:
            print(f"SSH connection to {host} is already closed or not active.")
    exit(0)


total_jobs_size, experiment_queue = get_experiment_queue("measurements/xdbc_experiments_master.csv")
jobs_to_run = experiment_queue.qsize()

pbar = tqdm(total=total_jobs_size, position=0, leave=True)
pbar.update(total_jobs_size - jobs_to_run)
print(f"Starting experiments, jobs to run: {jobs_to_run}")

ssh_connections = create_ssh_connections(hosts)
signal.signal(signal.SIGINT, close_ssh_connections)

# print(ssh_connections)
xdbc_version = 2


def write_csv_header(filename, config):
    header = ['date', 'xdbc_version', 'host', 'run', 'system', 'table', 'compression', 'format', 'network_parallelism',
              'bufpool_size',
              'buff_size', 'network', 'client_readmode', 'client_cpu', 'client_write_par', 'client_decomp_par',
              'server_cpu', 'server_read_par', 'server_read_partitions', 'server_deser_par', 'server_comp_par', 'time',
              'datasize', 'avg_cpu_server', 'avg_cpu_client']

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)


# Function to write results to a CSV file in a thread-safe manner
def write_to_csv(filename, host, config, result):
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [result['date']] + [xdbc_version, host] + [1, config['system'], config['table'], config['compression'],
                                                       config['format'], config['network_parallelism'],
                                                       config['bufpool_size'],
                                                       config['buff_size'], config['network'],
                                                       config['client_readmode'],
                                                       config['client_cpu'], config['client_write_par'],
                                                       config['client_decomp_par'],
                                                       config['server_cpu'], config['server_read_par'],
                                                       config['server_read_partitions'], config['server_deser_par'],
                                                       config['server_comp_par']] +
            [result['time'], result['size'], result['avg_cpu_server'], result['avg_cpu_client']])


# Function to be executed by each thread
def worker(host, filename):
    while True:
        try:
            config = experiment_queue.get(timeout=1)
        except queue.Empty:
            break
        else:
            result = run_job(ssh_connections[host], host, config)
            experiment_queue.task_done()
            pbar.update()
            if result is not None:
                write_to_csv(filename, host, config, result)
            elif not ssh_connections[host].get_transport() or not ssh_connections[host].get_transport().is_active():
                # print(f"Adding back to queue: {config}")
                # experiment_queue.put(config)
                break
        # break


def concatenate_timings_files(input_dir):
    # Get a list of all _server_timings.csv and _client_timings.csv files in the input directory
    server_files = glob.glob(os.path.join(input_dir, '*_server_timings.csv'))
    client_files = glob.glob(os.path.join(input_dir, '*_client_timings.csv'))

    # Initialize empty DataFrames to hold the concatenated data for server and client
    concatenated_server_data = pd.DataFrame()
    concatenated_client_data = pd.DataFrame()

    # Concatenate data from server files
    for server_file in server_files:
        df = pd.read_csv(server_file)
        concatenated_server_data = pd.concat([concatenated_server_data, df], ignore_index=True)

    # Concatenate data from client files
    for client_file in client_files:
        df = pd.read_csv(client_file)
        concatenated_client_data = pd.concat([concatenated_client_data, df], ignore_index=True)

    # Save the concatenated data to the output files in the input directory
    output_server_file = os.path.join(input_dir, 'concatenated_server_timings.csv')
    output_client_file = os.path.join(input_dir, 'concatenated_client_timings.csv')
    concatenated_server_data.to_csv(output_server_file, index=False)
    concatenated_client_data.to_csv(output_client_file, index=False)

    # Remove individual files
    for server_file in server_files:
        os.remove(server_file)
    for client_file in client_files:
        os.remove(client_file)


def main():
    # check and create dir/file if necessary
    directory = "measurements"
    filename = os.path.join(directory, "xdbc_experiments_master.csv")
    if not os.path.exists(directory):
        os.makedirs(directory)
    if not os.path.exists(filename):
        write_csv_header(filename, params)

    # create the internal statistics files
    for host, ssh in ssh_connections.items():
        # Execute the command on each SSH connection
        execute_ssh_cmd(ssh,
                        'docker exec xdbcserver bash -c "[ ! -f /tmp/xdbc_server_timings.csv ] && echo \'transfer_id,total_time,read_wait_time,read_time,deser_wait_time,deser_time,compression_wait_time,compression_time,network_wait_time,network_time\' > /tmp/xdbc_server_timings.csv"')
        execute_ssh_cmd(ssh,
                        'docker exec xdbcclient bash -c "[ ! -f /tmp/xdbc_client_timings.csv ] && echo \'transfer_id,total_time,rcv_wait_time,rcv_time,decomp_wait_time,decomp_time,write_wait_time,write_time\' > /tmp/xdbc_client_timings.csv"')

    # Create and start the threads
    num_workers = len(hosts)
    # print(num_workers)

    threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=worker, args=(hosts[i], filename,))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All threads have finished. Collecting internal statistics files")
    current_directory = os.getcwd()
    for host, ssh in ssh_connections.items():
        execute_ssh_cmd(ssh,
                        f"docker cp xdbcserver:/tmp/xdbc_server_timings.csv {current_directory}/measurements/{host}_server_timings.csv")

        execute_ssh_cmd(ssh,
                        f"docker cp xdbcclient:/tmp/xdbc_client_timings.csv {current_directory}/measurements/{host}_client_timings.csv")

    concatenate_timings_files("measurements")
    # TODO: proper ssh connection handling
    for host, ssh in ssh_connections.items():
        print(f"Closing ssh connection to: {host}")
        ssh.close()


if __name__ == "__main__":
    main()
