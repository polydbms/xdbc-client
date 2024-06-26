import sys
from configuration import get_experiment_queue
# from configuration import params
from configuration import hosts
from job_runner import run_job
from ssh_handler import create_ssh_connections, SSHConnection
import queue
import threading
import csv
import time
import signal
from tqdm import tqdm
import os
import glob
import pandas as pd
import shutil


def close_ssh_connections(signum, frame):
    global ssh_connections
    for host, ssh in ssh_connections.items():
        ssh.close()
    exit(0)


total_jobs_size, experiment_queue = get_experiment_queue("measurements/xdbc_experiments_master.csv")
jobs_to_run = experiment_queue.qsize()

pbar = tqdm(total=total_jobs_size, position=0, leave=True)
pbar.update(total_jobs_size - jobs_to_run)
print(f"Starting experiments, jobs to run: {jobs_to_run}")

ssh_connections = create_ssh_connections(hosts, jobs_to_run)
signal.signal(signal.SIGINT, close_ssh_connections)
print(f"Active ssh connections: {len(ssh_connections)}")


# print(ssh_connections)


def write_csv_header(filename, config=None):
    header = ['date', 'xdbc_version', 'host', 'run', 'system', 'table', 'compression', 'format', 'network_parallelism',
              'bufpool_size', 'buff_size', 'network', 'network_latency', 'network_loss', 'client_readmode',
              'client_cpu', 'client_write_par', 'client_decomp_par', 'server_cpu', 'server_read_par',
              'server_read_partitions', 'server_deser_par', 'server_comp_par', 'time', 'datasize', 'avg_cpu_server',
              'avg_cpu_client']

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)


# Function to write results to a CSV file in a thread-safe manner
def write_to_csv(filename, host, config, result):
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [result['date']] + [config['xdbc_version'], host] +
            [config['run'], config['system'], config['table'],
             config['compression'],
             config['format'], config['network_parallelism'],
             config['bufpool_size'],
             config['buff_size'], config['network'], config['network_latency'], config['network_loss'],
             config['client_readmode'],
             config['client_cpu'], config['client_write_par'],
             config['client_decomp_par'],
             config['server_cpu'], config['server_read_par'],
             config['server_read_partitions'],
             config['server_deser_par'],
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
            result = run_job(ssh_connections[host], config)
            if result is not None:
                experiment_queue.task_done()
                pbar.update()
                write_to_csv(filename, host, config, result)
            elif not ssh_connections[host].ssh.get_transport() or not ssh_connections[
                host].ssh.get_transport().is_active():
                # print(f"Adding back to queue: {config}")
                # experiment_queue.put(config)
                break
        # break


def append_if_not_duplicate(file_path, df, unique_id_column, existing_ids):
    # Find rows in df that have IDs not present in existing_ids
    df_to_append = df[~df[unique_id_column].isin(existing_ids)]

    # Append non-duplicate data to the file
    df_to_append.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))


def load_existing_ids(file_path, unique_id_column):
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path, usecols=[unique_id_column])
        return existing_df[unique_id_column].tolist()
    else:
        return []


def concatenate_timings_files(base_dir, ssh_connections):
    current_directory = os.getcwd()
    local_dir = os.path.join(current_directory, base_dir, "local")

    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir)

    for host, ssh in ssh_connections.items():
        ssh.execute_cmd(f"docker cp xdbcserver:/tmp/xdbc_server_timings.csv {local_dir}/{host}_server_timings.csv")
        ssh.execute_cmd(f"docker cp xdbcclient:/tmp/xdbc_client_timings.csv {local_dir}/{host}_client_timings.csv")

    server_files = glob.glob(os.path.join(local_dir, '*_server_timings.csv'))
    client_files = glob.glob(os.path.join(local_dir, '*_client_timings.csv'))

    output_server_file = os.path.join(base_dir, 'concatenated_server_timings.csv')
    output_client_file = os.path.join(base_dir, 'concatenated_client_timings.csv')

    # Load existing unique IDs
    existing_server_ids = load_existing_ids(output_server_file, 'transfer_id')
    existing_client_ids = load_existing_ids(output_client_file, 'transfer_id')

    # Concatenate data from server files
    for server_file in server_files:
        try:
            df = pd.read_csv(server_file)
            append_if_not_duplicate(output_server_file, df, 'transfer_id', existing_server_ids)
        except Exception as e:
            print(f'Problem with file:{server_file}. Exception:{e}')

    # Concatenate data from client files
    for client_file in client_files:
        try:
            df = pd.read_csv(client_file)
            append_if_not_duplicate(output_client_file, df, 'transfer_id', existing_client_ids)
        except Exception as e:
            print(f'Problem with file:{client_file}. Exception:{e}')

    for file_path in glob.glob(os.path.join(local_dir, '*_timings.csv')):
        os.remove(file_path)

    # Optionally, remove the local_dir if it's no longer needed
    shutil.rmtree(local_dir)


def main():
    # check and create dir/file if necessary
    directory = "measurements"
    filename = os.path.join(directory, "xdbc_experiments_master.csv")
    if not os.path.exists(directory):
        os.makedirs(directory)
    if not os.path.exists(filename):
        write_csv_header(filename)

    # create the internal statistics files
    for host, ssh in ssh_connections.items():
        # Execute the command on each SSH connection
        ssh.execute_cmd(
            'docker exec xdbcserver bash -c "[ ! -f /tmp/xdbc_server_timings.csv ] && echo \'transfer_id,total_time,read_wait_time,read_time,deser_wait_time,deser_time,compression_wait_time,compression_time,network_wait_time,network_time\' > /tmp/xdbc_server_timings.csv"')
        ssh.execute_cmd(
            'docker exec xdbcclient bash -c "[ ! -f /tmp/xdbc_client_timings.csv ] && echo \'transfer_id,total_time,rcv_wait_time,rcv_time,decomp_wait_time,decomp_time,write_wait_time,write_time\' > /tmp/xdbc_client_timings.csv"')

    # Create and start the threads
    num_workers = len(hosts)
    # print(num_workers)

    threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=worker, args=(hosts[i], filename))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # clean up csv files if any
    # for host, ssh in ssh_connections.items():
    #    for tbl in params['table']:
    #        print(tbl)
    #        execute_ssh_cmd(ssh, f"rm -f /dev/shm/{tbl}_*.csv")
    #        execute_ssh_cmd(ssh, f"docker exec xdbcclient bash -c 'rm -f /dev/shm/output*.csv'")

    print("All threads have finished. Collecting internal statistics files")

    concatenate_timings_files("measurements", ssh_connections)
    # TODO: proper ssh connection handling

    for host, ssh in ssh_connections.items():
        print(f"Closing ssh connection to: {host}")
        ssh.close()


if __name__ == "__main__":
    main()
