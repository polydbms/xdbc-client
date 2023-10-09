import sys
from configuration import get_experiment_queue
from configuration import params
from configuration import hosts
from job_runner import run_job
from ssh_handler import create_ssh_connections
import queue
import threading
import csv
import time
import signal
from tqdm import tqdm
import os


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


def write_csv_header(filename, config):
    header = ['date', 'host'] + list(config.keys()) + ['time', 'datasize']

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)


# Function to write results to a CSV file in a thread-safe manner
def write_to_csv(filename, host, config, result):
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([result['date']] + [host] + list(config.values()) + [result['time'], result['size']])


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
            pbar.update(1)
            if result is not None:
                write_to_csv(filename, host, config, result)
            elif not ssh_connections[host].get_transport() or not ssh_connections[host].get_transport().is_active():
                break
        # break


def main():
    filename = "measurements/xdbc_experiments_master.csv"

    if not os.path.exists(filename):
        write_csv_header(filename, params)

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

    # TODO: proper ssh connection handling
    for host, ssh in ssh_connections.items():
        print(f"Closing ssh connection to: {host}")
        ssh.close()

    print("All threads have finished.")


def process_results(results):
    """
    Process and analyze experiment results.

    Args:
        results (list): A list of experiment results.
    """
    # Implement logic to process and analyze experiment results
    # You can aggregate, analyze, and save the results as needed


if __name__ == "__main__":
    main()
