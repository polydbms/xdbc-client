from datetime import datetime
from pathlib import Path
import pandas as pd
import os
import threading
from queue import Queue

from experiments.experiment_scheduler.ssh_handler import SSHConnection
from experiments.model_optimizer import data_transfer_wrapper
from experiments.model_optimizer.Configs import *
from experiments.model_optimizer.additional_environments import *
from experiments.model_optimizer.model_implementations.syne_tune_ask_tell import Syne_Tune_Ask_Tell


def process_configuration(queue, environment, ssh_host, output_file, lock):
    """
    #todo

    Parameters:
        queue
        environment (dict): Dictionary containing the values for the environment (server_cpu,client_cpu,network).
        ssh_host (str): The shh host to use for executing the transfers.
        output_file (str): The file into which the results should be saved.
        lock (lock): Lock to lock the output file to not permit parallel writes.
    """
    while not queue.empty():
        config_id, config = queue.get()
        try:
            print(f"[{datetime.today().strftime('%H:%M:%S')}] starting to execute config {config_id} for environment {environment_to_string(environment)} on {ssh_host}  {config}")

            complete_config = create_complete_config(environment, 'time', 'dict', config)

            #run transfer
            ssh = SSHConnection(ssh_host, "bene")
            result = data_transfer_wrapper.transfer(complete_config, max_retries=1, ssh=ssh, i=config['config_id'])
            ssh.close()

            result['config_id'] = config_id
            with lock:
                df = pd.DataFrame(result, index=[0])
                if os.path.isfile(output_file):
                    df.to_csv(output_file, mode='a', header=False, index=False)
                else:
                    df.to_csv(output_file, mode='a', header=True, index=False)

            print(f"[{datetime.today().strftime('%H:%M:%S')}] finished executing  config {config_id} for environment {environment_to_string(environment)} on {ssh_host}  in {result['time']} seconds")

        finally:
            queue.task_done()


def execute_all_configurations(config_file, output_dir, ssh_hosts, count):
    """
    #todo

    Parameters:
        config_file (str): The file containing the configurations to be executed.
        output_dir (str): The directory into which the results should be saved
        ssh_hosts (list): The list of shh host to use for executing the transfers.
    """
    # read the configurations to be executed
    df = pd.read_csv(config_file)
    configs = df.to_dict(orient="records")[:count]  # converts dataframe to LIST of dicts

    #environments = [environment_1,environment_2,environment_3,environment_4,environment_5,environment_6,environment_7,environment_8,environment_9]#environments_list
    #environments = [environment_9,environment_5]#environments_list
    #environments = all_additional_environments
    #environments = envs_scale_network_test
    # environments = [environment_50, environment_51, environment_52, environment_53, environment_54, environment_55, environment_56, environment_57,
    #                 environment_70, environment_71, environment_72, environment_73, environment_74, environment_75, environment_76]
    environments = envs_scale_compute_test

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for environment in environments:
        output_file = os.path.join(output_dir, f"{environment_to_string(environment)}_random_samples.csv")

        # check for already executed configurations
        if os.path.exists(output_file):
            df_out = pd.read_csv(output_file)
            if "config_id" in df_out.columns:
                executed_configs = df_out["config_id"].tolist()
            else:
                executed_configs = []
        else:
            executed_configs = []

        # filter configurations to skip already executed ones
        remaining_configs = [
            (i, config) for i, config in enumerate(configs) if i not in executed_configs
        ]

        if not remaining_configs:
            print(f"[{datetime.today().strftime('%H:%M:%S')}] All configurations already executed for {environment_to_string(environment)}.")
            continue

        queue = Queue()
        for config_id, config in remaining_configs:
            queue.put((config_id, config))

        lock = threading.Lock()

        # start threads
        threads = []
        for i in range(len(ssh_hosts)):
            thread = threading.Thread(target=process_configuration,
                                      args=(queue, environment, ssh_hosts[i], output_file, lock))
            thread.start()
            threads.append(thread)

        # wait for threads to finish
        for thread in threads:
            thread.join()

        print(f"[{datetime.today().strftime('%H:%M:%S')}] Completed execution for {environment_to_string(environment)}. Results saved to {output_file}.")


def generate_random_configurations(n=1000):
    config_space_string = get_config_space_string(CONFIG_SPACE)

    filename = f"grid_configurations.csv"
    filepath = f"random_samples_{config_space_string}/"

    Path(filepath).mkdir(parents=True, exist_ok=True)

    results = pd.DataFrame()
    first_write_done = False

    optimizer = Syne_Tune_Ask_Tell(config_space=CONFIG_SPACE,
                                   underlying='grid_search')

    for i in range(0, n):

        suggested_config = optimizer.suggest()

        optimizer.report(suggested_config, {'time': 1})

        suggested_config['config_id'] = i

        df = pd.DataFrame(suggested_config, index=[0])

        if not first_write_done:
            df.to_csv(filepath + filename, mode='a', header=True, index=False)
            first_write_done = True
        else:
            df.to_csv(filepath + filename, mode='a', header=False, index=False)

        results = pd.concat([results, df], axis=0)


CONFIG_SPACE = config_space_variable_parameters_generalized_1310k

if __name__ == "__main__":

    #generate_random_configurations()

    config_space_string = get_config_space_string(CONFIG_SPACE)

    config_file = f"random_samples_{config_space_string}/grid_configurations_FIXED.csv"
    output_dir = f"random_samples_{config_space_string}"

    ssh_hosts = ["cloud-7.dima.tu-berlin.de", "cloud-8.dima.tu-berlin.de", "cloud-9.dima.tu-berlin.de", "cloud-10.dima.tu-berlin.de"]
    #ssh_hosts = [ "cloud-8.dima.tu-berlin.de", "cloud-10.dima.tu-berlin.de"]

    for i in [4, 20, 32, 44, 66, 88, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000]:
        print(f"starting executing with n = {i}")
        execute_all_configurations(config_file, output_dir, ssh_hosts, i)
