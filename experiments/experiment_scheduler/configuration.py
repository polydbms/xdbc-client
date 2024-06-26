from itertools import product
from itertools import groupby
import pandas as pd
import queue
import random

# TODO give as parameter
hosts_file_path = '../hostsfile.txt'

# Read the file and store the contents in a list
with open(hosts_file_path, 'r') as file:
    hosts = file.read().splitlines()

    # lan: 1000mbit, 0.3ms latency, 0% loss, wan: 100mbit, 25ms latency, 5% loss
    # "network": [1, 10, 50, 100, 250, 500, 1000],
params = {
    "xdbc_version": [8],
    "run": [1],
    "client_readmode": [1],
    "client_cpu": [16],
    "server_cpu": [16],
    "network": [1000],
    "network_latency": [0],
    "network_loss": [0],
    "system": ["csv"],
    "table": ["lineitem_sf10"],
    "bufpool_size": [2048, 4098, 8192, 16384, 32768],
    "buff_size": [32, 64, 128, 256, 512, 1024],
    "compression": ["nocomp", "zstd", "lz4", "lzo", "snappy"],  # "zstd", "lz4", "lzo", "snappy"
    "format": [1],
    "network_parallelism": [1],
    "client_write_par": [1],
    "client_decomp_par": [1],
    "server_read_partitions": [1],
    "server_read_par": [1],
    "server_deser_par": [1],
    "server_comp_par": [1]
}

base_configs = [

    # {"system": "csv", "bufpool_size": 8192, "buff_size": 128, "compression": "nocomp",
    # "format": 1, "network_parallelism": 1, "client_write_par": 1, "client_decomp_par": 1, "server_read_partitions": 1,
    # "server_read_par": 1, "server_deser_par": 1, "server_comp_par": 1, "client_readmode": 1},
    #    {"system": "csv", "bufpool_size": 1000, "buff_size": 10000, "compression": "nocomp",
    #     "format": 1, "network_parallelism": 1, "client_write_par": 1, "client_decomp_par": 1, "server_read_partitions": 1,
    #     "server_read_par": 1, "server_deser_par": 4, "server_comp_par": 4, "client_readmode": 1},
    #    {"system": "csv", "bufpool_size": 1000, "buff_size": 1000, "compression": "snappy",
    #     "format": 2, "network_parallelism": 2, "client_write_par": 4, "client_decomp_par": 4, "server_read_partitions": 1,
    #     "server_read_par": 1, "server_deser_par": 1, "server_comp_par": 1, "client_readmode": 2}
    #
]

environment_configs = [
    # {"client_cpu": 8.0, "server_cpu": 0.2, "network": 6},
    # {"client_cpu": 8.0, "server_cpu": 0.2, "network": 12},
    # {"client_cpu": 8.0, "server_cpu": 0.2, "network": 25},
    # {"client_cpu": 8.0, "server_cpu": 0.2, "network": 50},
    # {"client_cpu": 8.0, "server_cpu": 0.2, "network": 100},
    # {"client_cpu": 0.2, "server_cpu": 8.0, "network": 6},
    # {"client_cpu": 0.2, "server_cpu": 8.0, "network": 12},
    # {"client_cpu": 0.2, "server_cpu": 8.0, "network": 25},
    # {"client_cpu": 0.2, "server_cpu": 8.0, "network": 50},
    # {"client_cpu": 0.2, "server_cpu": 8.0, "network": 100},
    # {"client_cpu": 8.0, "server_cpu": 1.0, "network": 6},
    # {"client_cpu": 8.0, "server_cpu": 1.0, "network": 12},
    # {"client_cpu": 8.0, "server_cpu": 1.0, "network": 25},
    # {"client_cpu": 8.0, "server_cpu": 1.0, "network": 50},
    # {"client_cpu": 8.0, "server_cpu": 1.0, "network": 100},
    # {"client_cpu": 1.0, "server_cpu": 8.0, "network": 6},
    # {"client_cpu": 1.0, "server_cpu": 8.0, "network": 12},
    # {"client_cpu": 1.0, "server_cpu": 8.0, "network": 25},
    # {"client_cpu": 1.0, "server_cpu": 8.0, "network": 50},
    # {"client_cpu": 1.0, "server_cpu": 8.0, "network": 100},
    # {"client_cpu": 0.2, "server_cpu": 0.2, "network": 6},
    # {"client_cpu": 0.2, "server_cpu": 0.2, "network": 12},
    # {"client_cpu": 0.2, "server_cpu": 0.2, "network": 25},
    # {"client_cpu": 0.2, "server_cpu": 0.2, "network": 50},
    # {"client_cpu": 0.2, "server_cpu": 0.2, "network": 100},
    # {"client_cpu": 1.0, "server_cpu": 1.0, "network": 6},
    # {"client_cpu": 1.0, "server_cpu": 1.0, "network": 12},
    # {"client_cpu": 1.0, "server_cpu": 1.0, "network": 25},
    # {"client_cpu": 1.0, "server_cpu": 1.0, "network": 50},
    # {"client_cpu": 1.0, "server_cpu": 1.0, "network": 100},
    # {"client_cpu": 8.0, "server_cpu": 8.0, "network": 6},
    # {"client_cpu": 8.0, "server_cpu": 8.0, "network": 12},
    # {"client_cpu": 8.0, "server_cpu": 8.0, "network": 25},
    # {"client_cpu": 8.0, "server_cpu": 8.0, "network": 50},
    # {"client_cpu": 16.0, "server_cpu": 16.0, "network": 100}
]


# vary_params = {
#    "table": ["ss13husallm", "lineitem_sf10"],
#    "xdbc_version": [4],
# "server_comp_par": [1, 2, 4, 8],
# "client_decomp_par": [1, 2, 4, 8],
# "buff_size": [1000, 10000],
# "server_read_par": [1, 2],
# "compression": ["nocomp", "snappy", "lz4", "lzo", "zstd"],
# "format": [1, 2],
# "client_write_par": [4, 8],
# "client_readmode": [1, 2]
# }

def get_experiment_queue(filename=None):
    # exclude_columns = ["date", "xdbc_version", "host", "run", "time", "datasize", "avg_cpu_server", "avg_cpu_client"]
    exclude_columns = ["date", "host", "time", "datasize", "avg_cpu_server", "avg_cpu_client"]

    recorded_experiments = []

    if filename:
        try:
            df = pd.read_csv(filename)
            for col in exclude_columns:
                if col in df.columns:
                    df = df.drop(columns=[col])
            recorded_experiments = [frozenset(record.items()) for record in df.to_dict('records')]
        except Exception as e:
            print(f"An error occurred while reading the CSV file: {e}. Proceeding with all possible combinations.")

    all_combinations = []

    if base_configs and environment_configs and len(base_configs) > 0 and len(environment_configs) > 0:
        for base_config in base_configs:
            for env_config in environment_configs:
                combined_env_base = {**base_config, **env_config}
                filtered_params = {k: vary_params[k] for k in vary_params}
                sub_combinations = [dict(zip(filtered_params, v)) for v in product(*filtered_params.values())]
                for sub_comb in sub_combinations:
                    combined_config = {**combined_env_base, **sub_comb}
                    all_combinations.append(combined_config)
    else:
        # Exhaustive search with all combinations of parameters
        keys, values = zip(*params.items())
        all_combinations = [dict(zip(keys, v)) for v in product(*values)]

        # Sort and group by non-environment parameters if provided
        environment_param_keys = ["server_cpu", "client_cpu", "network"]

        def non_env_key(config):
            return tuple(str(config[k]) for k in sorted(config.keys()) if k not in environment_param_keys)

        all_combinations = sorted(all_combinations, key=non_env_key)
        grouped = [(key, list(group)) for key, group in groupby(all_combinations, key=non_env_key)]
        random.shuffle(grouped)
        all_combinations = [item for _, group in grouped for item in group]

    recorded_experiments_set = set(recorded_experiments)
    remaining_combinations = set(frozenset(comb.items()) for comb in all_combinations) - recorded_experiments_set
    remaining_combinations_list = [dict(comb) for comb in remaining_combinations]

    experiment_queue = queue.Queue()
    for item in remaining_combinations_list:
        experiment_queue.put(item)

    total_combinations = len(all_combinations)
    recorded_combinations = len(recorded_experiments_set)
    remaining_combinations = experiment_queue.qsize()

    print(f"Total generated combinations: {total_combinations}")
    print(f"Recorded combinations: {recorded_combinations}")
    print(f"Remaining combinations to run: {remaining_combinations}")
    print("First five configurations in the queue:")
    for _ in range(min(5, experiment_queue.qsize())):
        config = experiment_queue.get()
        print(config)
        experiment_queue.put(config)  # Re-enqueue the configuration

    return total_combinations, experiment_queue
