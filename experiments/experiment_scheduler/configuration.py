from itertools import product
from itertools import groupby
import pandas as pd
import queue
import random

hosts = [
    "sr630-wn-a-02.dima.tu-berlin.de",
    "sr630-wn-a-03.dima.tu-berlin.de",
    "sr630-wn-a-04.dima.tu-berlin.de",
    "sr630-wn-a-05.dima.tu-berlin.de",
    "sr630-wn-a-06.dima.tu-berlin.de",
    "sr630-wn-a-07.dima.tu-berlin.de",
    "sr630-wn-a-08.dima.tu-berlin.de",
    "sr630-wn-a-09.dima.tu-berlin.de",
    "sr630-wn-a-10.dima.tu-berlin.de",
    "sr630-wn-a-11.dima.tu-berlin.de",
    "sr630-wn-a-12.dima.tu-berlin.de",
    "sr630-wn-a-13.dima.tu-berlin.de",
    "sr630-wn-a-14.dima.tu-berlin.de",
    "sr630-wn-a-15.dima.tu-berlin.de",
    "sr630-wn-a-16.dima.tu-berlin.de",
    "sr630-wn-a-17.dima.tu-berlin.de",
    "sr630-wn-a-18.dima.tu-berlin.de",
    "sr630-wn-a-19.dima.tu-berlin.de",
    "sr630-wn-a-20.dima.tu-berlin.de",
    "sr630-wn-a-21.dima.tu-berlin.de",
    "sr630-wn-a-22.dima.tu-berlin.de",
    "sr630-wn-a-23.dima.tu-berlin.de",
    "sr630-wn-a-24.dima.tu-berlin.de",
    "sr630-wn-a-25.dima.tu-berlin.de",
    "sr630-wn-a-26.dima.tu-berlin.de",
    "sr630-wn-a-27.dima.tu-berlin.de",
    "sr630-wn-a-28.dima.tu-berlin.de",
    "sr630-wn-a-29.dima.tu-berlin.de",
    "sr630-wn-a-30.dima.tu-berlin.de",
    "sr630-wn-a-31.dima.tu-berlin.de",
    "sr630-wn-a-32.dima.tu-berlin.de",
    "sr630-wn-a-33.dima.tu-berlin.de",
    "sr630-wn-a-34.dima.tu-berlin.de",
    "sr630-wn-a-35.dima.tu-berlin.de",
    "sr630-wn-a-36.dima.tu-berlin.de",
    "sr630-wn-a-37.dima.tu-berlin.de",
    "sr630-wn-a-38.dima.tu-berlin.de",
    "sr630-wn-a-39.dima.tu-berlin.de",
    "sr630-wn-a-40.dima.tu-berlin.de",
    "sr630-wn-a-41.dima.tu-berlin.de",
    "sr630-wn-a-42.dima.tu-berlin.de",
    "sr630-wn-a-43.dima.tu-berlin.de",
    "sr630-wn-a-44.dima.tu-berlin.de",
    "sr630-wn-a-45.dima.tu-berlin.de",
    "sr630-wn-a-46.dima.tu-berlin.de",
    "sr630-wn-a-47.dima.tu-berlin.de",
    "sr630-wn-a-48.dima.tu-berlin.de",
    "sr630-wn-a-49.dima.tu-berlin.de",
    "sr630-wn-a-50.dima.tu-berlin.de",
    "sr630-wn-a-51.dima.tu-berlin.de",
    "sr630-wn-a-52.dima.tu-berlin.de",
    "sr630-wn-a-53.dima.tu-berlin.de",
    "sr630-wn-a-55.dima.tu-berlin.de",
    "sr630-wn-a-56.dima.tu-berlin.de",
    "sr630-wn-a-57.dima.tu-berlin.de",
    "sr630-wn-a-58.dima.tu-berlin.de",
    "sr630-wn-a-59.dima.tu-berlin.de",
    "sr630-wn-a-60.dima.tu-berlin.de"
]

params = {
    "client_cpu": [.2, 1, 8],
    "server_cpu": [.2, 1, 8],
    "network": [6, 100],
    "system": ["csv"],
    "table": ["test_10000000"],
    "bufpool_size": [1000],
    "buff_size": [10000],
    "compression": ["nocomp"],
    "format": [1],
    "network_parallelism": [1],
    "client_readmode": [2],
    "client_write_par": [1],
    "client_decomp_par": [1],
    "server_read_par": [1, 2, 4, 8],
    "server_read_partitions": [1, 2, 4, 8],
    "server_deser_par": [4],
    "server_comp_par": [4]
}


def get_experiment_queue(filename=None):
    # Columns that should not be considered when comparing combinations
    exclude_columns = ["xdbc_version", "run", "date", "host", "time", "datasize", "avg_cpu_server", "avg_cpu_client"]

    recorded_experiments = []

    if filename:
        try:
            df = pd.read_csv(filename)

            # Remove excluded columns for an accurate comparison
            for col in exclude_columns:
                if col in df.columns:
                    df = df.drop(columns=[col])

            # Convert DataFrame records to dictionaries and then to a set of frozensets for faster operations
            recorded_experiments = [frozenset(record.items()) for record in df.to_dict('records')]

        except Exception as e:
            print(f"An error occurred while reading the CSV file: {e}. Proceeding with all possible combinations.")

    # Generate all possible combinations from the parameters
    keys, values = zip(*params.items())
    all_combinations = [frozenset(zip(keys, v)) for v in product(*values)]

    # Convert the list of recorded experiments to a set for faster membership checking
    recorded_experiments_set = set(recorded_experiments)

    # Find the difference between all possible combinations and recorded experiments
    remaining_combinations = set(all_combinations) - recorded_experiments_set

    # Convert the frozensets back to dictionaries
    remaining_combinations_list = [dict(comb) for comb in remaining_combinations]

    # Shuffle the remaining combinations
    # random.shuffle(remaining_combinations_list)

    # Sort by non-environment parameters
    environment_params = ["client_cpu", "server_cpu", "network"]

    # Function to get the key for non-environment parameters
    def non_env_key(config):
        return tuple(str(config[k]) for k in sorted(config.keys()) if k not in environment_params)

    # Group the configurations based on non-environment parameters
    sorted_list = sorted(remaining_combinations_list, key=non_env_key)
    grouped = [(key, list(group)) for key, group in groupby(sorted_list, key=non_env_key)]

    # Shuffle the groups
    random.shuffle(grouped)

    # Flatten the shuffled groups back to a list
    shuffled_grouped_list = []
    for _, group in grouped:
        shuffled_grouped_list.extend(group)

    remaining_combinations_list = shuffled_grouped_list

    print('\n'.join(str(comb) for comb in remaining_combinations_list[:5]))

    # Initialize a queue and add the remaining combinations to it
    experiment_queue = queue.Queue()
    for item in remaining_combinations_list:
        experiment_queue.put(item)

    # Counters
    total_combinations = len(all_combinations)
    recorded_combinations = len(recorded_experiments_set)
    remaining_combinations = experiment_queue.qsize()  # Getting the size of the queue

    # Print the statistics
    print(f"Total generated combinations: {total_combinations}")
    print(f"Recorded combinations: {recorded_combinations}")
    print(f"Remaining combinations to run: {remaining_combinations}")

    # Return the total number of combinations and the experiment queue
    return total_combinations, experiment_queue
