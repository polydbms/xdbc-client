import itertools
import queue
import random
import csv

hosts = [
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
    "run": [1],
    "system": ["csv"],
    "table": ["test_10000000"],
    "compression": ["nocomp", "zstd", "snappy", "lzo", "lz4", "zlib", "cols"],
    "format": [1, 2],
    "network_parallelism": [1, 2, 4],
    "bufpool_size": [1000],
    "buff_size": [1000, 10000],
    "network": [100, 50, 6],
    "client_readmode": [2],
    "client_cpu": [7, 1, .2],
    "client_read_par": [1, 2, 4],
    "client_decomp_par": [1, 2, 4, 8],
    "server_cpu": [7, 1, .2],
    "server_read_par": [1, 2, 4, 8],
    "server_read_partitions": [1, 2, 4, 8],
    "server_deser_par": [1, 2, 4, 8],
}


def generate_param_combinations():
    # Extract parameter names and values from the configurations dictionary
    param_names = list(params.keys())
    param_values = list(params.values())

    # Generate all possible combinations of parameter values
    param_combinations = list(itertools.product(*param_values))

    # Create a list of dictionaries, each representing a parameter combination
    param_dicts = []
    for combo in param_combinations:
        param_dict = {param_names[i]: combo[i] for i in range(len(param_names))}
        param_dicts.append(param_dict)

    return param_dicts


def get_experiment_queue(filename=None):
    recorded_configs = set()

    # If a filename is provided, read the existing CSV file and extract recorded configurations
    if filename:
        try:
            with open(filename, mode="r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Extract relevant configuration columns from the CSV and convert values to strings
                    config = {key: str(row[key]) for key in params.keys()}
                    recorded_configs.add(tuple(config.values()))
        except FileNotFoundError:
            # If the file doesn't exist, there are no recorded configurations
            pass

    # Generate all possible configurations
    configurations = generate_param_combinations()

    # Filter out configurations that have already been recorded, and convert values to strings for comparison
    remaining_configs = [config for config in configurations if
                         tuple(str(val) for val in config.values()) not in recorded_configs]

    # Shuffle the remaining configurations randomly
    random.shuffle(remaining_configs)

    total_combinations = len(configurations)
    recorded_combinations = len(recorded_configs)
    remaining_combinations = len(remaining_configs)

    print(f"Total generated combinations: {total_combinations}")
    print(f"Recorded combinations: {recorded_combinations}")
    print(f"Remaining combinations to run: {remaining_combinations}")

    experiment_queue = queue.Queue()

    # Put all remaining configurations in the queue
    for config in remaining_configs:
        experiment_queue.put(config)

    # Print the first 10 elements in the queue as samples without removing them
    sample_configs = remaining_configs[:10]
    for i, config in enumerate(sample_configs, 1):
        print(f"Sample Configuration {i}: {config}")

    return total_combinations, experiment_queue
