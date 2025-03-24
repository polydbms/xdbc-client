import itertools
import datetime
import os
import csv
import random
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file

repetitions = 1
test_env = next((env for env in test_envs if env['name'] == "testb"), None)

# Prepare environment
env = test_env['env']
csv_file_path = "res/figure7b.csv"

set_env(env)

# Define the range for server parallelism parameters (1 to 8)
server_pars = ['read_par', 'deser_par', 'comp_par', 'send_par']
server_range = [1, 2, 4, 8]

# Define the range for client parallelism parameters
client_pars = ['rcv_par', 'decomp_par', 'ser_par', 'write_par']
rcv_send_par_range = [1]  # rcv_par and send_par have a smaller range
client_range = [1, 2, 4, 8]

# Define the formats
formats = [1]

# Define the compression libraries
compression_libs = ['nocomp', 'zstd', 'lzo', 'lz4', 'snappy']

# Array to hold configurations
configurations = []

# Generate all combinations
for server_values in itertools.product(server_range, repeat=len(server_pars) - 1):  # Exclude send_par
    for rcv_send_par_value in rcv_send_par_range:
        for client_values in itertools.product(client_range, repeat=len(client_pars) - 1):  # Exclude rcv_par
            for format_value in formats:
                for compression_lib in compression_libs:
                    # Construct the configuration dictionary
                    config = dict(zip(server_pars[:-1], server_values))  # Fill other server parameters
                    config['send_par'] = rcv_send_par_value
                    config['rcv_par'] = rcv_send_par_value  # Ensure rcv_par equals send_par
                    config.update(dict(zip(client_pars[1:], client_values)))  # Fill remaining client parameters
                    config['format'] = format_value

                    # Ensure buffer sizes are large enough
                    num_threads = sum(server_values) + sum(client_values) + rcv_send_par_value
                    config['buffer_size'] = 1024  # Always fixed
                    config['server_buffpool_size'] = (sum(server_values) + rcv_send_par_value + 4) * 1024 * 3
                    config['client_buffpool_size'] = (sum(client_values) + rcv_send_par_value + 4) * 1024 * 3
                    config['compression_lib'] = compression_lib
                    # Append configuration to the list
                    configurations.append(config)

# Shuffle the configurations
random.shuffle(configurations)

# Load existing configurations from CSV
existing_configs = set()
if os.path.exists(csv_file_path):
    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            existing_configs.add(tuple(row[key] for key in ['compression_lib'] + list(config.keys())))

# Filter out existing configurations
filtered_configurations = [
    config for config in configurations
    if tuple(map(str, [config['compression_lib']] + list(config.values()))) not in existing_configs
]

# Print information about configurations
print(f"Generated configurations: {len(configurations)}")
print(f"Existing configurations: {len(existing_configs)}")
print(f"Remaining configurations: {len(filtered_configurations)}")

# Print the first 10 configurations for verification
print("First 10 configurations:")
for config in filtered_configurations[:10]:
    print(config)

# Run the configurations
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
for config in filtered_configurations:
    for i in range(repetitions):
        config['skip_ser'] = 0

        t = run_xdbserver_and_xdbclient(config, env, perf_dir)

        print(f"xdbc for {config}: {t} s")

        timestamp = int(datetime.datetime.now().timestamp())

        # Add headers dynamically if the file does not exist or is empty
        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)

            # Write header row dynamically
            if file.tell() == 0:  # Check if file is empty
                header = ['timestamp', 'env_name', 'iteration', 'table'] + list(config.keys()) + ['time']
                writer.writerow(header)

            # Write data row dynamically
            row = [timestamp, test_env['name'], i + 1, "lineitem_sf10"] + list(config.values()) + [t]
            writer.writerow(row)
