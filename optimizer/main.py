from optimizers import ExhaustiveOptimizer, OptimizerHeuristics, OptimizerQueues, CustomOptimizer, \
    ExhaustivePruneOptimizer
from runner import run_xdbserver_and_xdbclient, print_metrics
from config import loader
from config.helpers import Helpers
import pandas as pd
import os


def main():
    # Define parameters
    average_throughputs = False
    sleep = 2
    mode = 2
    src = "csv"
    target = "csv"
    systems = f"{target}_{src}"
    cpus = {
        'server_cpu': 32,
        'client_cpu': 32
    }
    client_container = "xdbcclient"
    if target == "pandas":
        client_container = "xdbcpython"

    # network_bandwidth = loader.upper_bounds[systems][mode]['send']
    network_bandwidth = 125
    throughput_data = loader.throughput_data[systems][mode]
    perf_file = "local_measurements/xdbc_local.csv"
    if (os.path.isfile(perf_file) and average_throughputs):
        columns_to_keep_server = ['transfer_id', 'read_throughput_pb', 'deser_throughput_pb', 'comp_throughput_pb',
                                  'send_throughput_pb']  # Columns from the server DataFrame
        columns_to_keep_client = ['transfer_id', 'rcv_throughput_pb', 'decomp_throughput_pb',
                                  'write_throughput_pb']  # Columns from the client DataFrame

        df_server = pd.read_csv('/tmp/xdbc_server_timings.csv')[columns_to_keep_server]
        df_client = pd.read_csv('/tmp/xdbc_client_timings.csv')[columns_to_keep_client]
        df = pd.merge(df_server, df_client, on='transfer_id', how='inner')
        df_all = pd.read_csv("local_measurements/xdbc_local.csv")
        df_all = df_all[(df_all['source_system'] == src) & (df_all['target_system'] == target)]

        df_merged = pd.merge(df_all, df, left_on='date', right_on='transfer_id', how='inner')

        averaged = Helpers.calculate_average_throughputs(df_merged, throughput_data)
        if averaged != {}:
            throughput_data = averaged

        print("averaged")
        print(averaged)
    # throughput_data = loader.throughput_data_analytics
    optimizer_choice = "optimizer5"
    params = {"f0": 0.65,
              "a": 0.02,
              "upper_bounds": loader.upper_bounds[systems][mode],
              "max_total_workers_server": 32,
              "max_total_workers_client": 32,
              "compression_libraries": ["lzo", "snappy", "nocomp", "lz4", "zstd"],
              "lzo_ratio": 0.38,
              "snappy_ratio": 0.4,
              "lz4_ratio": 0.4,
              "zstd_ratio": 0.3,
              "nocomp_ratio": 1,
              'comp_snappy': 688,
              'comp_lzo': 610,
              'comp_lz4': 622,
              'comp_zstd': 405,
              'decomp_snappy': 1426,
              'decomp_lzo': 710,
              'decomp_lz4': 1625,
              'decomp_zstd': 1240}

    # Choose optimizer
    if optimizer_choice == "optimizer1":
        optimizer = OptimizerHeuristics(params)
    elif optimizer_choice == "optimizer2":
        optimizer = OptimizerQueues(params)
    elif optimizer_choice == "optimizer3":
        optimizer = CustomOptimizer(params)
    elif optimizer_choice == "optimizer4":
        optimizer = ExhaustiveOptimizer(params)
    elif optimizer_choice == "optimizer5":
        optimizer = ExhaustivePruneOptimizer(params)
    else:
        raise ValueError("Invalid optimizer choice")

    # update throughput data if there are available data

    # Generate config
    # best_config = optimizer.find_best_config(throughput_data)
    # throughput = optimizer.calculate_throughput(best_config, throughput_data)

    # best_config = optimizer.opt_with_comp(best_config, throughput_data)
    best_config = loader.default_config
    throughput = optimizer.calculate_throughput(best_config, throughput_data)

    print(best_config)
    best_config['table'] = "iotm"

    t = 0.1
    t = run_xdbserver_and_xdbclient(best_config, systems, mode, cpus, sleep, network_bandwidth)
    print("Actual time:", t)
    print("Estimated time:", 9200 / throughput)
    print("Actual total throughput:", 9200 / t)
    print("Estimated total throughput:", throughput)

    print("Real throughputs:")
    real = print_metrics(dict=True, client_container=client_container)
    print(real)
    print("Estimated throughputs:")
    estimated = optimizer.calculate_throughput(best_config, throughput_data, True)
    print(estimated)
    print("Best Config:")
    print(best_config)

    # Calculate the difference
    modified_real = {
        key.replace('_throughput_pb', ''): value * best_config[key.replace('_throughput_pb', '_par')]
        for key, value in real.items()
        if key.endswith('_throughput_pb')
    }

    # Calculate the difference
    differences = {}

    for key in estimated:
        if key in modified_real:
            differences[key] = estimated[key] - modified_real[key]

    print("Prediction errors:")
    print(differences)


if __name__ == "__main__":
    main()
