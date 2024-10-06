import math
from itertools import product


class ExhaustiveOptimizer:
    def __init__(self, params):
        self.params = params

    def effective_service_rate(self, base_rate, workers):
        f0 = self.params["f0"]
        a = self.params["a"]
        return base_rate * (1 + f0 * (1 - math.exp(-a * (workers - 1))) / a)

    def calculate_throughput(self, config, throughput_data):
        compression_lib = config['compression_lib']

        workers = {key.replace('_par', ''): value for key, value in config.items() if key != 'compression_lib'}

        if compression_lib and compression_lib != 'nocomp':
            compression_ratio = self.params[f"{compression_lib}_ratio"]
            compression_rate = throughput_data[f"compression_{compression_lib}_par"][1]
            decompression_rate = throughput_data[f"decompression_{compression_lib}_par"][1]
            effective_network_bandwidth = self.params["upper_bounds"]['network'] / compression_ratio
        else:
            compression_ratio = 1
            compression_rate = throughput_data['compression_nocomp_par'][1]
            decompression_rate = throughput_data['decompression_nocomp_par'][1]
            effective_network_bandwidth = self.params["upper_bounds"]['network']

        stage_rates = {}
        for stage in workers:
            if stage in ['compression', 'decompression']:
                stage_key = f"{stage}_{compression_lib}_par" if compression_lib != 'nocomp' else f"{stage}_nocomp_par"
            else:
                stage_key = f"{stage}_par"
            stage_rates[stage] = self.effective_service_rate(throughput_data[stage_key][1], workers[stage])

        effective_rate_compression = stage_rates['compression']
        effective_rate_decompression = stage_rates['decompression']
        effective_rate_processing_server = min(stage_rates['read'], stage_rates['deserialization'], stage_rates['send'])
        effective_rate_processing_client = min(stage_rates['receive'], stage_rates['write'])

        bottleneck_rate = min(effective_rate_compression, effective_rate_decompression,
                              effective_rate_processing_server, effective_rate_processing_client,
                              effective_network_bandwidth)
        return bottleneck_rate

    def find_best_config(self, throughput_data):
        best_config = None
        best_throughput = 0
        max_total_workers_server = self.params["max_total_workers_server"]
        max_total_workers_client = self.params["max_total_workers_client"]

        server_stages = ['read', 'deserialization', 'compression', 'send']
        client_stages = ['receive', 'decompression', 'write']

        # Iterate over each compression library and the option of no compression
        for compression_lib in self.params.get("compression_libraries", ["nocomp"]):
            # Iterate over all possible combinations of worker allocations for all stages
            for worker_combination_server in product(range(1, max_total_workers_server + 1), repeat=len(server_stages)):
                for worker_combination_client in product(range(1, max_total_workers_client + 1),
                                                         repeat=len(client_stages)):
                    workers_server = dict(zip(server_stages, worker_combination_server))
                    workers_client = dict(zip(client_stages, worker_combination_client))

                    total_workers_server = sum(workers_server.values())
                    total_workers_client = sum(workers_client.values())

                    if total_workers_server > max_total_workers_server or total_workers_client > max_total_workers_client:
                        continue

                    config = {f"{stage}_par": workers for stage, workers in
                              {**workers_server, **workers_client}.items()}
                    config["compression_lib"] = compression_lib

                    throughput = self.calculate_throughput(config, throughput_data)
                    if throughput > best_throughput:
                        best_throughput = throughput
                        best_config = config

        return best_config
