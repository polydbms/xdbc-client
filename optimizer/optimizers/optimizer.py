import itertools
import math


class OptimizerHeuristics:
    def __init__(self, params):
        self.params = params

    def estimate_speedup(self, parallelism, f):
        # Using Amdahl's Law to estimate speedup
        return 1 / ((1 - f) + (f / parallelism))

    def calculate_throughput(self, config, throughput_data):
        # Dictionary to store estimated throughputs
        estimated_throughput = {}

        for component, parallelism in config.items():
            # print(component)
            # If the component is related to parallelism
            if component in throughput_data and 1 in throughput_data[component]:
                base_throughput = throughput_data[component][1]
                # Adjust f based on the number of threads
                f = self.params['f0'] * math.exp(-self.params['a'] * (parallelism - 1))
                speedup = self.estimate_speedup(parallelism, f)
                estimated_throughput[component] = base_throughput * speedup
            else:
                # For components like buffer sizes
                estimated_throughput[component] = throughput_data[component][parallelism]

        # Calculate overall throughput as the minimum estimated throughput (bottleneck)
        overall_throughput = min(estimated_throughput.values())

        return overall_throughput

    def find_best_config(self, throughput_data):
        # Define possible values for each parameter

        min_par = 1
        max_par = 10
        doubling_range = [2 ** i for i in range(0, max_par.bit_length())]
        incrementing_range = list(range(min_par, max_par + 1))
        double_add = [1, 2] + [2 ** i for i in range(2, 4)] + [10]

        par_range = double_add

        param_ranges = {
            'read_par': par_range,
            'deserialization_par': par_range,
            'compression_par': par_range,
            'send_par': [1],
            'receive_par': [1],
            'decompression_par': par_range,
            'write_par': par_range
        }

        # Generate all possible configurations
        keys, values = zip(*param_ranges.items())
        all_configs = [dict(zip(keys, v)) for v in itertools.product(*values)]

        best_config = None
        best_throughput = 0

        # Evaluate each configuration
        for config in all_configs:
            throughput = self.calculate_throughput(config, throughput_data)
            # print(f"{config}: {throughput}")
            if throughput > best_throughput:
                best_throughput = throughput
                best_config = config

        return best_config
