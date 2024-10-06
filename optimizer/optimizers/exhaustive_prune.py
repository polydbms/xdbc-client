import math
from itertools import product


class ExhaustivePruneOptimizer:
    def __init__(self, params):
        self.params = params

    def effective_service_rate(self, base_rate, workers):
        f0 = self.params["f0"]
        a = self.params["a"]
        return base_rate * (1 + f0 * (1 - math.exp(-a * (workers - 1))) / a)

    def calculate_throughput(self, config, throughput_data, return_throughputs=False):
        calc_thr = throughput_data.copy()
        for component in throughput_data.keys():
            #print(f'{component} : {calc_thr[component]}')
            calc_thr[component] = self.effective_service_rate(throughput_data[component], config[f"{component}_par"])

        if return_throughputs:
            return calc_thr
        return self.nth_slowest(calc_thr, 0)[1]

    def nth_slowest(self, data, n):
        # Sort the dictionary by value in descending order
        sorted_items = sorted(data.items(), key=lambda item: item[1])

        # Return the n-th item from the sorted list
        return sorted_items[n] if n < len(sorted_items) else None

    def format_config(self, config):
        # Create a new dictionary with updated keys
        formatted_config = {f"{key}_par": value for key, value in config.items()}

        return formatted_config

    def find_best_config(self, throughput_data):
        max_total_workers_server = self.params["max_total_workers_server"]
        max_total_workers_client = self.params["max_total_workers_client"]
        calc_throughputs = throughput_data.copy()

        server_stages = ['read', 'deser', 'comp', 'send']
        client_stages = ['rcv', 'decomp', 'write']

        best_config = {
            'read': 1,
            'deser': 1,
            'comp': 1,
            'send': 1,
            'rcv': 1,
            'decomp': 1,
            'write': 1,
        }

        min_upper_bound_pair = min(self.params['upper_bounds'].items(), key=lambda x: x[1])
        lowest_upper_bound_component = min_upper_bound_pair[0]
        lowest_upper_bound_thr = min_upper_bound_pair[1]

        print(f"min: {lowest_upper_bound_component}: {lowest_upper_bound_thr}")

        sorted_data = dict(sorted(throughput_data.items(), key=lambda item: item[1]))
        workers_server = 4
        workers_client = 3

        slowest = self.nth_slowest(calc_throughputs, 0)
        slowest_component = slowest[0]
        slowest_thr = slowest[1]

        while slowest_thr < lowest_upper_bound_thr and (
                workers_client < max_total_workers_client and workers_server < max_total_workers_server):
            slowest = self.nth_slowest(calc_throughputs, 0)
            slowest_component = slowest[0]
            slowest_thr = slowest[1]
            # print(f"slowest: {slowest}")
            second_slowest_thr = self.nth_slowest(calc_throughputs, 1)[1]

            while workers_client < max_total_workers_client and workers_server < max_total_workers_server and slowest_thr < lowest_upper_bound_thr:
                best_config[slowest_component] = best_config[slowest_component] + 1
                new_throughput = self.effective_service_rate(throughput_data[slowest_component],
                                                             best_config[slowest_component])

                calc_throughputs[slowest_component] = new_throughput
                # print(f"{slowest_component} gets {best_config[slowest_component]} and reaches {new_throughput}")
                if slowest_component in server_stages:
                    workers_server += 1
                if slowest_component in client_stages:
                    workers_client += 1

                if new_throughput >= second_slowest_thr or new_throughput >= lowest_upper_bound_thr:
                    # print(f"{new_throughput} >= {second_slowest_thr} or {new_throughput} >= {lowest_upper_bound_thr}:")
                    break

        print("initial")
        print(throughput_data)
        print("estimated")
        print(calc_throughputs)
        highest_net = min(best_config['send'], best_config['rcv'])
        best_config['send'] = best_config['rcv'] = highest_net
        return self.format_config(best_config)

    def opt_with_comp(self, best_config, throughput_data):
        calc_throughputs = self.calculate_throughput(best_config, throughput_data, True)
        # print(f"min after opt: {calc_throughputs}")
        slowest_comp = self.nth_slowest(calc_throughputs, 0)
        slowest = next(iter(slowest_comp))
        if slowest in ['send', 'rcv']:
            print("Trying to compress")
            throughput_data['send'] = throughput_data['send'] * (1 / self.params['snappy_ratio'])
            throughput_data['rcv'] = throughput_data['rcv'] * (1 / self.params['snappy_ratio'])
            throughput_data['comp'] = self.params['comp_snappy']
            throughput_data['decomp'] = self.params['decomp_snappy']

            self.params['upper_bounds']['send'] *= (1 / self.params['snappy_ratio'])
            self.params['upper_bounds']['rcv'] *= (1 / self.params['snappy_ratio'])
            # print(throughput_data)
            new_best_config = self.find_best_config(throughput_data)
            new_best_config['compression_lib'] = 'snappy'

            new_throughputs = self.calculate_throughput(new_best_config, throughput_data, True)
            new_slowest = self.nth_slowest(new_throughputs, 0)[1]
            print(f"new slowest {new_slowest} vs slowest {slowest_comp[1]}")
            if new_slowest > slowest_comp[1]:
                print("Compression pays off")
                return new_best_config
            else:
                print("Compression does not pay off")
                return best_config
            # print(new_best_config)
            # print("new throughputs")
            # print(self.calculate_throughput(new_best_config, throughput_data, True))
