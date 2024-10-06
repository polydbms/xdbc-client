import math


class CustomOptimizer:
    def __init__(self, params):
        self.params = params

    def effective_service_rate(self, base_rate, workers):
        f0 = self.params["f0"]
        a = self.params["a"]
        return base_rate * (1 + f0 * (1 - math.exp(-a * (workers - 1))) / a)

    def calculate_throughput(self, inconfig, throughput_data):
        throughput_per_queue = {}
        # print(inconfig)
        config = self.find_best_config(throughput_data)
        for queue, workers in config.items():
            # TODO: remove hack
            if "compression_par" in queue:
                queue = queue.replace("compression_par", "compression_nocomp_par")

            base_rate = throughput_data[queue][1]
            effective_rate = self.effective_service_rate(base_rate, workers)
            throughput_per_queue[queue] = effective_rate * workers

        return min(throughput_per_queue.values())

    def find_best_config(self, throughput_data):
        best_config = {}

        # Find the smallest entry in the upper bounds

        min_limit = min(self.params["upper_bounds"].values())

        for queue in throughput_data:
            queue_name = queue.replace('_par', '')

            if not ("lzo" in queue_name or "snappy" in queue_name or "lz4" in queue_name or "zstd" in queue_name):

                if "nocomp" in queue_name:
                    queue_name = queue_name.replace("_nocomp", "")
                base_rate = throughput_data[queue][1]
                max_rate = self.params["upper_bounds"].get(queue_name, float('inf'))
                workers = 1
                while True:
                    effective_rate = self.effective_service_rate(base_rate, workers)
                    # print(f"{queue} : {effective_rate}, x {workers}")
                    if effective_rate >= min(min_limit, max_rate):
                        break
                    workers += 1

                # print(f"{queue} mu: {effective_rate}, base: {base_rate} * {workers} workers")
                best_config[queue_name] = workers

        # make sure send is same as rcv parallelism
        best_config["send"] = best_config["receive"] = math.floor((best_config["send"] + best_config["receive"]) / 2)
        best_config_with_par = {f"{k}_par": v for k, v in best_config.items()}

        return best_config_with_par
