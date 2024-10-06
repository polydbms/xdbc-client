class OptimizerQueues:
    def __init__(self, params):
        self.optimal_throughput = float('inf')
        self.params = params
        # Ordered list of components based on the processing sequence
        self.component_sequence = ['read_par', 'deserialization_par', 'compression_par', 'send_par', 'receive_par',
                                   'decompression_par',
                                   'write_par']

    def find_best_config(self, components, max_threads=10):
        optimal_configurations = {}
        prev_throughput = self.params['upper_bounds']['read']  # Start with the upper bound for reading

        for component in self.component_sequence:
            component_throughput = components[component][1]

            # Calculate the number of threads needed
            num_threads = min(max(1, int(prev_throughput / component_throughput)), max_threads)
            optimal_configurations[component] = num_threads

            # Update the previous throughput based on the current component's total throughput
            prev_throughput = num_threads * component_throughput

            # Apply specific upper bounds for send and write components
            if component in ['send_par', 'receive_par']:
                prev_throughput = min(prev_throughput, self.params['upper_bounds']['network'])
            elif component == 'write_par':
                prev_throughput = min(prev_throughput, self.params['upper_bounds']['write'])

        # Backtrack if necessary to ensure balanced throughput
        for i in range(len(self.component_sequence) - 2, -1, -1):
            current_component = self.component_sequence[i]
            next_component = self.component_sequence[i + 1]

            current_throughput_key = current_component
            next_throughput_key = next_component

            current_throughput = components[current_throughput_key][1]
            next_throughput = components[next_throughput_key][1]

            # Update the throughput for the current component based on the adjusted number of threads
            updated_throughput = optimal_configurations[current_component] * current_throughput

            # Update the slowest throughput if necessary
            self.optimal_throughput = min(self.optimal_throughput, updated_throughput)

            required_threads = min(
                max(1, int(optimal_configurations[next_component] * next_throughput / current_throughput)), max_threads)
            optimal_configurations[current_component] = min(optimal_configurations[current_component], required_threads)

        return optimal_configurations

    def calculate_throughput(self, config, throughput_data):
        return self.optimal_throughput
