import pandas as pd


class Helpers:
    @staticmethod
    def read_last_line(file_path):
        with open(file_path, 'r') as file:
            lines = file.readlines()
            last_line = lines[-1].strip()
            return last_line

    @staticmethod
    def calculate_average_throughputs(df_merged, throughput_data):
        # Step 1: Identify all components from the DataFrame columns that match the "_throughput_pb" pattern
        components = [col.replace('_throughput_pb', '') for col in df_merged.columns if col.endswith('_throughput_pb')]
        # Create a dictionary to store the sum and count for averaging later
        throughput_sums = {component: 0 for component in components}
        throughput_counts = {component: 0 for component in components}

        for component in components:
            throughput_pb_col = f'{component}_throughput_pb'

            if component in throughput_data:
                # Calculate throughput per buffer by dividing the _throughput_pb values by the corresponding _par values
                throughput_per_buffer = df_merged[throughput_pb_col] * df_merged[f"{component}_par"]
                # print(f"{component}: {df_merged[throughput_pb_col]}")

                # Sum the throughputs and keep track of the count
                throughput_sums[component] += throughput_per_buffer.sum()
                throughput_counts[component] += len(throughput_per_buffer)

        # Calculate the average throughput per buffer for each component
        averaged_throughputs = {component: throughput_sums[component] / throughput_counts[component]
                                for component in components if throughput_counts[component] > 0}

        # Merge with the provided throughput_data
        #for component in throughput_data:
        #    if component in averaged_throughputs:
        #        averaged_throughputs[component] = (averaged_throughputs[component] + throughput_data[component]) / 2
        #    else:
        #        averaged_throughputs[component] = throughput_data[component]

        return averaged_throughputs
