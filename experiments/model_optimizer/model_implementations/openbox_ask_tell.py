from datetime import datetime
from collections import defaultdict

import numpy as np
import pandas as pd
from ConfigSpace import Configuration
from openbox import Advisor, Observation, History

from scipy.stats import wasserstein_distance
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
from experiments.model_optimizer import Metrics
from experiments.model_optimizer.Configs import *
from experiments.model_optimizer import Transfer_Data_Processor


class OpenBox_Ask_Tell():

    available_optimization_algorithms = ['bayesian_open_box', 'bayesian_gp_open_box', 'bayesian_prf_open_box',]

    available_transfer_algorithms = ['tlbo_rgpe_prf', 'tlbo_sgpr_prf', 'tlbo_topov3_prf',
                                     'tlbo_rgpe_gp', 'tlbo_sgpr_gp', 'tlbo_topov3_gp']



    available_algorithms = available_optimization_algorithms + available_transfer_algorithms


    def __init__(self, config_space, metric='time', mode='time', underlying='bayesian'):
        """
        Initialize the OpenBox Ask-Tell Class.

        Parameters:
            config_space (dict): Configuration space for the optimizer.
            metric (str): Metric to optimize.
            mode (str): Mode of optimization.
            underlying (str): Underlying optimization algorithm.
        """
        self.config_space = convert_from_general(config_space, 'open_box')
        self.metric = metric
        self.mode = mode
        self.underlying = f"{underlying}_open_box"

        self.history = None
        self.advisor = None
        self.current_suggestions = {}

        self.reset()

    def reset(self):
        """
        Resets the optimizer to remove any previous knowledge of completed evaluations.
        Initializes the appropriate advisor based on the underlying algorithm.
        """
        print(f"[{datetime.today().strftime('%H:%M:%S')}] Resetting the optimizer...")
        bayesian_config = {
            "config_space": self.config_space,
            "acq_type": "ei",
            "surrogate_type": "prf",
            "rand_prob": 0
        }

        transfer_config = {
            "config_space": self.config_space,
            "num_objectives": 1,
            "num_constraints": 0,
            "initial_trials": 0,
            "rand_prob": 0,
            "transfer_learning_history": self.history,
            "acq_type": "ei"
        }

        transfer_algorithms = ['tlbo_rgpe_prf', 'tlbo_sgpr_prf', 'tlbo_topov3_prf', 'tlbo_rgpe_gp', 'tlbo_sgpr_gp',
                               'tlbo_topov3_gp']

        if 'bayesian' in self.underlying:
            if 'bayesian_gp' in self.underlying:
                bayesian_config["surrogate_type"] = "gp"
            elif 'bayesian_prf' in self.underlying:
                bayesian_config["surrogate_type"] = "prf"

            print(f"[{datetime.today().strftime('%H:%M:%S')}] Initializing Bayesian optimization with {bayesian_config['surrogate_type']} surrogates")
            self.advisor = Advisor(**bayesian_config)
        elif any(key in self.underlying for key in transfer_algorithms):
            if not self.history:
                print(f"[{datetime.today().strftime('%H:%M:%S')}] Transfer learning history is empty. Skipping initialization of the transfer learning advisor for underlying algorithm {self.underlying}.")
            else:
                if 'tlbo_rgpe_prf' in self.underlying:
                    transfer_config["surrogate_type"] = "tlbo_rgpe_prf"
                elif 'tlbo_sgpr_prf' in self.underlying:
                    transfer_config["surrogate_type"] = "tlbo_sgpr_prf"
                elif 'tlbo_topov3_prf' in self.underlying:
                    transfer_config["surrogate_type"] = "tlbo_topov3_prf"
                elif 'tlbo_rgpe_gp' in self.underlying:
                    transfer_config["surrogate_type"] = "tlbo_rgpe_gp"
                elif 'tlbo_sgpr_gp' in self.underlying:
                    transfer_config["surrogate_type"] = "tlbo_sgpr_gp"
                elif 'tlbo_topov3_gp' in self.underlying:
                    transfer_config["surrogate_type"] = "tlbo_topov3_gp"
                print(
                    f"[{datetime.today().strftime('%H:%M:%S')}] Initializing transfer learning algorithm: {self.underlying}")
                self.advisor = Advisor(**transfer_config)
        else:
            print(f"[{datetime.today().strftime('%H:%M:%S')}] Unknown underlying algorithm: {self.underlying}")
            raise ValueError(f"Unknown underlying algorithm: {self.underlying}")

        self.current_suggestions = {}

    def suggest(self):
        """
        Gets the next suggested trial from the underlying optimization algorithm and returns it as a dictionary.

        Returns:
            dict: The next suggested configurations.
        """
        suggested_config = self.advisor.get_suggestion()
        self.current_suggestions[frozenset(suggested_config.items())] = suggested_config
        return suggested_config.get_dictionary()

    def report(self, config, result):
        """
        Reports the result of a completed trial to the underlying optimization algorithm.

        Parameters:
            config (dict): The configuration that was evaluated.
            result (dict): The result of that configuration.

        """
        complete_config = self.current_suggestions[frozenset(config.items())]
        metric_value = Metrics.get_metric(self.metric, result)
        observation = Observation(config=complete_config, objectives=metric_value)
        self.advisor.update_observation(observation)


    def load_transfer_learning_history_per_env_from_dataframe(self, data, training_data_per_env=-1):
        """
        Loads a list of files as previous evaluations into the underlying optimizer, creating one history per environment.
        Groups evaluations by environment and loads them as a transfer learning history into the algorithm.

        Parameters:
            file_list (list): List of file paths to load.
            training_data_per_env (int): Number of samples to use per environment (-1 for no limit).
        """
        '''
        # Group data by unique combinations of 'server_cpu', 'client_cpu', and 'network'
        grouped_data = defaultdict(list)

        df = data[(data['server_cpu'] > 0) & (data['client_cpu'] > 0) & (data['network'] > 0)]

        for combination, group in df.groupby(['server_cpu', 'client_cpu', 'network']):

            if training_data_per_env != -1:
                group = group.head(training_data_per_env)
                if f"_n_{training_data_per_env}" not in self.underlying:
                    self.underlying += f"_n_{training_data_per_env}"

            grouped_data[combination].append(group)

        grouped_data = {key: pd.concat(frames, ignore_index=True) for key, frames in grouped_data.items()}

        clusters = self._cluster_groups(grouped_data, column='time')

        group_to_rows = {}
        for group_key, group_df in grouped_data.items():
            group_to_rows[group_key] = df[
                (df['server_cpu'] == group_key[0]) &
                (df['client_cpu'] == group_key[1]) &
                (df['network'] == group_key[2])
                ]

        # Split data based on clusters
        cluster_dataframes = {}
        for cluster_index, cluster in enumerate(clusters):
            # Create the cluster key
            cluster_key = "cluster-" + "-".join(
                f"{group_key[0]}_{group_key[1]}_{group_key[2]}" for group_key in cluster
            )

            # Combine rows for this cluster
            cluster_rows = pd.concat([group_to_rows[group_key] for group_key in cluster], ignore_index=True)
            cluster_dataframes[cluster_key] = cluster_rows
        '''

        cluster_dataframes = Transfer_Data_Processor.process_data(data,training_data_per_env)


        self.underlying = self.underlying + "_with_clustering"

        # Create transfer learning histories
        transfer_learning_history = []

        #for env_key, df in grouped_data.items():
        for env_key, df in cluster_dataframes.items():
            df = df[df['time'].notna() & (df['time'] > 0)]

            if training_data_per_env != -1:
                #df = df.head(training_data_per_env)
                if f"_n_{training_data_per_env}" not in self.underlying:
                    self.underlying += f"_n_{training_data_per_env}"

            history = self._create_history(env_key, df)
            transfer_learning_history.append(history)
            print(
                f"[{datetime.today().strftime('%H:%M:%S')}] Loaded History for Environment {env_key} with length {len(df)}")

        self.history = transfer_learning_history
        self.reset()

    def _create_history(self, env_key, df):
        """
        Creates a history object for a given environment.

        Parameters:
            env_key (tuple): The environment key (e.g., combination of server, client, and network).
            df (DataFrame): DataFrame containing the configuration and metrics for the environment.

        Returns:
            History: A populated History object.
        """
        history = History(task_id=f'history_{env_key}', config_space=self.config_space)

        for _, row in df.iterrows():
            config = {
                'compression_lib': row['compression'],
                'bufpool_size': float(row['server_bufferpool_size'] / row['buffer_size']),
                'buffer_size': row['buffer_size'],
                'send_par': row['send_par'],
                'write_par': row['write_par'],
                'decomp_par': row['decomp_par'],
                'read_partitions': row['read_partitions'],
                'read_par': row['read_par'],
                'deser_par': row['deser_par'],
                'comp_par': row['comp_par'],
                'ser_par': 1,  # todo
            }

            metric = row[self.metric]
            observation = Observation(config=Configuration(self.config_space, config), objectives=metric)
            history.update_observation(observation)

        return history
