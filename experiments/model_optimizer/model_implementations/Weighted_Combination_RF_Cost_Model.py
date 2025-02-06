import numpy as np
import pandas as pd
from prettytable import PrettyTable
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from xgboost import XGBRegressor

from experiments.model_optimizer import Transfer_Data_Processor
from experiments.model_optimizer.Configs import *

#

class Per_Environment_RF_Cost_Model:
    def __init__(self,
                 input_fields,
                 metric='time',
                 data_per_env=100,
                 underlying="cost_model",
                 cluster=True,
                 regression_model='xgb',
                 network_transformation=True,
                 history_ratio=0.5,
                 history_weight_decay_factor=0.9):
        """
       Initializes the Per_Environment_RF_Cost_Model, which trains and predicts using
       Regression Models for multiple environments or clusters.

       Parameters:
           input_fields (list): List of feature names used for training and prediction.
           metric (str): Metric to optimize.
           underlying (str): Identifier-String for the underlying model.
           cluster (bool): True if data should be clustered, False if one model per environment
           regression_model (str): The underlying regression model to use [rfs, gdb, xgb]
           network_transformation (bool): True if network values should be transformed with sigmoid like function for distance calculation. Has no effect if history_ratio is 1.
           history_ratio (float): The ratio between the weights calculated from the environment signatures, and from the weight history. Should be between [0,1]. 0 meaning only weights from environments signatures, and 1 meaning only weights from history.
       """
        self.input_fields = input_fields
        self.metric = metric
        self.underlying = underlying
        self.data_per_env = data_per_env
        self.cluster = cluster
        self.regression_model = regression_model
        self.network_transformation = network_transformation
        self.history_ratio = history_ratio
        self.history_weight_decay_factor = history_weight_decay_factor

        self.continous_maintained_history_vector = None
        self.norm_total = 0
        self.total_history_updates = 0
        self.models = {}  # dict to store models's per environment
        self.environments = []  # list of known environments

        #temporary
        if "only_hist" in self.underlying:
            self.history_ratio = 1
        elif "update" in self.underlying:
            self.history_ratio = 0.5
        else:
            self.history_ratio = 0



    def train(self, x_train, y_train):
        """
        Train the random forests models. Splits data per Environment, and trains one model for each environment.

        Parameters:
           x_train (dataframe): Input data containing all fields from input fields.
           y_train (dataframe): Target data containing the values of the specified metric.
        """

        combine_data = x_train.copy()
        combine_data[self.metric] = y_train

        combine_data = self.convert_dataframe(combine_data)

        if 'cluster' in self.underlying:# or self.cluster:
            # cluster data automatically
            grouped = Transfer_Data_Processor.process_data(data=combine_data, training_data_per_env=self.data_per_env, cluster_labes_avg=True, n_clusters=0)
        else:
            # one cluster per environment
            grouped = Transfer_Data_Processor.process_data(data=combine_data, training_data_per_env=self.data_per_env, cluster_labes_avg=True, n_clusters=-1)


        # then train a model for each environment-group
        for env, group in grouped.items():

            env = env.replace("cluster-avg_","")

            X = group[self.input_fields].values
            y = group[self.metric].values

            if "_rfs_" in self.underlying:# or 'rfs' in self.regression_model:
                model_name = "RandomForestRegressor"
                model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=None,
                    min_samples_split=2,
                    min_samples_leaf=1,
                    bootstrap=True,
                    random_state=123)

            elif "_gdb_" in self.underlying:# or 'gdb' in self.regression_model:
                model_name = "GradientBoostingRegressor"
                model = GradientBoostingRegressor(
                    n_estimators=200,
                    learning_rate=0.05,
                    max_depth=None,
                    subsample=0.8,
                    min_samples_split=5,
                    min_samples_leaf=4,
                    random_state=123)

            elif "_xgb_" in self.underlying:# or 'xgb' in self.regression_model:
                model_name = "XGBRegressor"
                model = XGBRegressor(
                    objective="reg:squarederror",
                    n_estimators=100,
                    max_depth=None,
                    learning_rate=0.05, #0.1
                    min_child_weight=1,
                    reg_lambda=1,
                    reg_alpha=0,
                    gamma=0.5,
                    subsample=0.8,
                    random_state=123)
            else:
                raise ValueError(f"Unkown underlying regression model in algorithm signature: {self.regression_model}")


            model.fit(X, y)

            self.models[env] = model
            self.environments.append(env)
            print(f"Trained {model_name} for cluster {env} with length {len(group)}")

        self.continous_maintained_history_vector = np.zeros(len(self.models))

    def update(self, config, result):
        """
        Updates the weight history by calculating a weight vector for the  configuration
        and result,depending on how close they are to the individual models' predictions.

        Parameters:
            config (dict): Configuration which was executed.
            result (dict): Result of that execution contianing `self.metric`.
        """

        data = self.convert_dict(dict(config))
        data["compression"] = data["compression_lib"]

        if isinstance(data, dict):
            data = pd.DataFrame([data])

        X = data[self.input_fields].values

        # Collect all prediction of the known models
        known_predictions = {}
        for env, model in self.models.items():
            prediction = model.predict(X)
            known_predictions[env] = prediction[0]

        # dict {env-key : weight }
        weight_vector = {}

        # Calculate the difference between the true result and the predictions
        total_difference = 0
        for env, prediction in known_predictions.items():

            squared_error = (prediction - result[self.metric])**2 # calculate the error for the model
            weight = 1 / (squared_error + 1e-8)                   # 1 / error = weight
            weight_vector[env] = weight                         # add to weight vector
            total_difference += weight

        # Create a weight vector
        for env in weight_vector:
            weight_vector[env] /= total_difference              # normalize weights to sum = 1

        weight_array = np.array(list(weight_vector.values()))

        self.norm_total = self.history_weight_decay_factor * (self.norm_total + 1)
        self.total_history_updates = self.total_history_updates + 1
        self.continous_maintained_history_vector = (self.history_weight_decay_factor * self.continous_maintained_history_vector) + weight_array
        self.continous_maintained_history_vector = self.continous_maintained_history_vector / self.norm_total

    def convert_dataframe(self, df, reverse=False):
        mapping = {"nocomp": 0, "zstd": 1, "lz4": 2, "lzo": 3, "snappy": 4}
        reverse_mapping = {v: k for k, v in mapping.items()}
        field_name = "compression" if "compression" in df.columns else "compression_lib"
        if reverse:
            df[field_name] = df[field_name].map(reverse_mapping).fillna(df[field_name])
        else:
            df[field_name] = df[field_name].map(mapping).fillna(df[field_name])
        return df

    def convert_dict(self, data, reverse=False):
        mapping = {"nocomp": 0, "zstd": 1, "lz4": 2, "lzo": 3, "snappy": 4}
        reverse_mapping = {v: k for k, v in mapping.items()}
        field_name = "compression" if "compression" in data else "compression_lib"
        if reverse:
            data[field_name] = reverse_mapping.get(data[field_name], data[field_name])
        else:
            data[field_name] = mapping.get(data[field_name], data[field_name])
        return data

    def predict(self, data, target_environment, print_wieghts=False):
        """
        Predict for a target environment using the cost model for that environment,
        or if the environment is not known, a linear combination of known models.

        Parameters:
            data (dict): Data to be predicted on.
            target_environment (dict): Dictionary containing the values for the target environment (server_cpu,client_cpu,network).
        Returns:
            dict: Dictionary containing the result of the specified metric.
        """

        data = self.convert_dict(dict(data))
        data["compression"] = data["compression_lib"]

        target_server = target_environment['server_cpu']
        target_client = target_environment['client_cpu']
        target_network = target_environment['network']

        if isinstance(data, dict):
            data = pd.DataFrame([data])

        X = data[self.input_fields].values

        # if the target environment is known, use its model directly
        if environment_to_string(target_environment) in self.environments:
            y = self.models[environment_to_string(target_environment)].predict(X)
            return {self.metric: y[0]}

        # Collect all known environments and their predictions
        known_features = []
        known_predictions = []

        for env, model in self.models.items():
            env_dict = unparse_environment_float(env)
            known_features.append([round(env_dict['server_cpu'],2), round(env_dict['client_cpu'],2), round(env_dict['network'],2)])
            predictions = self.models[env].predict(X)
            known_predictions.append(predictions)

        known_features = np.array(known_features, dtype=float)
        known_predictions = np.array(known_predictions).T
        target_features = np.array([[target_server, target_client, target_network]], dtype=float)


        # Transform network using a sigmoid like function
        if "net_trans" in self.underlying:# or self.network_transformation:
            def transform_network(x):
                transformed_network = 9.45 / (1 + 31 * np.exp(-0.03 * x))
                return np.round(transformed_network,decimals=2)

            known_features[:, 2] = transform_network(known_features[:, 2])
            target_features[0][2] = transform_network(target_features[0][2])
        else:
            known_features[:, 2] /= 100
            target_features[0][2] /= 100


        # Calculate the distances between environment signatures
        distances = np.linalg.norm(known_features - target_features[0], axis=1)
        distances = np.where(distances == 0, 1e-8, distances) # to not divide by 0
        environment_weights = 1 / distances
        environment_weights_normalized = environment_weights / np.sum(environment_weights)


        # Use a combination of both weights or only environment weights
        if self.history_ratio < 1:

            # Dynamic history-ratio
            # hist_weight_ratio = self.total_history_updates / ((self.total_history_updates * 1.2) + 4)

            # Combine the environment & history weight vectors based on history_ratio
            combined_weights = (self.continous_maintained_history_vector * self.history_ratio) + (environment_weights_normalized * (1 - self.history_ratio))

        # Use only history weights
        elif self.history_ratio == 1:
            # Edge case if history is empty
            if np.all(self.continous_maintained_history_vector == 0):
                combined_weights = np.full(self.continous_maintained_history_vector.shape, 1/self.continous_maintained_history_vector.size)
            else:
                combined_weights = self.continous_maintained_history_vector

        # Normalize weight vector to 1
        combined_weights_normalized = combined_weights / np.sum(combined_weights)

        # Multiply predictions with weights to get final prediction
        prediction_w_history = np.dot(combined_weights_normalized, known_predictions.flatten())

        if print_wieghts:
            #use normalized weights for combining predictions, non normalized for environments printing

            prediction = np.dot(environment_weights_normalized, known_predictions.flatten())
            predicted_environment = np.dot(environment_weights, known_features)

            predicted_environment_w_history = np.dot(combined_weights, known_features)

            data_df = {'Env/Cluster': [" ".join(map(str, row)) for row in known_features],
                       'Predictions': known_predictions[0],
                       'Weights': environment_weights_normalized,
                       'Weights w/ hist.': combined_weights_normalized,
                       'Hist. Weights': self.continous_maintained_history_vector}

            df_to_print = pd.DataFrame(data_df)
            df_to_print = df_to_print.round(2)

            df_to_print = df_to_print.sort_values(by='Weights w/ hist.', ascending=True, inplace=False)

            table = PrettyTable()
            table.field_names = df_to_print.columns.tolist()
            for row in df_to_print.itertuples(index=False):
                table.add_row(row)

            print(f"\nTarget Environment: S{target_environment['server_cpu']}_C{target_environment['client_cpu']}_N{target_environment['network']}")
            print(f"Feature vector: {target_features}")
            print(table)

            print(f"Prediction :         { round(prediction,2)}")
            print(f"Prediction w/ hist.: { round(prediction_w_history,2)}")
            print(f"Estimated Environment :         { np.round(predicted_environment,2)}")
            print(f"Estimated Environment w/ hist.: { np.round(predicted_environment_w_history,2)}")

        return {self.metric: prediction_w_history}
