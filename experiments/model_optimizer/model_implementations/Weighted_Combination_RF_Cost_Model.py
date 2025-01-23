import numpy as np
import pandas as pd
from prettytable import PrettyTable
from scipy.optimize import nnls, lsq_linear
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge

from experiments.model_optimizer import Transfer_Data_Processor
from experiments.model_optimizer.Configs import *


class Per_Environment_RF_Cost_Model:
    def __init__(self, input_fields, metric='time', data_per_env=100, underlying="cost_model_rfs"):
        """
       Initializes the Per_Environment_RF_Cost_Model, which trains and predicts using
       Random Forest models for multiple environment.

       Parameters:
           input_fields (list): List of feature names used for training and prediction.
           metric (str): Metric to optimize.
           underlying (str): Identifier-String for the underlying model.
       """
        self.input_fields = input_fields
        self.metric = metric
        self.models = {}  # dict to store rf's per environment
        self.environments = []  # list of known environments
        self.underlying = underlying
        self.data_per_env = data_per_env
        self.weight_history = []

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

        if 'cluster' in self.underlying:
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

            model = RandomForestRegressor(n_estimators=100,
                                          max_depth=None,
                                          min_samples_split=2,
                                          min_samples_leaf=1,
                                          bootstrap=True,
                                          random_state=123)
            model.fit(X, y)

            self.models[env] = model
            self.environments.append(env)

    def update(self, config, result, factor=10):
        """
        Updates the weight history by calculating a weight vector for the  configuration
        and result,depending on how close they are to the individual models' predictions.

        Parameters:
            config (dict): Configuration which was executed.
            result (dict): Result of that execution contianing `self.metric`.
            factor (int): Factor to amplify weights.
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
            difference = abs(prediction - result[self.metric])  # calculate the error for the model
            weight = 1 / (difference + 1e-8)                    # 1 / error = weight
            weight_vector[env] = weight                         # add to weight vector
            total_difference += weight

        # Create a weight vector
        for env in weight_vector:
            weight_vector[env] /= total_difference              # normalize weights to sum = 1

        for env in weight_vector:
            weight_vector[env] *= factor                        # add factor to control the influence od the weight vector

        self.weight_history.append(weight_vector)

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

    def predict(self, data, target_environment, weights_algorithm="distances", print_wieghts=False):
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

        # if not, collect known environment, their predictions, and use lr to estimate the weights for a linear combination

        known_features = []
        known_predictions = []

        for env, model in self.models.items():
            env_dict = unparse_environment_float(env)
            env_server = round(env_dict['server_cpu'],2)
            env_client = round(env_dict['client_cpu'],2)
            env_network = round(env_dict['network'],2)

            known_features.append([env_server, env_client, env_network])

            predictions = self.models[env].predict(X)
            known_predictions.append(predictions)

        known_features = np.array(known_features)
        known_predictions = np.array(known_predictions).T

        target_features = np.array([[target_server, target_client, target_network]])

        #divide network by 100 to bring into the same value range as the other values
        known_features = np.array(known_features, dtype=float)
        target_features = np.array(target_features, dtype=float)
        known_features[:, 2] /= 100
        target_features[0][2] /= 100

        weights = None
        weights_normalized = None

        # Calculate the weights per model using one of these methods
        if weights_algorithm == "linear_regression":

            reg = LinearRegression(positive=True)  # use only positive weights
            reg.fit(known_features.transpose(), target_features.transpose())
            weights = reg.coef_[0]
            weights_normalized = weights / np.sum(weights)

        elif weights_algorithm == "ridge_regression":

            reg = Ridge(positive=True, alpha=1.0)
            reg.fit(known_features.transpose(), target_features.transpose())
            weights = reg.coef_[0]
            weights_normalized = weights / np.sum(weights)

        elif weights_algorithm == "random_forest_regression":

            reg = RandomForestRegressor(n_estimators=100, random_state=123)
            reg.fit(known_features.transpose(), target_features.transpose())
            weights = reg.feature_importances_[0]
            weights_normalized = weights / np.sum(weights)

        elif weights_algorithm == "least_squares_linear":

            res = lsq_linear(known_features.T, target_features[0], bounds=(0, np.inf))
            weights = res.x[0]
            weights_normalized = weights / np.sum(weights)

        elif weights_algorithm == "distances":

            distances = np.linalg.norm(known_features - target_features[0], axis=1)
            distances = np.where(distances == 0, 1e-8, distances)
            weights = 1 / distances
            weights_normalized = weights / np.sum(weights)

        elif weights_algorithm == "distances_test":

            weights, residuals, rank, s = np.linalg.lstsq(known_features.T, target_features.flatten(), rcond=None)
            weights_normalized = weights / np.sum(weights)



        # Introducce weight history from update method
        total_weights = len(self.weight_history)
        averaged_weights = np.zeros_like(weights)
        weight_decay_factor = 0.9 # todo whats a good value ?

        # Calculate the actual added wieghts vector from the history
        for idx, weight_vector in enumerate(self.weight_history):
            influence = weight_decay_factor ** (total_weights - idx - 1)
            for i, env in enumerate(self.models.keys()):
                averaged_weights[i] += influence * weight_vector.get(env, 0)

            # normalize
            normalization_factor = sum(weight_decay_factor ** (total_weights - idx - 1) for idx in range(total_weights))
            averaged_weights /= normalization_factor

        if not np.all(averaged_weights == 0):
            averaged_weights = np.where(averaged_weights == 0, 1e-8, averaged_weights)
            averaged_weights /= np.sum(averaged_weights)

        # Combine the weight vectors
        final_weights = weights_normalized + averaged_weights
        final_weights_normalized = final_weights / np.sum(final_weights)


        #use normalized weights for combining predictions, non normalized for environments printing
        combined_predictions = np.dot(weights_normalized, known_predictions.flatten())
        combined_environment = np.dot(weights, known_features)

        combined_predictions_final = np.dot(final_weights_normalized, known_predictions.flatten())
        combined_environment_final = np.dot(final_weights, known_features)


        data_df = {'Env/Cluster' : [" ".join(map(str, row)) for row in known_features],
                   'Predictions': known_predictions[0],
                   'Weights': weights_normalized,
                   'Weights w/ hist.': final_weights_normalized,
                   'Hist. Weights': averaged_weights}

        df_to_print = pd.DataFrame(data_df)
        df_to_print = df_to_print.round(2)

        df_to_print = df_to_print.sort_values(by='Weights w/ hist.', ascending=True, inplace=False)

        table = PrettyTable()
        table.field_names = df_to_print.columns.tolist()
        for row in df_to_print.itertuples(index=False):
            table.add_row(row)

        if print_wieghts == True:

            print(f"\nTarget Environment: S{target_environment['server_cpu']}_C{target_environment['client_cpu']}_N{target_environment['network']}")
            print(f"Feature vector: {target_features}")
            print(table)

            print(f"Prediction :         { round(combined_predictions,2)}")
            print(f"Prediction w/ hist.: { round(combined_predictions_final,2)}")
            print(f"Estimated Environment :         { np.round(combined_environment,2)}")
            print(f"Estimated Environment w/ hist.: { np.round(combined_environment_final,2)}")

        # Depending on what cost model we created, return one of the combined predictions.
        # ( the underlying_str is what I use the identify an algorithm after I ran it, so this way I can distinguish weather the update part was used or not)
        if 'update' in self.underlying:
            return {self.metric: combined_predictions_final}
        else:
            return {self.metric: combined_predictions}
