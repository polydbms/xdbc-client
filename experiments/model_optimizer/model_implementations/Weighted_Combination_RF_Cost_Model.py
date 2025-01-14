import numpy as np
import pandas as pd
from scipy.optimize import nnls, lsq_linear
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge

from experiments.model_optimizer.Configs import *


class Per_Environment_RF_Cost_Model:
    def __init__(self, input_fields, metric='time', underlying="cost_model_rfs"):
        self.input_fields = input_fields
        self.metric = metric
        self.models = {}  # dict to store rf's per environment
        self.environments = []  # list of known environments
        self.underlying = underlying

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

        # group data per environment by using the env_to_string method
        combine_data['environment'] = combine_data.apply(
            lambda row: environment_values_to_string(row["server_cpu"], row["client_cpu"], row["network"]), axis=1
        )
        grouped = combine_data.groupby('environment')

        # then train a model for each environment-group
        for env, group in grouped:
            X = group[self.input_fields].values
            y = group[self.metric].values

            model = RandomForestRegressor(n_estimators=100, random_state=123)
            model.fit(X, y)

            self.models[env] = model
            self.environments.append(env)

    def update(self, config, result, factor=10):
        '''

        if we get a new result, we cant build a cost model based on that single result.
        we could get the estimate of all knonw cost models for that config, and compare their predicted result to the known result,
        and this way influence the wheigts by which they are combined in the prediction.

        we could calculate the distance between the actual result and the predicted results, and somhow calculate wheight-factors from that.
        then include these factors in the weight prediction calculation to get a more accurate prediction.


        '''
        #todo
        '''
        X = config[self.input_fields].values

        known_predictions = []

        for env, model in self.models.items():

            predictions = self.models[env].predict(X)
            known_predictions.append(predictions)

        known_predictions = np.array(known_predictions).T

        '''
        pass

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

    def predict(self, data, target_environment, weights_algorithm="distances"):
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

        # if not, collect known environment, their predictions, and use lr to estimtae the wheights for a linear combination

        known_features = []
        known_predictions = []

        for env, model in self.models.items():
            env_dict = unparse_environment(env)
            env_server = env_dict['server_cpu']
            env_client = env_dict['client_cpu']
            env_network = env_dict['network']

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

        global weights

        if weights_algorithm == "linear_regression":

            reg = LinearRegression(positive=True)  # use only positive weights
            reg.fit(known_features.transpose(), target_features.transpose())
            weights = reg.coef_

        elif weights_algorithm == "ridge_regression":

            reg = Ridge(positive=True, alpha=1.0)
            reg.fit(known_features.transpose(), target_features.transpose())
            weights = reg.coef_

        elif weights_algorithm == "random_forest_regression":

            reg = RandomForestRegressor(n_estimators=100, random_state=123)
            reg.fit(known_features.transpose(), target_features.transpose())
            weights = reg.feature_importances_

        elif weights_algorithm == "least_squares_linear":

            res = lsq_linear(known_features.T, target_features[0], bounds=(0, np.inf))
            weights = res.x

        elif weights_algorithm == "distances":

            distances = np.linalg.norm(known_features - target_features[0], axis=1)
            distances = np.where(distances == 0, 1e-8, distances)
            weights = 1 / distances
            weights /= np.sum(weights)

        combined_predictions = np.dot(known_predictions, weights.T)
        combined_environment = np.dot(known_features.T, weights.T).T[0]

        #print(f"\nEstimated Environment : ['server_cpu': {round(combined_environment[0],2)}, 'client_cpu': {round(combined_environment[1],2)}, 'network': {round(combined_environment[2],2)}] ")
        #print(f"True Environment        : {target_environment}")
        #print(f"Individual Factors : {round(target_server / combined_environment[0][0],2)}  {round(target_client / combined_environment[1][0],2)}  {round(target_network / combined_environment[2][0],2)}")
        #print(f"Average of Factors : {(round(target_server / combined_environment[0][0],2) + round(target_client / combined_environment[1][0],2) + round(target_network / combined_environment[2][0],2))/3}")
        #print(f"Sqrt Factors :       {np.sqrt(round(target_server / combined_environment[0][0], 2))}  {np.sqrt(round(target_client / combined_environment[1][0], 2))}  {np.sqrt(round(target_network / combined_environment[2][0], 2))}")
        #print(f"Average of Sqrt Factors : {(np.sqrt(round(target_server / combined_environment[0][0], 2)) + np.sqrt(round(target_client / combined_environment[1][0], 2)) + np.sqrt(round(target_network / combined_environment[2][0], 2)))/3}")

        return {self.metric: combined_predictions[0]}
