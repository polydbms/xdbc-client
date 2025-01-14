import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

class Random_Forest_Regression_Cost_Model():


    def __init__(self,parameter_bounds=None,metric='time',n_estimators=30,max_features=3,max_depth=10,min_samples_split=10,min_samples_leaf=2):
        """
        Initialize the random forest model.

        Parameters: #todo
            metric (str):
            n_estimators (int):
            max_features (int):
            max_depth (int):
            min_samples_split (int):
            min_samples_leaf (int):
        """

        self.parameter_bounds = parameter_bounds
        self.metric = metric

        self.orig_x = None
        self.orig_y = None

        params = {
            "n_estimators": n_estimators,
            "max_features": max_features,
            "max_depth": max_depth,
            "min_samples_split": min_samples_split,
            "min_samples_leaf": min_samples_leaf,
            #"warm_start":True,
            #"oob_score":True,
            "random_state": 123,
        }

        self.random_forest = RandomForestRegressor(**params)
        pass

    def train(self,x_train,y_train):
        """
        Train the random forest model.

         Parameters:
            x_train (dataframe): Input data
            y_train (dataframe): Target data
        """
        #y = float(y)

        x_train = x_train[[#"xdbc_version",
               #"run",
               #"format",
               #"client_readmode",
               "client_cpu",
               "server_cpu",
               "network",
               #"network_latency",
               #"network_loss",
               #"source_system",
               #"target_system",
               #"table",
               "bufpool_size",
               "buffer_size",
               #"compression",
               "send_par",
               #"rcv_par",
               "write_par",
               "decomp_par",
               "read_partitions",
               "read_par",
               "deser_par",
               "comp_par"
               ]]

        x_train['server_cpu'] *= 100
        x_train['client_cpu'] *= 100
        x_train['network'] *= 100

        x_train['server_cpu_2'] = x_train['server_cpu']
        x_train['client_cpu_2'] = x_train['client_cpu']
        x_train['network_2'] = x_train['network']

        y_train = y_train[[self.metric]]

        self.orig_x = x_train
        self.orig_y = y_train

        self.random_forest.fit(x_train,y_train)
        pass


    def predict(self,config):
        """
        Predict the output for given input data.

        Parameters:
            config (dict): Input data
        Returns:
            Predicted value
        """

        #randomforestregressor doesnt nativly support categorical features, todo implement one hot encoding

        x = {
            key: value for key, value in config.items() if key in {
                #"xdbc_version",
                #"run",
                #"format",
                #"client_readmode",
                "client_cpu",
                "server_cpu",
                "network",
                #"network_latency",
                #"network_loss",
                #"source_system",
                #"target_system",
                #"table",
                "bufpool_size",
                "buffer_size",
                #"compression",
                "send_par",
                #"rcv_par",
                "write_par",
                "decomp_par",
                "read_partitions",
                "read_par",
                "deser_par",
                "comp_par"
            }
        }


        x['server_cpu'] *= 100
        x['client_cpu'] *= 100
        x['network'] *= 100

        x['server_cpu_2'] = x['server_cpu']
        x['client_cpu_2'] = x['client_cpu']
        x['network_2'] = x['network']

        x_float = dict([a, float(x)] for a, x in x.items())

        arr = np.array(list(x_float.values()))

        y = self.random_forest.predict(arr.reshape(1,-1))

        return {self.metric: y}


    def update(self,config,result,factor=100):
        x = {
            key: value for key, value in config.items() if key in {
                #"xdbc_version",
                #"run",
                #"format",
                #"client_readmode",
                "client_cpu",
                "server_cpu",
                "network",
                #"network_latency",
                #"network_loss",
                #"source_system",
                #"target_system",
                #"table",
                "bufpool_size",
                "buffer_size",
                #"compression",
                "send_par",
                #"rcv_par",
                "write_par",
                "decomp_par",
                "read_partitions",
                "read_par",
                "deser_par",
                "comp_par"
            }
        }


        x['server_cpu'] *= 100
        x['client_cpu'] *= 100
        x['network'] *= 100

        x['server_cpu_2'] = x['server_cpu']
        x['client_cpu_2'] = x['client_cpu']
        x['network_2'] = x['network']


        x_float = dict([a, float(x)] for a, x in x.items())

        #x_float = np.array(list(x_float.values()))

        x_df = pd.DataFrame.from_dict([x_float])

        x_df_repeated = pd.DataFrame(np.repeat(x_df.to_numpy(), factor, axis=0), columns=x_df.columns)


        x = pd.concat([self.orig_x,x_df_repeated])


        y_float = float(result[self.metric])

        y_df = pd.DataFrame({self.metric:[y_float]})

        y_df_repeated = pd.DataFrame(np.repeat(y_df.to_numpy(), 100, axis=0), columns=y_df.columns)

        y = pd.concat([self.orig_y,y_df_repeated],ignore_index=True)


        self.orig_x = x
        self.orig_y = y

        self.random_forest.fit(x,y)


    def num_to_compression(self):
        pass


    def compression_to_num(self):
        pass