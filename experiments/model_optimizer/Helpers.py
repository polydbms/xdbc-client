import glob
import itertools
import numpy as np
import pandas as pd
from sklearn.metrics import *
from sklearn.model_selection import train_test_split

from experiments.model_optimizer.model_implementations.neural_network import Neural_Network_Cost_Model
from experiments.model_optimizer.model_implementations.random_forest import Random_Forest_Regression_Cost_Model


def load_data_from_csv(type,environment_list=None):

    df = pd.DataFrame


    if type == 'random_samples_1310k':

        if environment_list is not None:
            file_list = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/*_random_samples.csv')

            list_single_dfs = []

            for file in file_list:

                df = pd.read_csv(file)
                list_single_dfs.append(df)

            df = pd.concat(list_single_dfs, axis=0, ignore_index=True)

            df = df[df['time'].notna()]

            #df = df[df.apply(lambda row: (row['server_cpu'], row['client_cpu'], row['network']) in allowed_combinations, axis=1)]

            condition = df.apply(
                lambda row: any(
                    all(row[key] == value for key, value in comb.items() if key != 'timeout')
                    for comb in environment_list
                ),
                axis=1
            )

            df = df[condition]



        else:

            file_list = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/*_random_samples.csv')

            list_single_dfs = []

            for file in file_list:

                df = pd.read_csv(file)
                list_single_dfs.append(df)

            df = pd.concat(list_single_dfs, axis=0, ignore_index=True)

            df = df[df['time'].notna()]


    elif type == 'random_samples_175k':

        if environment_list is not None:
            file_list = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_175k/*_random_samples.csv')

            list_single_dfs = []

            for file in file_list:

                df = pd.read_csv(file)
                list_single_dfs.append(df)

            df = pd.concat(list_single_dfs, axis=0, ignore_index=True)

            df = df[df['time'].notna()]

            #df = df[df.apply(lambda row: (row['server_cpu'], row['client_cpu'], row['network']) in allowed_combinations, axis=1)]

            condition = df.apply(
                lambda row: any(
                    all(row[key] == value for key, value in comb.items() if key != 'timeout')
                    for comb in environment_list
                ),
                axis=1
            )

            df = df[condition]



        else:

            file_list = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_175k/*_random_samples.csv')

            list_single_dfs = []

            for file in file_list:

                df = pd.read_csv(file)
                list_single_dfs.append(df)

            df = pd.concat(list_single_dfs, axis=0, ignore_index=True)

            df = df[df['time'].notna()]


    else:
        df = pd.read_csv(type + ".csv")

    filtered_data = df[
        (df.server_cpu > 0) &
        (df.client_cpu > 0) &
        (df.network < 20000) &
        (df.table == "lineitem_sf10") #&
        #(df.format == 1) &
        #(df.client_readmode == 1) &
        #(df.system == "csv")# &
        #(df.network_latency == 0) &
        #(df.server_read_partitions <= 4) &
        #(df.server_read_par <= 4) &
        #(df.server_deser_par <= 4) &
        #(df.server_comp_par <= 4) &
        #(df.client_write_par <= 4) &
        #(df.client_decomp_par <= 4) &
        #(df.network_parallelism <= 4) #&
        #(df.xdbc_version == 8)
        ]

    filtered_data['bufpool_size'] = filtered_data[['server_bufferpool_size']].div(filtered_data.buffer_size, axis=0)

    filtered_data = filtered_data[filtered_data['time'].notnull()]

    #filtered_data = filtered_data[filtered_data.time < 500]

    return filtered_data


def split_data(data, metric, split_into_train_and_test=False):
    x = data[["xdbc_version",
              "run",
              "format",
              "client_readmode",
              "client_cpu",
              "server_cpu",
              "network",
              "network_latency",
              "network_loss",
              "source_system",
              "target_system",
              "table",
              "bufpool_size",
              "buffer_size",
              "compression",
              "send_par",
              "rcv_par",
              "write_par",
              "decomp_par",
              "read_partitions",
              "read_par",
              "deser_par",
              "comp_par"]]

    #x['bufpool_size'] = x['server_bufferpool_size']

    y = data[[metric]]

    if split_into_train_and_test:
        #todo
        pass
    else:
        return x, y



def check_for_duplicate(config, filename="random_samples_16_16_1000.csv"):
    df = pd.read_csv(filename)

    filtered_data = df[
        (df.server_cpu == config['server_cpu']) &
        (df.client_cpu == config['client_cpu']) &
        (df.network == config['network']) &
        (df.table == config['table']) &
        #(df.format == config['format']) &
        (df.client_readmode == config['client_readmode']) &
        #(df.system == config['system']) &
        (df.network_latency == config['network_latency']) &
        (df.server_read_partitions == config['server_read_partitions']) &
        (df.server_read_par == config['server_read_par']) &
        (df.server_deser_par == config['server_deser_par']) &
        (df.server_comp_par == config['server_comp_par']) &
        (df.client_write_par == config['client_write_par']) &
        (df.client_decomp_par == config['client_decomp_par']) &
        (df.network_parallelism == config['send_par']) &
        (df.xdbc_version == config['xdbc_version'])
        ]

    if filtered_data.size > 0 and (filtered_data.iloc[0]['time'] > 1):

        result = {
            "date": filtered_data.iloc[0]['date'],
            "time": filtered_data.iloc[0]['time'],
            "size": filtered_data.iloc[0]['size'],
            "avg_cpu_server": filtered_data.iloc[0]['avg_cpu_server'],
            "avg_cpu_client": filtered_data.iloc[0]['avg_cpu_client']
        }

        return result
    else:
        return None


def get_next_trial_id(path="C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/results_1310k/"):

    all_files = glob.glob(path+"*/*/*.csv")


    all_trial_ids = []

    for file in all_files:
        df = pd.read_csv(file)
        df = df[df['time'].notna()]
        trial_ids = df['trial_id'].unique().tolist()
        all_trial_ids.append(trial_ids)

    if len(all_trial_ids) == 0:
        return 1

    flat_list = []

    for a in all_trial_ids:
        for b in a:
            flat_list.append(b)

    next_id = max(flat_list) + 1

    return next_id


def evaluate_regression_model(model,x,y,return_score=False):
    y_true = np.array(y)
    y_prediction = np.array([])

    for index, row in x.iterrows():
        y_single = model.predict(row)['time']
        y_prediction = np.append(y_prediction,y_single)

    if return_score == True:
        return mean_squared_error(y_true, y_prediction)
    else:
        print(f"evaluation of regression-model : ")
        #print(f"acccuracy_score :          {round(accuracy_score(y_true, y_prediction,normalize=True),2)} Best : 1.0")
        print(f"explained_variance_score : {round(explained_variance_score(y_true, y_prediction),2)} Best : 1.0")
        print(f"mean_absolute_error :      {round(mean_absolute_error(y_true, y_prediction),2)} Lower is better")
        print(f"mean_squared_error :       {round(mean_squared_error(y_true, y_prediction),2)} Lower is better")
        print(f"median_absolute_error :    {round(median_absolute_error(y_true, y_prediction),2)} Best : 0.0")
        print(f"r2_score :                 {round(r2_score(y_true, y_prediction),2)} Best : 1.0")





def tune_random_forest():


    #data = load_data_from_csv(type='all')
    data = load_data_from_csv(type='all')

    x, y = split_data(data, 'time')

    x_train, x_test, y_train, y_test = train_test_split(x, y,
                                                        random_state=42,
                                                        test_size=0.25,
                                                        shuffle=True)

    param_grid = {
        'n_estimators': [15, 30, 50],
        'max_features': [ 2, 3, 5,10, 'sqrt'],
        'max_depth': [5, 10, 15, 25],
        'min_samples_split': [2,5, 10],
        'min_samples_leaf': [1,2, 5]
    }

    param_combinations = list(itertools.product(
        param_grid['n_estimators'],
        param_grid['max_features'],
        param_grid['max_depth'],
        param_grid['min_samples_split'],
        param_grid['min_samples_leaf']
    ))

    best_score = +np.inf
    best_params = None

    print(f"Testing {len(param_combinations)} different combinations")
    i = 1
    for params in param_combinations:
        n_estimators, max_features, max_depth, min_samples_split, min_samples_leaf = params

        cost_model = Random_Forest_Regression_Cost_Model(
            metric='time',
            n_estimators=n_estimators,
            max_features=max_features,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf
        )
        cost_model.train(x_train, y_train)

        score = evaluate_regression_model(cost_model, x_test, y_test, True)

        if score < best_score:
            best_score = score
            best_params = params
            print(f"{i} found new best score : "+str(best_score))
        else:
            print(f"{i} found worse score : "+str(best_score))
        i = i+1


    print("Best Parameters:")
    print(f"n_estimators: {best_params[0]}")
    print(f"max_features: {best_params[1]}")
    print(f"max_depth: {best_params[2]}")
    print(f"min_samples_split: {best_params[3]}")
    print(f"min_samples_leaf: {best_params[4]}")
    print(f"Best Score: {best_score}")


'''

For orig. 192 search grid and 0.25 split:

Best Parameters:
n_estimators: 50
max_features: 3
max_depth: 15
min_samples_split: 10
Best Score: 6579.737930502058

for 720 grid and 0.25 split:

Best Parameters:
n_estimators: 50
max_features: sqrt
max_depth: 15
min_samples_split: 20
min_samples_leaf: 5
Best Score: 6554.722485774223

288 grid : 

Best Parameters:
n_estimators: 30
max_features: 3
max_depth: 10
min_samples_split: 10
min_samples_leaf: 2
Best Score: 6474.883431087938



324 grid:

Best Parameters:
n_estimators: 50
max_features: 2
max_depth: 15
min_samples_split: 5
min_samples_leaf: 2
Best Score: 6506.523654469058

auskommentierte paramter rein : 
n_estimators: 30
max_features: 2
max_depth: 10
min_samples_split: 5
min_samples_leaf: 1
Best Score: 6468.629858037902


'''

def tune_nn():


    #data = load_data_from_csv(type='all')
    data = load_data_from_csv(type='all')

    x, y = split_data(data, 'time')

    x_train, x_test, y_train, y_test = train_test_split(x, y,
                                                        random_state=42,
                                                        test_size=0.25,
                                                        shuffle=True)

    param_grid = {
        'hidden_layers': [[128, 64, 32], [128, 128, 128], [256, 512, 128], [256, 128, 64, 32, 16]],
        'learning_rate': [0.0001,0.001,0.01],
        'epochs': [50,80,100,150],
        'batch_size': [32,64,128],
        'dropout': [0,0.2,0.4]
    }

    param_combinations = list(itertools.product(
        param_grid['hidden_layers'],
        param_grid['learning_rate'],
        param_grid['epochs'],
        param_grid['batch_size'],
        param_grid['dropout']
    ))

    best_score = +np.inf
    best_params = None

    print(f"Testing {len(param_combinations)} different combinations")
    i = 1
    for params in param_combinations:
        hidden_layers, learning_rate, epochs, batch_size,dropout = params

        cost_model = Neural_Network_Cost_Model(
            input_size=13,
            metric='time',
            hidden_layers=hidden_layers,
            learning_rate=learning_rate,
            epochs=epochs,
            batch_size=batch_size,
            dropout=dropout
        )

        score = cost_model.train(x_train, y_train)

        #evaluate_regression_model(cost_model, x_test, y_test, False)

        if score < best_score:
            best_score = score
            best_params = params
            print(f"{i} found new best score : "+str(best_score))
        else:
            print(f"{i} found worse score : "+str(score))
        i = i+1


    print("Best Parameters:")
    print(f"hidden_layers: {best_params[0]}")
    print(f"learning_rate: {best_params[1]}")
    print(f"epochs: {best_params[2]}")
    print(f"batch_size: {best_params[3]}")
    print(f"dropout: {best_params[4]}")
    print(f"Best Score: {best_score}")

'''
432 found worse score : 10744.2353515625
Best Parameters:
hidden_layers: [128, 128, 128]
learning_rate: 0.01
epochs: 100
batch_size: 64
dropout: 0
Best Score: 693.44287109375
'''

def test_random_forest():
    cost_model = Random_Forest_Regression_Cost_Model(metric='time')

    data = load_data_from_csv(type='all')

    x, y = split_data(data, 'time')

    x_train, x_test, y_train, y_test = train_test_split(x, y,
                                                        random_state=42,
                                                        test_size=0.25,
                                                        shuffle=True)

    cost_model.train(x_train, y_train)

    evaluate_regression_model(cost_model, x_test, y_test,return_score=False)



