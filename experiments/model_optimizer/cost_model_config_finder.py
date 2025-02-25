from collections import defaultdict, Counter
from datetime import datetime


from experiments.model_optimizer.Configs import \
    config_space_variable_parameters_generalized_FOR_NEW_ITERATION_FLEXIBLE_EX_BufSiz, create_complete_config
from experiments.model_optimizer.environments import *
from experiments.model_optimizer.model_implementations.Weighted_Combination_RF_Cost_Model import \
    Per_Environment_RF_Cost_Model
from experiments.model_optimizer.model_implementations.lhs_search_optimizer import LHS_Search_Optimizer



def get_next_suggestion(search_space, n_queries, cost_model, environment, mode, metric):

        searcher = LHS_Search_Optimizer(config_space=search_space, n_samples=n_queries)

        configurations = []

        start = datetime.now()

        for i_inner in range(1, n_queries + 1):

            #get a configration
            suggested_config = searcher.suggest()

            #get the cost prediction for that configuration
            result_cost_model = cost_model.predict(data=suggested_config, target_environment=environment, print_wieghts=False)
            result_metric = float(result_cost_model[metric])
            configurations.append({'config': suggested_config, 'performance': result_metric})

        end = datetime.now()

        best_performance = max(entry['performance'] for entry in configurations)

        top_configurations = [
            entry for entry in configurations
            if entry['performance'] == best_performance
        ]

        return top_configurations[0]['config']



        # Idea : instead of taking the single best prediction, take the average of all top n best predictions. Might make it more robust.

        #sorted_configurations = sorted(configurations, key=lambda x: x['performance'], reverse=True)
        #top_n = max(1, len(sorted_configurations) // 10)
        #top_configurations = sorted_configurations[:top_n]


        if mode == 'max':

            best_performance = max(entry['performance'] for entry in configurations)
            threshold = best_performance * 0.025
            top_configurations = [
                entry for entry in configurations
                if entry['performance'] >= best_performance - threshold
            ]
        elif mode == 'min':

            best_performance = min(entry['performance'] for entry in configurations)
            threshold = best_performance * 0.025
            top_configurations = [
                entry for entry in configurations
                if entry['performance'] <= best_performance + threshold
            ]

        discrete_params = {"compression", "format", "buffer_size"}

        param_numeric_sums = defaultdict(float)
        param_numeric_counts = defaultdict(int)
        param_discrete_counts = defaultdict(Counter)

        for entry in top_configurations:
            config = entry["config"]
            for param, value in config.items():
                if param in discrete_params:
                    param_discrete_counts[param][value] += 1
                else:
                    try:
                        numeric_value = float(value)
                        param_numeric_sums[param] += numeric_value
                        param_numeric_counts[param] += 1
                    except Exception:
                        param_discrete_counts[param][value] += 1


        averaged_config = {}

        for param in param_numeric_sums:
            averaged_config[param] = int(round(param_numeric_sums[param] / param_numeric_counts[param]))

        for param, counter in param_discrete_counts.items():
            most_common_val = counter.most_common(1)[0][0]
            averaged_config[param] = most_common_val


        prediction = cost_model.predict(averaged_config,target_environment=environment, print_wieghts=False)




        print(f"[{datetime.today().strftime('%H:%M:%S')}] [Cost_Model_Config_Finder] running {n_queries} surrogate transfers took {(end - start).total_seconds()} seconds")
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [Cost_Model_Config_Finder] best found configuration has predicted metric of {prediction}  ({metric})")
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [Cost_Model_Config_Finder] best predicted config {averaged_config}")

        return averaged_config


if __name__ == "__main__":

    search_space = config_space_variable_parameters_generalized_FOR_NEW_ITERATION_FLEXIBLE_EX_BufSiz

    n_queries = 2000

    mode = 'max'

    metric = 'uncompressed_throughput'
    #metric = 'time'

    #environment = env_S2_C2_N50
    #environment = env_S2_C8_N50
    #environment = env_S4_C16_N50

    #environment = env_S8_C8_N150
    #environment = env_S8_C2_N150
    #environment = env_S8_C16_N150

    #environment = env_S16_C4_N1000
    #environment = env_S16_C8_N1000
    environment = env_S16_C16_N1000


    input_fields = [
        #"client_cpu",
        #"server_cpu",
        #"network",         # if i split the rf by env anyway, why would i need to include env variabels ??
        #"network_latency",
        #"network_loss",
        #"source_system",   # potentialy relevant if variety in training data, also the format
        #"target_system",
        #"table",
        "client_bufpool_factor",
        "server_bufpool_factor",
        "buffer_size",
        "compression",
        "send_par",
        #"rcv_par",
        "write_par",
        "decomp_par",
        "read_par",
        "deser_par",
        "ser_par",
        "comp_par"
    ]

    cost_model = Per_Environment_RF_Cost_Model(input_fields=input_fields,
                                               metric=metric,
                                               data_per_env=500,
                                               underlying="test_cost_model",
                                               cluster=True,
                                               regression_model='xgb',
                                               network_transformation=True,
                                               history_ratio=1
                                               )

    data, suffix = get_transfer_learning_data_for_environment(environment, False)

    x = data
    y = data[metric]

    cost_model.train(x, y)



    config = get_next_suggestion(search_space, n_queries, cost_model, environment, mode, metric)


