from datetime import datetime
import time
from pathlib import Path
import threading
from queue import Queue

from experiments.experiment_scheduler.ssh_handler import SSHConnection
from experiments.model_optimizer import Stopping_Rules, data_transfer_wrapper
from experiments.model_optimizer.Helpers import *
from experiments.model_optimizer.NestedSSHHandler import NestedSSHClient
from experiments.model_optimizer.cost_model_config_finder import get_next_suggestion
from experiments.model_optimizer.environments import *
from experiments.model_optimizer.model_implementations.Weighted_Combination_RF_Cost_Model import \
    Per_Environment_RF_Cost_Model
from experiments.model_optimizer.model_implementations.lhs_search_optimizer import LHS_Search_Optimizer
from experiments.model_optimizer.model_implementations.Weighted_Combination_RF_Cost_Model_old import *
from experiments.model_optimizer.model_implementations.openbox_ask_tell import OpenBox_Ask_Tell
from experiments.model_optimizer.model_implementations.syne_tune_ask_tell import Syne_Tune_Ask_Tell
from experiments.model_optimizer.model_implementations.own_random_search import Own_Random_Search


def main():



    execute_optimization_runs_multi_threaded(ssh_hosts=reserved_hosts_big_cluster,
                                             environments_to_run=environment_list_main_envs_3_times_sorted_slowest_first,
                                             algorithms_to_run=["cost_model_xgb_rs_exc_cluster_update_only_hist_v26"],
                                             config_space=config_space_variable_parameters_generalized_FOR_NEW_ITERATION_FLEXIBLE_EX_BufSiz,
                                             metric='uncompressed_throughput',
                                             mode='max',
                                             loop_count=25,
                                             use_all_environments=True,
                                             training_data_per_env=900,
                                             max_training_transfers_per_iteration=1000,
                                             use_history=True,
                                             override=True)



    



















    '''

    input_fields = ["client_cpu",
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
                    ]

    TARGET_ENVIRONMENT = environment_1
    all_envs = [environment_1,environment_2,environment_3,environment_4,environment_5,environment_6,environment_7,environment_8,environment_9]
    all_envs.remove(TARGET_ENVIRONMENT)
    environments_without_target = all_envs

    cost_model_exc_target = Per_Environment_RF_Cost_Model(input_fields=input_fields, metric='time')
    data_exc_target = load_data_from_csv(type='random_samples_1310k',environment_list=environments_without_target)
    x, y = split_data(data_exc_target, METRIC)
    cost_model_exc_target.train(x, y)

    cost_model_all_envs = Per_Environment_RF_Cost_Model(input_fields=input_fields, metric='time')
    data_all_envs = load_data_from_csv(type='random_samples_1310k')
    x, y = split_data(data_all_envs, METRIC)
    cost_model_all_envs.train(x, y)

    optimizer = Syne_Tune_Ask_Tell(config_space=config_space_variable_parameters_generalized_1310k, metric=METRIC,
                                   mode=MODE, underlying='random_search')

    sum_factors = 0
    iterations = 20

    for i in range(0, iterations):
        suggested_config = optimizer.suggest()
        complete_config = create_complete_config(TARGET_ENVIRONMENT, METRIC, 'dict', suggested_config)

        result_exc_16 = cost_model_exc_target.predict(complete_config, TARGET_ENVIRONMENT)[METRIC]

        result_all_envs = cost_model_all_envs.predict(complete_config, TARGET_ENVIRONMENT)[METRIC]

        print(f"Cost Model All Envs : {round(result_all_envs, 2)} Cost Model not all envs : {round(result_exc_16, 2)} Factor: {round(result_all_envs / result_exc_16, 2)}")
        sum_factors = sum_factors + round(result_all_envs / result_exc_16, 2)

    print(f"Average Factor : {sum_factors / iterations}")
    '''

cost_model_algorithms = ["test_cost_model_rfs_rs", "test_cost_model_rfs_bay",
                         "cost_model_rfs_rs", "cost_model_rfs_bay",
                         "cost_model_rfs_rs_cluster", "cost_model_rfs_bay_cluster"]

#========================================================================
# The actual optimization loops
#========================================================================

def experiment_direct_optimization_loop(optimizer, environment, metric, mode, config_space, loop_count, trial_id=-1, ssh=None):

    print(f"\n----------------------------- \n now starting with optimizer {optimizer.underlying} with environment {environment_to_string(environment)} for {loop_count} iterations on [{ssh.hostname}]\n ----------------------------- \n")

    #setup stuff, create files etc.

    #if trial_id == -1:
    #    trial_id = get_next_trial_id()

    config_space_string = get_config_space_string(config_space)


    filename = f"results_{optimizer.underlying}_{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = f"results_{config_space_string}/{environment_to_string(environment)}/{optimizer.underlying}/"

    Path(filepath).mkdir(parents=True, exist_ok=True)

    results = pd.DataFrame()
    first_write_done = False

    #optimization loop

    time_lost_too_timeouts = 0

    start_outer = datetime.now()
    i = 1
    while i < loop_count+1:

        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{optimizer.underlying}] starting  transfer #{i}")
        start = datetime.now()

        #get next config
        suggested_config = optimizer.suggest()
        complete_config = create_complete_config(environment, metric, 'dict', suggested_config)

        result = None

        #run transfer
        result = data_transfer_wrapper.transfer(complete_config, i, max_retries=0, ssh=ssh)

        # if a transfer times out, save how long that timeout took
        # kinda bad to need to move the retry logic this high up
        if result['transfer_id'] == -1:
            time_lost_too_timeouts = time_lost_too_timeouts + (datetime.now() - start).total_seconds()

            #try once more
            start = datetime.now()

            result = data_transfer_wrapper.transfer(complete_config, i, max_retries=0, ssh=ssh)
            if result['transfer_id'] == -1:
                time_lost_too_timeouts = time_lost_too_timeouts + (datetime.now() - start).total_seconds()
                loop_count = loop_count+1
                # if failed twice, add another iteration to still get to the specified number of evaluations for that algorithm


        #report the result
        optimizer.report(suggested_config, result)


        #save results in csv etc.

        result['trial_id'] = trial_id
        result['algo'] = optimizer.underlying
        end_temp = datetime.now()
        result['seconds_since_start_of_opt_run'] = ((end_temp - start_outer).total_seconds() - time_lost_too_timeouts)


        df = pd.DataFrame(result, index=[0])

        if not first_write_done:
            df.to_csv(filepath + filename, mode='a', header=True)
            first_write_done = True
        else:
            df.to_csv(filepath + filename, mode='a', header=False)

        results = pd.concat([results, df], axis=0)

        end = datetime.now()
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{optimizer.underlying}] completed transfer #{i} in {(end - start).total_seconds()} seconds (result was {result[metric]})")

        #print(f"Rule no_improvment_iterations : " + str(should_stop))
        i += 1


def experiment_indirect_optimization(cost_model, optimizer, environment, metric, mode, config_space, max_training_transfers_per_iteration, max_real_transfers, ssh_host, trial_id=-1):

    print(f"\n----------------------------- \n now starting with cost model {cost_model.underlying} with environment {environment_to_string(environment)} for {max_real_transfers} real transfers on [{ssh_host}]\n ----------------------------- \n")

    # create files etc
    global result
    #if trial_id == -1:
    #    trial_id = get_next_trial_id()

    config_space_string = get_config_space_string(config_space)

    filename = f"results_{cost_model.underlying}_{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = f"results_{config_space_string}/{environment_to_string(environment)}/{cost_model.underlying}/"
    Path(filepath).mkdir(parents=True, exist_ok=True)
    results = pd.DataFrame()
    first_write_done = False

    # loop variables
    best_config = None
    best_metric = -1
    count_real_transfers = 0
    count_training_transfers = 0
    time_lost_too_timeouts = 0

    #create ssh connection
    if ssh_host in reserved_hosts_big_cluster:
        ssh = NestedSSHClient(jump_host=big_cluster_main_host,
                              jump_username=get_username_for_host(big_cluster_main_host),
                              target_host=ssh_host,
                              target_username=get_username_for_host(ssh_host))
    else:
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))


    start_outer = datetime.now()
    for i in range(1, max_real_transfers+1):

        best_predicted_config = None
        best_predicted_metric = -1

        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{cost_model.underlying}] now starting to run {max_training_transfers_per_iteration} surrogate transfers")
        '''
        start_inner = datetime.now()
        for i_inner in range(1, max_training_transfers_per_iteration + 1):

            #get a configration
            suggested_config = optimizer.suggest()
            complete_config = create_complete_config(environment, metric, 'dict', suggested_config)
            #print(f"{i} suggested config : {suggested_config}")

            #get the cost prediction for that configuration
            result_cost_model = cost_model.predict(data=complete_config, target_environment=environment, print_wieghts=False)
            result_metric = float(result_cost_model[metric])

            #if found configuration is new best, update
            if mode == 'min':
                if best_predicted_metric == -1 or best_predicted_metric > result_metric:
                    best_predicted_config = complete_config
                    best_predicted_metric = result_metric
            elif mode == 'max':
                if best_predicted_metric == -1 or best_predicted_metric < result_metric:
                    best_predicted_config = complete_config
                    best_predicted_metric = result_metric

            #report result to optimizer
            optimizer.report(suggested_config, result_cost_model)
        end_inner = datetime.now()
        '''

        best_predicted_config = get_next_suggestion(config_space,max_training_transfers_per_iteration,cost_model,environment,mode,metric)

        complete_config = create_complete_config(environment, metric, 'dict', best_predicted_config)
        best_predicted_metric = cost_model.predict(data=complete_config, target_environment=environment, print_wieghts=True)
        best_predicted_metric = float(best_predicted_metric[metric])


        #print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{cost_model.underlying}] running {max_training_transfers_per_iteration} surrogate transfers took {(end_inner - start_inner).total_seconds()} seconds")
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{cost_model.underlying}] best found configuration has predicted metric of {best_predicted_metric}  ({metric})")
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{cost_model.underlying}] now running best predicted config {best_predicted_config}")
        # Get weight table for best config



        start = datetime.now()
        # run the actual transfer
        result = data_transfer_wrapper.transfer(complete_config, i, max_retries=0, ssh=ssh)

        # if a transfer times out, save how long that timeout took
        # kinda bad to need to move the retry logic this high up
        if result['transfer_id'] == -1:
            time_lost_too_timeouts = time_lost_too_timeouts + (datetime.now() - start).total_seconds()

            #try once more
            start = datetime.now()

            result = data_transfer_wrapper.transfer(complete_config, i, max_retries=0, ssh=ssh)
            if result['transfer_id'] == -1:
                time_lost_too_timeouts = time_lost_too_timeouts + (datetime.now() - start).total_seconds()




        count_real_transfers = count_real_transfers + 1
        result_metric = float(result[metric])
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] [{cost_model.underlying}] true data transfer completed in {result['time']} seconds, result metric was {result[metric]} ({metric})")

        # add additional fields
        result['trial_id'] = trial_id
        result['algo'] = cost_model.underlying
        end_temp = datetime.now()
        result['seconds_since_start_of_opt_run'] = ((end_temp - start_outer).total_seconds() - time_lost_too_timeouts)
        result['predicted_metric'] = best_predicted_metric

        # save results to file
        df = pd.DataFrame(result, index=[0])
        if not first_write_done:
            df.to_csv(filepath + filename, mode='a', header=True)
            first_write_done = True
        else:
            df.to_csv(filepath + filename, mode='a', header=False)
        results = pd.concat([results, df], axis=0)

        #update model with new result
        cost_model.update(best_predicted_config, result)
        optimizer.reset()

        #if real execution result is new best, update
        if mode == 'min':
            if best_metric == -1 or best_metric > result_metric:
                best_config = best_predicted_config
                best_metric = result_metric
        elif mode == 'max':
            if best_metric == -1 or best_metric < result_metric:
                best_config = best_predicted_config
                best_metric = result_metric

    end_outer = datetime.now()

    print(f"[{datetime.today().strftime('%H:%M:%S')}] [{cost_model.underlying}] running {count_real_transfers} real transfers with {max_training_transfers_per_iteration * count_real_transfers} training transfers took {(end_outer - start_outer).total_seconds()} seconds")
    print(f"[{datetime.today().strftime('%H:%M:%S')}] [{cost_model.underlying}] best found configuration has metric of {best_metric} ({metric})")
    print(f"[{datetime.today().strftime('%H:%M:%S')}] [{cost_model.underlying}] best config : {best_config}")


#========================================================================
# Initialization of Optimizers / Models
#========================================================================

def execute_run(algorithm, ssh_host, environment, config_space, metric, mode, loop_count, training_data_per_env, use_all_environments, max_training_transfers_per_iteration, use_history, override):
    '''
        should work with any algorithm that is specified in the available algorithm lists
    '''

    if algorithm in OpenBox_Ask_Tell.available_transfer_algorithms + Syne_Tune_Ask_Tell.available_transfer_algorithms:
        execute_transfer_algorithm_run(algorithm, ssh_host, use_all_environments, environment, config_space, metric, mode, loop_count, training_data_per_env)

    elif algorithm in OpenBox_Ask_Tell.available_optimization_algorithms + Syne_Tune_Ask_Tell.available_optimization_algorithms:
        execute_optimization_algorithm_run(algorithm, ssh_host, environment, config_space, metric, mode, loop_count)

    elif algorithm in Own_Random_Search.available_algorithms:
        execute_optimization_algorithm_run(algorithm, ssh_host, environment, config_space, metric, mode, loop_count)

    elif algorithm in cost_model_algorithms or override:
        execute_cost_model_run(ssh_host, algorithm, config_space, metric, mode, environment, max_training_transfers_per_iteration, loop_count, use_history, use_all_environments, training_data_per_env, override)

    else:
        raise ValueError(f"Algorithm {algorithm} is not in available algorithms.")


def execute_optimization_algorithm_run(algorithm, ssh_host, environment, config_space, metric, mode, loop_count):
    if ssh_host in reserved_hosts_big_cluster:
        ssh = NestedSSHClient(jump_host=big_cluster_main_host,
                              jump_username=get_username_for_host(big_cluster_main_host) ,
                              target_host=ssh_host,
                              target_username=get_username_for_host(ssh_host))
    else:
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))


    optimizer = None

    if algorithm in Syne_Tune_Ask_Tell.available_optimization_algorithms:
        optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=algorithm)

    elif algorithm in OpenBox_Ask_Tell.available_optimization_algorithms:
        optimizer = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=algorithm)

    elif algorithm in Own_Random_Search.available_algorithms:
        optimizer = Own_Random_Search(config_space=config_space, metric=metric, mode=mode, underlying=algorithm)

    else:
        raise ValueError(f"Algorithm {algorithm} is not in available transfer algorithms of any supported library wrapper.")

    experiment_direct_optimization_loop(optimizer=optimizer, environment=environment, metric=metric, mode=mode, config_space=config_space, loop_count=loop_count, ssh=ssh)

    ssh.close()


def execute_transfer_algorithm_run(algorithm, ssh_host, use_all_environments, environment, config_space, metric, mode, loop_count, training_data_per_env):
    if ssh_host in reserved_hosts_big_cluster:
        ssh = NestedSSHClient(jump_host=big_cluster_main_host,
                              jump_username=get_username_for_host(big_cluster_main_host) ,
                              target_host=ssh_host,
                              target_username=get_username_for_host(ssh_host))
    else:
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))


    global optimizer

    data, suffix = get_transfer_learning_data_for_environment(environment, use_all_environments)

    if algorithm in Syne_Tune_Ask_Tell.available_transfer_algorithms:
        optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=algorithm+suffix)

    elif algorithm in OpenBox_Ask_Tell.available_transfer_algorithms:
        optimizer = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=algorithm+suffix)

    else:
        raise ValueError(f"Algorithm {algorithm} is not in available transfer algorithms of any supported library wrapper.")

    optimizer.load_transfer_learning_history_per_env_from_dataframe(data=data, training_data_per_env=training_data_per_env)

    experiment_direct_optimization_loop(optimizer=optimizer, environment=environment, metric=metric, mode=mode, config_space=config_space, loop_count=loop_count, ssh=ssh)

    ssh.close()


def execute_cost_model_run(ssh_host, algorithm, config_space, metric, mode, environment, max_training_transfers_per_iteration, max_real_transfers, use_history, use_all_environments, training_data_per_env, override):
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

    if "_rs" in algorithm:
        #search_optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying="random_search")
        #search_optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying="grid_search")
        search_optimizer = LHS_Search_Optimizer(config_space=config_space ,n_samples=max_training_transfers_per_iteration)
    #elif "_bay" in algorithm:
    #    search_optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying="bayesian_syne_tune")

    if "only_hist" in algorithm:
        history_ratio = 1
    elif "update" in algorithm:
        history_ratio = 0.5
    else:
        history_ratio = 0

    if "cluster" in algorithm:
        cluster = True
    else:
        cluster = False

    if "net_trans" in algorithm:
        network_transformation = True
    else:
        network_transformation = False

    if "_rfs_" in algorithm:
        regression_model = "rfs"
    elif "_gdb_" in algorithm:
        regression_model = "gdb"
    elif "_xgb_" in algorithm:
        regression_model = "xgb"


    cost_model = Per_Environment_RF_Cost_Model(input_fields=input_fields,
                                               metric=metric,
                                               data_per_env=training_data_per_env,
                                               underlying=algorithm,
                                               cluster=cluster,
                                               regression_model=regression_model,
                                               network_transformation=network_transformation,
                                               history_ratio=history_ratio
                                               )

    if cost_model is None:
        raise ValueError(f"Cost Model could not be initialized: {algorithm}")

    data, suffix = get_transfer_learning_data_for_environment(environment, use_all_environments)

    x = data#[input_fields]
    y = data[metric]

    cost_model.train(x, y)

    experiment_indirect_optimization(cost_model=cost_model,
                                     optimizer=search_optimizer,
                                     environment=environment,
                                     config_space=config_space,
                                     metric=metric,
                                     mode=mode,
                                     max_training_transfers_per_iteration=max_training_transfers_per_iteration,
                                     max_real_transfers=max_real_transfers,
                                     ssh_host=ssh_host)


def get_transfer_learning_data_for_environment(target_environment, use_all_environments, except_N_most_similar=1):

    base_path = "C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_10_5M_semi_flex"
    environments_to_use = []
    suffix = ""

    if use_all_environments:

        if target_environment in environment_list_base_envs:
            environments_to_use = environment_list_base_envs
            suffix = "_all_envs"


    # environments sorted by similarity to the key-environemt. calculated using spearman rank coefficient on a sample size of 400 samples per environment.
    dict_environment_most_similars = {
        "S2_C2_N50": ['S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C8_N50', 'S2_C8_N150', 'S16_C4_N50', 'S2_C8_N1000', 'S16_C4_N150', 'S4_C16_N50', 'S8_C16_N50', 'S16_C4_N1000', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50', 'S8_C8_N1000', 'S8_C8_N150', 'S4_C16_N150', 'S16_C8_N1000', 'S16_C8_N150', 'S8_C16_N150', 'S4_C16_N1000', 'S16_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000'],
        "S2_C8_N50": ['S2_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S8_C16_N50', 'S8_C8_N50', 'S4_C16_N150', 'S16_C16_N50', 'S16_C8_N50', 'S2_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S8_C8_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C2_N50', 'S16_C16_N150', 'S4_C16_N1000', 'S16_C4_N150', 'S8_C8_N1000', 'S8_C16_N1000', 'S16_C8_N1000', 'S16_C16_N1000', 'S2_C2_N1000', 'S2_C2_N150', 'S16_C4_N1000', 'S8_C2_N1000', 'S8_C2_N150'],
        "S4_C16_N50": ['S4_C16_N50', 'S8_C8_N50', 'S8_C16_N50', 'S16_C16_N50', 'S16_C8_N50', 'S16_C4_N50', 'S8_C16_N150', 'S8_C8_N150', 'S16_C16_N150', 'S16_C8_N150', 'S4_C16_N150', 'S2_C8_N50', 'S8_C16_N1000', 'S16_C16_N1000', 'S16_C4_N150', 'S16_C8_N1000', 'S8_C8_N1000', 'S4_C16_N1000', 'S16_C4_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N150'],
        "S8_C8_N50": ['S8_C8_N50', 'S8_C16_N50', 'S16_C16_N50', 'S16_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S16_C16_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C8_N150', 'S4_C16_N150', 'S2_C8_N50', 'S16_C16_N1000', 'S8_C16_N1000', 'S16_C8_N1000', 'S16_C4_N150', 'S8_C8_N1000', 'S16_C4_N1000', 'S4_C16_N1000', 'S8_C2_N50', 'S2_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N1000', 'S2_C2_N150'],
        "S8_C2_N50": ['S8_C2_N50', 'S2_C2_N50', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N1000', 'S2_C2_N150', 'S2_C8_N50', 'S16_C4_N50', 'S16_C4_N150', 'S4_C16_N50', 'S16_C16_N50', 'S8_C8_N50', 'S8_C16_N50', 'S16_C8_N50', 'S16_C4_N1000', 'S2_C8_N150', 'S2_C8_N1000', 'S16_C8_N150', 'S8_C8_N150', 'S16_C8_N1000', 'S8_C8_N1000', 'S4_C16_N150', 'S8_C16_N150', 'S16_C16_N150', 'S4_C16_N1000', 'S8_C16_N1000', 'S16_C16_N1000'],
        "S8_C16_N50": ['S8_C16_N50', 'S16_C16_N50', 'S16_C8_N50', 'S8_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S16_C16_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C8_N150', 'S4_C16_N150', 'S2_C8_N50', 'S16_C16_N1000', 'S8_C16_N1000', 'S16_C8_N1000', 'S16_C4_N150', 'S8_C8_N1000', 'S16_C4_N1000', 'S4_C16_N1000', 'S8_C2_N50', 'S2_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N150'],
        "S16_C4_N50": ['S16_C4_N50', 'S16_C16_N50', 'S8_C16_N50', 'S16_C8_N50', 'S8_C8_N50', 'S4_C16_N50', 'S16_C8_N150', 'S8_C8_N150', 'S16_C16_N150', 'S8_C16_N150', 'S4_C16_N150', 'S2_C8_N50', 'S16_C4_N150', 'S16_C8_N1000', 'S8_C8_N1000', 'S8_C16_N1000', 'S16_C16_N1000', 'S16_C4_N1000', 'S4_C16_N1000', 'S8_C2_N50', 'S2_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N1000', 'S2_C2_N150'],
        "S16_C8_N50": ['S16_C8_N50', 'S8_C16_N50', 'S16_C16_N50', 'S8_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S16_C16_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C8_N150', 'S4_C16_N150', 'S2_C8_N50', 'S16_C16_N1000', 'S8_C16_N1000', 'S16_C8_N1000', 'S16_C4_N150', 'S8_C8_N1000', 'S16_C4_N1000', 'S4_C16_N1000', 'S8_C2_N50', 'S2_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N1000', 'S2_C2_N150'],
        "S16_C16_N50": ['S16_C16_N50', 'S8_C16_N50', 'S16_C8_N50', 'S8_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S16_C16_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C8_N150', 'S4_C16_N150', 'S2_C8_N50', 'S16_C16_N1000', 'S8_C16_N1000', 'S16_C8_N1000', 'S16_C4_N150', 'S8_C8_N1000', 'S16_C4_N1000', 'S4_C16_N1000', 'S8_C2_N50', 'S2_C2_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N1000', 'S2_C2_N150'],
        "S2_C2_N150": ['S2_C2_N150', 'S2_C2_N1000', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N50', 'S8_C2_N50', 'S2_C8_N1000', 'S2_C8_N150', 'S16_C4_N1000', 'S2_C8_N50', 'S16_C4_N150', 'S4_C16_N1000', 'S16_C4_N50', 'S8_C8_N1000', 'S16_C8_N1000', 'S4_C16_N50', 'S4_C16_N150', 'S8_C16_N50', 'S16_C16_N50', 'S8_C8_N50', 'S16_C8_N50', 'S8_C8_N150', 'S16_C8_N150', 'S8_C16_N150', 'S16_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000'],
        "S2_C8_N150": ['S2_C8_N150', 'S2_C8_N1000', 'S4_C16_N1000', 'S2_C8_N50', 'S4_C16_N150', 'S16_C4_N1000', 'S2_C2_N150', 'S2_C2_N1000', 'S8_C8_N1000', 'S16_C4_N150', 'S16_C8_N1000', 'S2_C2_N50', 'S8_C8_N150', 'S8_C16_N1000', 'S8_C16_N150', 'S16_C8_N150', 'S16_C16_N150', 'S8_C2_N150', 'S8_C2_N1000', 'S4_C16_N50', 'S16_C16_N1000', 'S16_C4_N50', 'S8_C2_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50'],
        "S4_C16_N150": ['S4_C16_N150', 'S8_C8_N150', 'S8_C16_N150', 'S16_C8_N150', 'S4_C16_N1000', 'S16_C16_N150', 'S8_C8_N1000', 'S8_C16_N1000', 'S16_C8_N1000', 'S16_C16_N1000', 'S16_C4_N150', 'S4_C16_N50', 'S16_C4_N50', 'S8_C16_N50', 'S16_C16_N50', 'S8_C8_N50', 'S16_C8_N50', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N150', 'S8_C2_N1000'],
        "S8_C8_N150": ['S8_C8_N150', 'S16_C8_N150', 'S8_C16_N150', 'S16_C16_N150', 'S4_C16_N150', 'S8_C8_N1000', 'S16_C8_N1000', 'S8_C16_N1000', 'S16_C16_N1000', 'S16_C4_N150', 'S4_C16_N50', 'S16_C4_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C8_N50', 'S16_C16_N50', 'S4_C16_N1000', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150'],
        "S8_C2_N150": ['S8_C2_N150', 'S8_C2_N1000', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N50', 'S2_C2_N50', 'S16_C4_N1000', 'S2_C8_N1000', 'S2_C8_N150', 'S16_C4_N150', 'S2_C8_N50', 'S16_C4_N50', 'S4_C16_N50', 'S16_C16_N50', 'S8_C8_N1000', 'S16_C8_N50', 'S8_C8_N50', 'S8_C16_N50', 'S16_C8_N1000', 'S4_C16_N1000', 'S16_C8_N150', 'S8_C8_N150', 'S4_C16_N150', 'S8_C16_N150', 'S16_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000'],
        "S8_C16_N150": ['S8_C16_N150', 'S16_C16_N150', 'S8_C8_N150', 'S16_C8_N150', 'S8_C16_N1000', 'S4_C16_N150', 'S16_C16_N1000', 'S16_C8_N1000', 'S8_C8_N1000', 'S4_C16_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50', 'S16_C4_N50', 'S16_C4_N150', 'S4_C16_N1000', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N150', 'S8_C2_N1000'],
        "S16_C4_N150": ['S16_C4_N150', 'S16_C4_N1000', 'S16_C8_N1000', 'S8_C8_N1000', 'S8_C8_N150', 'S16_C8_N150', 'S8_C16_N150', 'S4_C16_N150', 'S16_C16_N150', 'S4_C16_N1000', 'S16_C4_N50', 'S8_C16_N1000', 'S4_C16_N50', 'S16_C16_N50', 'S8_C16_N50', 'S16_C8_N50', 'S8_C8_N50', 'S16_C16_N1000', 'S2_C8_N150', 'S2_C8_N50', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S8_C2_N150', 'S2_C2_N150', 'S8_C2_N1000'],
        "S16_C8_N150": ['S16_C8_N150', 'S8_C8_N150', 'S16_C16_N150', 'S8_C16_N150', 'S16_C8_N1000', 'S8_C8_N1000', 'S4_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000', 'S16_C4_N150', 'S8_C16_N50', 'S16_C4_N50', 'S16_C16_N50', 'S8_C8_N50', 'S16_C8_N50', 'S4_C16_N50', 'S4_C16_N1000', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N150', 'S8_C2_N1000'],
        "S16_C16_N150": ['S16_C16_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C8_N150', 'S16_C16_N1000', 'S16_C8_N1000', 'S8_C16_N1000', 'S8_C8_N1000', 'S4_C16_N150', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S16_C4_N150', 'S4_C16_N1000', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N150', 'S8_C2_N1000'],
        "S2_C2_N1000": ['S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N50', 'S8_C2_N50', 'S2_C8_N1000', 'S2_C8_N150', 'S2_C8_N50', 'S16_C4_N1000', 'S16_C4_N150', 'S4_C16_N1000', 'S16_C4_N50', 'S8_C8_N1000', 'S4_C16_N50', 'S16_C8_N1000', 'S4_C16_N150', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50', 'S8_C8_N150', 'S16_C8_N150', 'S8_C16_N150', 'S16_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000'],
        "S2_C8_N1000": ['S2_C8_N1000', 'S2_C8_N150', 'S4_C16_N1000', 'S2_C8_N50', 'S4_C16_N150', 'S2_C2_N150', 'S2_C2_N1000', 'S16_C4_N1000', 'S8_C8_N1000', 'S16_C4_N150', 'S16_C8_N1000', 'S2_C2_N50', 'S8_C8_N150', 'S8_C16_N1000', 'S8_C16_N150', 'S8_C2_N150', 'S16_C8_N150', 'S8_C2_N1000', 'S16_C16_N150', 'S16_C16_N1000', 'S4_C16_N50', 'S16_C4_N50', 'S8_C2_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50'],
        "S4_C16_N1000": ['S4_C16_N1000', 'S4_C16_N150', 'S8_C8_N1000', 'S16_C8_N1000', 'S8_C16_N1000', 'S8_C8_N150', 'S2_C8_N150', 'S16_C4_N1000', 'S2_C8_N1000', 'S8_C16_N150', 'S16_C4_N150', 'S16_C8_N150', 'S16_C16_N1000', 'S16_C16_N150', 'S2_C8_N50', 'S4_C16_N50', 'S16_C4_N50', 'S8_C16_N50', 'S16_C16_N50', 'S8_C8_N50', 'S16_C8_N50', 'S2_C2_N150', 'S2_C2_N1000', 'S2_C2_N50', 'S8_C2_N150', 'S8_C2_N1000', 'S8_C2_N50'],
        "S8_C8_N1000": ['S8_C8_N1000', 'S16_C8_N1000', 'S8_C8_N150', 'S8_C16_N1000', 'S16_C8_N150', 'S8_C16_N150', 'S16_C16_N150', 'S4_C16_N150', 'S16_C16_N1000', 'S16_C4_N150', 'S4_C16_N1000', 'S16_C4_N1000', 'S16_C4_N50', 'S4_C16_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S16_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C8_N50', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150'],
        "S8_C2_N1000": ['S8_C2_N1000', 'S8_C2_N150', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N50', 'S2_C2_N50', 'S2_C8_N1000', 'S16_C4_N1000', 'S2_C8_N150', 'S16_C4_N150', 'S2_C8_N50', 'S16_C4_N50', 'S4_C16_N50', 'S8_C8_N1000', 'S16_C16_N50', 'S16_C8_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C8_N1000', 'S4_C16_N1000', 'S8_C8_N150', 'S16_C8_N150', 'S4_C16_N150', 'S8_C16_N150', 'S16_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000'],
        "S8_C16_N1000": ['S8_C16_N1000', 'S16_C16_N1000', 'S8_C16_N150', 'S8_C8_N1000', 'S8_C8_N150', 'S16_C8_N1000', 'S16_C16_N150', 'S16_C8_N150', 'S4_C16_N150', 'S4_C16_N1000', 'S16_C4_N150', 'S4_C16_N50', 'S8_C8_N50', 'S8_C16_N50', 'S16_C8_N50', 'S16_C16_N50', 'S16_C4_N50', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150'],
        "S16_C4_N1000": ['S16_C4_N1000', 'S16_C4_N150', 'S8_C8_N1000', 'S16_C8_N1000', 'S4_C16_N1000', 'S8_C8_N150', 'S16_C8_N150', 'S4_C16_N150', 'S8_C16_N150', 'S16_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000', 'S2_C8_N150', 'S2_C8_N1000', 'S16_C4_N50', 'S4_C16_N50', 'S8_C16_N50', 'S16_C16_N50', 'S16_C8_N50', 'S8_C8_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N150', 'S8_C2_N1000', 'S2_C2_N50', 'S2_C8_N50', 'S8_C2_N50'],
        "S16_C8_N1000": ['S16_C8_N1000', 'S8_C8_N1000', 'S16_C8_N150', 'S8_C8_N150', 'S16_C16_N150', 'S8_C16_N150', 'S8_C16_N1000', 'S16_C16_N1000', 'S16_C4_N150', 'S4_C16_N150', 'S4_C16_N1000', 'S16_C4_N1000', 'S16_C4_N50', 'S8_C16_N50', 'S8_C8_N50', 'S16_C16_N50', 'S4_C16_N50', 'S16_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C8_N50', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150'],
        "S16_C16_N1000": ['S16_C16_N1000', 'S8_C16_N1000', 'S16_C16_N150', 'S8_C16_N150', 'S16_C8_N150', 'S8_C8_N150', 'S16_C8_N1000', 'S8_C8_N1000', 'S4_C16_N150', 'S4_C16_N1000', 'S8_C16_N50', 'S8_C8_N50', 'S16_C8_N50', 'S16_C16_N50', 'S4_C16_N50', 'S16_C4_N150', 'S16_C4_N50', 'S16_C4_N1000', 'S2_C8_N50', 'S2_C8_N150', 'S2_C8_N1000', 'S2_C2_N50', 'S8_C2_N50', 'S2_C2_N1000', 'S2_C2_N150', 'S8_C2_N1000', 'S8_C2_N150']
    }

    if environment_to_string(target_environment) in dict_environment_most_similars.keys():

        environments_to_use = dict_environment_most_similars[environment_to_string(target_environment)][except_N_most_similar:]

        environments_not_used = dict_environment_most_similars[environment_to_string(target_environment)][:except_N_most_similar]

        suffix = "_exc"

        suffix_alt = suffix + f"_top_{except_N_most_similar}_similar"

        for environment in environments_not_used:
            suffix = suffix + "_" + environment.replace("_", "")


    #environments_to_use = dict_environment_most_similars[environment_to_string(target_environment)][0] #temporary todo todo
    #environments_to_use = [environments_to_use]

    time_threshold = 2
    file_list = [glob.glob(f"{base_path}/{signature}_random_sample*.csv") for signature in environments_to_use]
    data_frames = []


    base_path_currated = "C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/currated_datasets_100high_100_rest"
    file_list_currated = [glob.glob(f"{base_path_currated}/{signature}_currated_dataset*.csv") for signature in environments_to_use]
    for file in file_list_currated:
        if file:
            df = pd.read_csv(file[0])
            df = df[(df['time'] > time_threshold)]
            data_frames.append(df)


    for file in file_list:
        if file:
            df = pd.read_csv(file[0])
            df = df[(df['transfer_id'] > 0)]
            data_frames.append(df)
    data = pd.concat(data_frames, axis=0, ignore_index=True) if data_frames else pd.DataFrame()

    return data, suffix


'''
def test_transfer_on_changing_environment_setup(algo):
    optimizer = OpenBox_Ask_Tell(config_space=config_space_variable_parameters_generalized_1310k, metric='time', mode='min', underlying=algo + '_changing_envs')

    file_list_env_1 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C16_N1000_*.csv')
    file_list_env_2 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C8_N100_*.csv')
    file_list_env_3 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S8_C1_N50_*.csv')
    file_list_env_4 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C16_N10000_*.csv')
    file_list_env_5 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C4_N10_*.csv')
    file_list_env_6 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S8_C8_N500_*.csv')
    file_list_env_7 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S2_C2_N20_*.csv')
    file_list_env_8 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C1_N1000_*.csv')
    file_list_env_9 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C1_N10_*.csv')

    file_list_optimizer = [file_list_env_2, file_list_env_4, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_8, file_list_env_9]

    optimizer.load_transfer_learning_history_per_env(file_list_optimizer)


    ssh = SSHConnection("cloud-10.dima.tu-berlin.de", get_username_for_host("cloud-10.dima.tu-berlin.de"))

    test_transfer_on_changing_environment(optimizer=optimizer,
                                          environment1=environment_1,
                                          environment2=environment_3,
                                          metric='time',
                                          config_space=config_space_variable_parameters_generalized_1310k,
                                          loop_count=25,
                                          ssh=ssh)

    ssh.close()


def test_transfer_on_changing_environment(optimizer, environment1, environment2, metric, config_space, loop_count, trial_id=-1, ssh=None):
    #start with env1, then go to env3
    iterations_per_env = loop_count

    total_iterations = iterations_per_env * 2


    print(f"\n----------------------------- \n now starting with optimizer {optimizer.underlying} with environment {environment_to_string(environment1)} for {loop_count} iterations\n ----------------------------- \n")

    #setup stuff, create files etc.

    if trial_id == -1:
        trial_id = get_next_trial_id()

    config_space_string = get_config_space_string(config_space)


    filename = f"results_{optimizer.underlying}_{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = f"results_{config_space_string}/changing_env1_to_env3/{optimizer.underlying}/"

    Path(filepath).mkdir(parents=True, exist_ok=True)

    results = pd.DataFrame()
    first_write_done = False

    current_environment = environment1

    #optimization loop

    time_lost_too_timeouts = 0

    start_outer = datetime.now()
    i = 1
    while i < total_iterations+1:

        if i > iterations_per_env:
            if current_environment == environment1:
                current_environment = environment2
                print(f"\n----------------------------- \n now changed from environemnt {environment_to_string(environment1)} to environment {environment_to_string(environment2)} after {i} iterations\n ----------------------------- \n")



        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] starting  transfer #{i}")
        start = datetime.now()

        #get next config
        suggested_config = optimizer.suggest()
        complete_config = create_complete_config(current_environment, metric, 'dict', suggested_config)

        result = None

        #run transfer
        result = data_transfer_wrapper.transfer(complete_config, i, max_retries=0, ssh=ssh)

        #report the result
        optimizer.report(suggested_config, result)


        # if a transfer times out, save how long that timeout took
        # kinda bad to need to move the retry logic this high up
        if result['time'] == -1:
            time_lost_too_timeouts = time_lost_too_timeouts + (datetime.now() - start).total_seconds()

            #try once more
            start = datetime.now()

            result = data_transfer_wrapper.transfer(complete_config, i, max_retries=0, ssh=ssh)
            if result['time'] == -1:
                time_lost_too_timeouts = time_lost_too_timeouts + (datetime.now() - start).total_seconds()
                loop_count = loop_count+1
                # if failed twice, add another iteration to still get to the specified number of evaluations for that algorithm


        #save results in csv etc.

        result['trial_id'] = trial_id
        result['algo'] = optimizer.underlying
        end_temp = datetime.now()
        result['seconds_since_start_of_opt_run'] = ((end_temp - start_outer).total_seconds() - time_lost_too_timeouts)


        df = pd.DataFrame(result, index=[0])

        if not first_write_done:
            df.to_csv(filepath + filename, mode='a', header=True)
            first_write_done = True
        else:
            df.to_csv(filepath + filename, mode='a', header=False)

        results = pd.concat([results, df], axis=0)

        end = datetime.now()
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] completed transfer #{i} in {(end - start).total_seconds()} seconds (result time was {result['time']})")


        i += 1

    pass
'''


#========================================================================
# Scheduling the execution of optimization runs
#========================================================================

def queue_function(queue, ssh_host):
    while not queue.empty():
        environment, algo, config_space, metric, mode, loop_count, training_data_per_env, use_all_environments, max_training_transfers_per_iteration, use_history, override = queue.get()
        try:
            execute_run(algorithm=algo,
                        ssh_host=ssh_host,
                        use_all_environments=use_all_environments,
                        environment=environment,
                        config_space=config_space,
                        metric=metric,
                        mode=mode,
                        loop_count=loop_count,
                        training_data_per_env=training_data_per_env,
                        max_training_transfers_per_iteration=max_training_transfers_per_iteration,
                        use_history=use_history,
                        override=override)
        finally:
            queue.task_done()
            print(f"\n\n[{datetime.today().strftime('%H:%M:%S')}] Optimization Runs left in Queue : {queue.qsize()}\n\n")

    print(f"\n\n[{datetime.today().strftime('%H:%M:%S')}] [{ssh_host}] Thread finished execution, Queue is empty.\n\n")


def execute_optimization_runs_multi_threaded(ssh_hosts, environments_to_run, algorithms_to_run, config_space, metric, mode, loop_count, training_data_per_env=-1, use_all_environments=None, max_training_transfers_per_iteration=None, use_history=None, override=False):

    queue = Queue()

    for algo in algorithms_to_run:
        for environment in environments_to_run:

            queue.put((environment, algo, config_space, metric, mode, loop_count, training_data_per_env, use_all_environments, max_training_transfers_per_iteration, use_history, override))

    total_number_of_transfers = len(environments_to_run) * len(algorithms_to_run) * loop_count
    total_time_estimate = 0

    overhead_per_run = 22  # seconds

    for env in environments_to_run:
        if env == env_S2_C2_N50:                        # average run time per transfer in seconds, calculated from random samples.
            total_time_estimate = total_time_estimate + ((186 + overhead_per_run) * len(algorithms_to_run) * loop_count)
        if env == env_S2_C8_N50:
            total_time_estimate = total_time_estimate + ((143 + overhead_per_run) * len(algorithms_to_run) * loop_count)
        if env == env_S4_C16_N50:
            total_time_estimate = total_time_estimate + ((116 + overhead_per_run) * len(algorithms_to_run) * loop_count)

        if env == env_S8_C8_N150:
            total_time_estimate = total_time_estimate + ((61 + overhead_per_run) * len(algorithms_to_run) * loop_count)
        if env == env_S8_C2_N150:
            total_time_estimate = total_time_estimate + ((164 + overhead_per_run) * len(algorithms_to_run) * loop_count)
        if env == env_S8_C16_N150:
            total_time_estimate = total_time_estimate + ((60 + overhead_per_run) * len(algorithms_to_run) * loop_count)

        if env == env_S16_C4_N1000:
            total_time_estimate = total_time_estimate + ((79 + overhead_per_run) * len(algorithms_to_run) * loop_count)
        if env == env_S16_C8_N1000:
            total_time_estimate = total_time_estimate + ((54 + overhead_per_run) * len(algorithms_to_run) * loop_count)
        if env == env_S16_C16_N1000:
            total_time_estimate = total_time_estimate + ((50 + overhead_per_run) * len(algorithms_to_run) * loop_count)

    print(total_time_estimate)

    total_time_estimate = total_time_estimate * 1.5  # overhead for boilerplate stuff, timeoutes, logging etc...
    total_time_estimate = total_time_estimate / len(ssh_hosts)

    hours, remainder = divmod(total_time_estimate, 3600)
    minutes, seconds = divmod(remainder, 60)


    print(f"\n\n[{datetime.today().strftime('%H:%M:%S')}] Time Estimate for all Runs : {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
    print(f"[{datetime.today().strftime('%H:%M:%S')}] Optimization Runs in Queue : {queue.qsize()}")
    print(f"[{datetime.today().strftime('%H:%M:%S')}] Total Transfers in Queue : {total_number_of_transfers}\n\n")


    # start threads
    threads = []
    for i in range(len(ssh_hosts)):
        thread = threading.Thread(target=queue_function, args=(queue, ssh_hosts[i]))
        thread.start()
        threads.append(thread)
        time.sleep(15)  # so no threads start at the same time, bc filenames depend on timestamp


    # wait for threads to finish
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
