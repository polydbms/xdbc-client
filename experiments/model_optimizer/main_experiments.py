import time
from pathlib import Path
import threading
from queue import Queue
from experiments.experiment_scheduler.ssh_handler import SSHConnection
from experiments.model_optimizer import Stopping_Rules, data_transfer_wrapper
from experiments.model_optimizer.Configs import *
from experiments.model_optimizer.Helpers import *
from experiments.model_optimizer.model_implementations.Weighted_Combination_RF_Cost_Model import \
    Per_Environment_RF_Cost_Model
from experiments.model_optimizer.model_implementations.openbox_ask_tell import OpenBox_Ask_Tell
from experiments.model_optimizer.model_implementations.syne_tune_ask_tell import Syne_Tune_Ask_Tell


def main():


    '''
    algos = [#"tlbo_sgpr_gp",
             #"tlbo_topov3_gp",
             #"tlbo_sgpr_prf",
             #"tlbo_topov3_prf",
             #"tlbo_rgpe_gp",
             #"tlbo_sgpr_gp",
             "tlbo_topov3_gp"
    ]


    for algo in algos:
        test_transfer_on_changing_environment_setup(algo)
    '''



    '''
    test_cost_model_on_unknown_environments(ssh_host="cloud-7.dima.tu-berlin.de",
                                            environments_to_run=environments_list,
                                            config_space=config_space_variable_parameters_generalized_1310k,
                                            metric='time',
                                            mode='min',
                                            max_training_transfers_per_iteration=500,
                                            max_real_transfers=25)

    test_cost_model_on_all_environments(ssh_host="cloud-7.dima.tu-berlin.de",
                                        environments_to_run=environments_list,
                                        config_space=config_space_variable_parameters_generalized_1310k,
                                        metric='time',
                                        mode='min',
                                        max_training_transfers_per_iteration=500,
                                        max_real_transfers=25)

    test_cost_model_on_unknown_environments(ssh_host="cloud-7.dima.tu-berlin.de",
                                            environments_to_run=environments_list,
                                            config_space=config_space_variable_parameters_generalized_1310k,
                                            metric='time',
                                            mode='min',
                                            max_training_transfers_per_iteration=500,
                                            max_real_transfers=25)

    test_cost_model_on_all_environments(ssh_host="cloud-7.dima.tu-berlin.de",
                                        environments_to_run=environments_list,
                                        config_space=config_space_variable_parameters_generalized_1310k,
                                        metric='time',
                                        mode='min',
                                        max_training_transfers_per_iteration=500,
                                        max_real_transfers=25)
    '''

    # EXAMPLE :
    #execute_optimization_runs_multi_threaded(ssh_hosts = ["cloud-7.dima.tu-berlin.de", "cloud-8.dima.tu-berlin.de", "cloud-9.dima.tu-berlin.de", "cloud-10.dima.tu-berlin.de"],
    #                            environments_to_run= [environment_1, environment_2, environment_3, environment_4, environment_6, environment_7, environment_8, environment_5, environment_9],
    #                            algorithms_to_run=["random_search_syne_tune", "bayesian_open_box",  "bayesian_syne_tune"],
    #                            config_space=config_space_variable_parameters_generalized_1310k,
    #                            metric='time',
    #                            mode='min',
    #                            loop_count=25)

    algos_to_run =[
        #"tlbo_rgpe_prf_all",
        #"tlbo_sgpr_prf_all",
        #"tlbo_topov3_prf_all",
        #"tlbo_rgpe_gp_all",
        #"tlbo_sgpr_gp_all",
        #"tlbo_topov3_gp_all",
        "tlbo_rgpe_prf_exc",
        #"tlbo_sgpr_prf_exc",
        "tlbo_topov3_prf_exc",
        #"tlbo_rgpe_gp_exc",
        #"tlbo_sgpr_gp_exc",
        #"tlbo_topov3_gp_exc",
        #"zero_shot_all",
        #"quantile_all",
        "zero_shot_exc",
        "quantile_exc"
    ]

    for n in [25,50,75,100,150,250,350,450]:


        execute_optimization_runs_multi_threaded(ssh_hosts = ["cloud-7.dima.tu-berlin.de"],#, "cloud-8.dima.tu-berlin.de", "cloud-9.dima.tu-berlin.de", "cloud-10.dima.tu-berlin.de"],
                                    environments_to_run= [environment_1, environment_2, environment_3],
                                    algorithms_to_run=algos_to_run,
                                    config_space=config_space_variable_parameters_generalized_1310k,
                                    metric='time',
                                    mode='min',
                                    loop_count=25,
                                    training_data_per_env=n)










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






def experiment_indirect_optimization(cost_model, optimizer, environment, metric, config_space, max_training_transfers_per_iteration, max_real_transfers,ssh_host, trial_id=-1):
    # train cost model

    # search cost model for config

    # run actual transfer

    # update model

    # repeat

    #
    #
    #
    #
    #
    #   currently hardcoded for weighted random forests cost model
    #   cant handle timeouts in any way
    #
    #
    #
    #
    #



    # create files etc
    global result
    if trial_id == -1:
        trial_id = get_next_trial_id()

    config_space_string = get_config_space_string(config_space)

    filename = f"results_{cost_model.underlying}_{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    filepath = f"results_{config_space_string}/{environment_to_string(environment)}/{cost_model.underlying}/"
    Path(filepath).mkdir(parents=True, exist_ok=True)
    results = pd.DataFrame()
    first_write_done = False


    # create cost model
    #data = load_data_from_csv(type='random_samples_1310k')
    #x, y = split_data(data, metric)
    #cost_model.train(x, y)


    # loop variables
    best_config = None
    best_time = -1
    count_real_transfers = 0
    count_training_transfers = 0



    #create ssh connection if supplied
    ssh=None
    if ssh_host is not None:
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))



    start_outer = datetime.now()
    for i in range(1, max_real_transfers):

        best_predicted_config = None
        best_predicted_time = -1

        print(f"[{datetime.today().strftime('%H:%M:%S')}] now starting to run {max_training_transfers_per_iteration} surrogate transfers")

        start_inner = datetime.now()
        for i_inner in range(1, max_training_transfers_per_iteration + 1):


            #get a configration
            suggested_config = optimizer.suggest()
            complete_config = create_complete_config(environment, metric, 'dict', suggested_config)
            #print(f"{i} suggested config : {suggested_config}")


            #get the cost prediction for that configuration
            result_cost_model = cost_model.predict(complete_config, environment)
            result_time = float(result_cost_model[metric])
            #print(f"{i_inner} predicted time : {result_time}")


            #if found configuration is new best, update
            if best_predicted_time == -1 or best_predicted_time > result_time:
                best_predicted_config = complete_config
                best_predicted_time = result_time


            #report result to optimizer
            optimizer.report(suggested_config, result_cost_model)
        end_inner = datetime.now()


        print(f"[{datetime.today().strftime('%H:%M:%S')}] running {max_training_transfers_per_iteration} surrogate transfers took {(end_inner - start_inner).total_seconds()} seconds")
        print(f"[{datetime.today().strftime('%H:%M:%S')}] best found configuration has predicted time of {best_predicted_time}")
        print(f"[{datetime.today().strftime('%H:%M:%S')}] now running best predicted config {best_predicted_config}")

        # run the actual transfer
        result = data_transfer_wrapper.transfer(best_predicted_config, i,ssh=ssh)
        count_real_transfers = count_real_transfers + 1
        result_time = float(result[metric])
        print(f"[{datetime.today().strftime('%H:%M:%S')}] true data transfer completed in {result['time']} seconds \n")


        # add additional fields
        result['trial_id'] = trial_id
        result['algo'] = cost_model.underlying
        end_temp = datetime.now()
        result['seconds_since_start_of_opt_run'] = ((end_temp - start_outer).total_seconds())


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
        if best_time == -1 or best_time > result_time:
            best_config = best_predicted_config
            best_time = result_time



    end_outer = datetime.now()

    print(f"[{datetime.today().strftime('%H:%M:%S')}] running {count_real_transfers} real transfers with {max_training_transfers_per_iteration * count_real_transfers} training transfers took {(end_outer - start_outer).total_seconds()} seconds")
    print(f"[{datetime.today().strftime('%H:%M:%S')}] best found configuration has time of {best_time}")
    print(f"[{datetime.today().strftime('%H:%M:%S')}] best config : {best_config}")

def test_cost_model_on_unknown_environments(ssh_host,config_space, metric, mode, environments_to_run,max_training_transfers_per_iteration, max_real_transfers,):
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
                    "compression",
                    "send_par",
                    #"rcv_par",
                    "write_par",
                    "decomp_par",
                    "read_partitions",
                    "read_par",
                    "deser_par",
                    "comp_par"
                    ]


    for environment in environments_to_run:

        envs_to_exclude = []
        if environment == environment_1:
            envs_to_exclude = [environment_1,environment_4]
        if environment == environment_2:
            envs_to_exclude = [environment_2,environment_5]
        if environment == environment_3:
            envs_to_exclude = [environment_3,environment_8]
        if environment == environment_4:
            envs_to_exclude = [environment_1,environment_4]
        if environment == environment_5:
            envs_to_exclude = [environment_2,environment_5]
        if environment == environment_6:
            envs_to_exclude = [environment_6,environment_7]
        if environment == environment_7:
            envs_to_exclude = [environment_6,environment_7]
        if environment == environment_8:
            envs_to_exclude = [environment_3,environment_8]
        if environment == environment_9:
            envs_to_exclude = [environment_3,environment_9]

        all_envs_inner = [environment_1,environment_2,environment_3,environment_4,environment_5,environment_6,environment_7,environment_8,environment_9]


        for env in envs_to_exclude:
            if env in all_envs_inner:
                all_envs_inner.remove(env)
        environments_without_target = all_envs_inner

        # create cost model
        cost_model = Per_Environment_RF_Cost_Model(input_fields=input_fields,metric=metric, underlying="cost_model_rfs_exc")

        data = load_data_from_csv(type='random_samples_1310k',environment_list=environments_without_target)

        x, y = split_data(data, metric=metric)
        cost_model.train(x, y)


        # use random search for maximum exploration ? or use 'actual' optimization algorithm for better exploitation ?
        optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying="random_search")
        experiment_indirect_optimization(cost_model=cost_model,
                                         optimizer=optimizer,
                                         environment=environment,
                                         config_space=config_space,
                                         metric=metric,
                                         max_training_transfers_per_iteration=max_training_transfers_per_iteration,
                                         max_real_transfers=max_real_transfers,
                                         ssh_host=ssh_host)


def test_cost_model_on_all_environments(ssh_host,config_space, metric, mode, environments_to_run,max_training_transfers_per_iteration, max_real_transfers,):
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
                    "compression",
                    "send_par",
                    #"rcv_par",
                    "write_par",
                    "decomp_par",
                    "read_partitions",
                    "read_par",
                    "deser_par",
                    "comp_par"
                    ]


    for environment in environments_to_run:



        all_envs_inner = [environment_1,environment_2,environment_3,environment_4,environment_5,environment_6,environment_7,environment_8,environment_9]

        # create cost model
        cost_model = Per_Environment_RF_Cost_Model(input_fields=input_fields,metric=metric, underlying="cost_model_rfs_all")

        data = load_data_from_csv(type='random_samples_1310k',environment_list=all_envs_inner)

        x, y = split_data(data, metric=metric)
        cost_model.train(x, y)


        # use random search for maximum exploration ? or use 'actual' optimization algorithm for better exploitation ?
        optimizer = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying="random_search")
        experiment_indirect_optimization(cost_model=cost_model,
                                         optimizer=optimizer,
                                         environment=environment,
                                         config_space=config_space,
                                         metric=metric,
                                         max_training_transfers_per_iteration=max_training_transfers_per_iteration,
                                         max_real_transfers=max_real_transfers,
                                         ssh_host=ssh_host)


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



def experiment_direct_optimization_loop(optimizer, environment, metric, config_space, loop_count, trial_id=-1, ssh=None):

    print(f"\n----------------------------- \n now starting with optimizer {optimizer.underlying} with environment {environment_to_string(environment)} for {loop_count} iterations\n ----------------------------- \n")

    #setup stuff, create files etc.

    if trial_id == -1:
        trial_id = get_next_trial_id()

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

        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] starting  transfer #{i}")
        start = datetime.now()

        #get next config
        suggested_config = optimizer.suggest()
        complete_config = create_complete_config(environment, metric, 'dict', suggested_config)

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

        #should_stop = Stopping_Rules.get_decision("no_improvment_iterations", results['time'].values)

        #print(f"Rule no_improvment_iterations : " + str(should_stop))
        i += 1


def transfer_all(ssh_host,environments_to_run, config_space, metric, mode, underlying, library, loop_count, training_data_per_env):

    global optimizer_1,optimizer_2,optimizer_3,optimizer_4,optimizer_5,optimizer_6,optimizer_7,optimizer_8,optimizer_9
    ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

    if library == "syne_tune":
        optimizer_1 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_2 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_3 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_4 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_5 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_6 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_7 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_8 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')
        optimizer_9 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_all_envs')

    elif library == "openbox":
        optimizer_1 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_2 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_3 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_4 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_5 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_6 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_7 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_8 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')
        optimizer_9 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_all_envs')

    file_list_env_1 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C16_N1000_*.csv')
    file_list_env_2 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C8_N100_*.csv')
    file_list_env_3 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S8_C1_N50_*.csv')
    file_list_env_4 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C16_N10000_*.csv')
    file_list_env_5 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C4_N10_*.csv')
    file_list_env_6 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S8_C8_N500_*.csv')
    file_list_env_7 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S2_C2_N20_*.csv')
    file_list_env_8 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C1_N1000*.csv')
    file_list_env_9 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C1_N10_*.csv')

    for optimizer,environment in [(optimizer_1,environment_1),
                                  (optimizer_2,environment_2),
                                  (optimizer_3,environment_3),
                                  (optimizer_4,environment_4),
                                  (optimizer_5,environment_5),
                                  (optimizer_6,environment_6),
                                  (optimizer_7,environment_7),
                                  (optimizer_8,environment_8),
                                  (optimizer_9,environment_9)]:

        if environment in environments_to_run:
            print(f"Executing on {environment_to_string(environment)}")
            #try:
            optimizer.load_transfer_learning_history_per_env(file_list=[file_list_env_1, file_list_env_2, file_list_env_3, file_list_env_4, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_8, file_list_env_9], training_data_per_env=training_data_per_env)
            experiment_direct_optimization_loop(optimizer=optimizer, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
            #except:
            #    pass
        else:
            print(f"NOT executing on {environment_to_string(environment)}")

    ssh.close()


def transfer_exc(ssh_host,environments_to_run, config_space, metric, mode, underlying, library, loop_count, training_data_per_env):

    global optimizer_1,optimizer_2,optimizer_3,optimizer_4,optimizer_5,optimizer_6,optimizer_7,optimizer_8,optimizer_9
    ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

    if library == "syne_tune":
        optimizer_1 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env1_env4')
        optimizer_2 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env2_env5')
        optimizer_3 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env3_env8')
        optimizer_4 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env1_env4')
        optimizer_5 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env2_env5')
        optimizer_6 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env6_env7')
        optimizer_7 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env6_env7')
        optimizer_8 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env3_env8')
        optimizer_9 = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_transfer_exc_env3_env9')

    elif library == "openbox":
        optimizer_1 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env1_env4')
        optimizer_2 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env2_env5')
        optimizer_3 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env3_env8')
        optimizer_4 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env1_env4')
        optimizer_5 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env2_env5')
        optimizer_6 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env6_env7')
        optimizer_7 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env6_env7')
        optimizer_8 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env3_env8')
        optimizer_9 = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying=underlying+'_exc_env3_env9')

    file_list_env_1 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C16_N1000_*.csv')
    file_list_env_2 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C8_N100_*.csv')
    file_list_env_3 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S8_C1_N50_*.csv')
    file_list_env_4 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C16_N10000_*.csv')
    file_list_env_5 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C4_N10_*.csv')
    file_list_env_6 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S8_C8_N500_*.csv')
    file_list_env_7 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S2_C2_N20_*.csv')
    file_list_env_8 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C1_N1000_*.csv')
    file_list_env_9 = glob.glob('C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/random_samples_1310k/S16_C1_N10_*.csv')

    file_list_optimizer_1 = [file_list_env_2, file_list_env_3, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_8, file_list_env_9]
    file_list_optimizer_2 = [file_list_env_1, file_list_env_3, file_list_env_4, file_list_env_6, file_list_env_7, file_list_env_8, file_list_env_9]
    file_list_optimizer_3 = [file_list_env_1, file_list_env_2, file_list_env_4, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_9]
    file_list_optimizer_4 = [file_list_env_2, file_list_env_3, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_8, file_list_env_9]
    file_list_optimizer_5 = [file_list_env_1, file_list_env_3, file_list_env_4, file_list_env_6, file_list_env_7, file_list_env_8, file_list_env_9]
    file_list_optimizer_6 = [file_list_env_1, file_list_env_2, file_list_env_3, file_list_env_4, file_list_env_5, file_list_env_8, file_list_env_9]
    file_list_optimizer_7 = [file_list_env_1, file_list_env_2, file_list_env_3, file_list_env_4, file_list_env_5, file_list_env_8, file_list_env_9]
    file_list_optimizer_8 = [file_list_env_1, file_list_env_2, file_list_env_4, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_9]
    file_list_optimizer_9 = [file_list_env_1, file_list_env_2, file_list_env_4, file_list_env_5, file_list_env_6, file_list_env_7, file_list_env_8]

    for optimizer,environment,file_list in [(optimizer_1,environment_1,file_list_optimizer_1),
                                          (optimizer_2,environment_2,file_list_optimizer_2),
                                          (optimizer_3,environment_3,file_list_optimizer_3),
                                          (optimizer_4,environment_4,file_list_optimizer_4),
                                          (optimizer_5,environment_5,file_list_optimizer_5),
                                          (optimizer_6,environment_6,file_list_optimizer_6),
                                          (optimizer_7,environment_7,file_list_optimizer_7),
                                          (optimizer_8,environment_8,file_list_optimizer_8),
                                          (optimizer_9,environment_9,file_list_optimizer_9)]:

        if environment in environments_to_run:
            print(f"executing on {environment_to_string(environment)}")
            #try:
            optimizer.load_transfer_learning_history_per_env(file_list=file_list, training_data_per_env=training_data_per_env)
            experiment_direct_optimization_loop(optimizer=optimizer, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
            #except:
            #    pass
        else:
            print(f"NOT executing on {environment_to_string(environment)}")

    ssh.close()


'''
optimization algos:

bayesian_open_box
bayesian_syne_tune
random_search_syne_tune
hyperband
asha
grid search


open_box_transfer:

tlbo_rgpe_prf_all
tlbo_sgpr_prf_all
tlbo_topov3_prf_all
tlbo_rgpe_gp_all
tlbo_sgpr_gp_all
tlbo_topov3_gp_all

tlbo_rgpe_prf_exc
tlbo_sgpr_prf_exc
tlbo_topov3_prf_exc
tlbo_rgpe_gp_exc
tlbo_sgpr_gp_exc
tlbo_topov3_gp_exc


syne_tune_transfer:

zero_shot_all
quantile_all

zero_shot_exc
quantile_exc



'''
def run_algo_for_environment(ssh_host, environment, algo, config_space, metric, mode, loop_count,training_data_per_env):
    if algo == "bayesian_open_box":
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

        optimizer_bayesian_open_box = OpenBox_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying='bayesian')
        experiment_direct_optimization_loop(optimizer=optimizer_bayesian_open_box, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
        optimizer_bayesian_open_box.reset()

        ssh.close()

    elif algo == "bayesian_syne_tune":
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

        optimizer_bayesian_syne_tune = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying='bayesian')
        experiment_direct_optimization_loop(optimizer=optimizer_bayesian_syne_tune, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
        optimizer_bayesian_syne_tune.reset()

        ssh.close()

    elif algo == "random_search_syne_tune":
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

        optimizer_random_search_syne_tune = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying='random_search')
        experiment_direct_optimization_loop(optimizer=optimizer_random_search_syne_tune, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
        optimizer_random_search_syne_tune.reset()

        ssh.close()

    elif algo == "grid_search_syne_tune":
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

        optimizer_random_search_syne_tune = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying='grid_search')
        experiment_direct_optimization_loop(optimizer=optimizer_random_search_syne_tune, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
        optimizer_random_search_syne_tune.reset()

        ssh.close()

    elif algo == "asha_syne_tune":
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

        optimizer_random_search_syne_tune = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying='asha')
        experiment_direct_optimization_loop(optimizer=optimizer_random_search_syne_tune, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
        optimizer_random_search_syne_tune.reset()

        ssh.close()

    elif algo == "hyperband_syne_tune":
        ssh = SSHConnection(ssh_host, get_username_for_host(ssh_host))

        optimizer_random_search_syne_tune = Syne_Tune_Ask_Tell(config_space=config_space, metric=metric, mode=mode, underlying='hyperband')
        experiment_direct_optimization_loop(optimizer=optimizer_random_search_syne_tune, environment=environment, metric=metric, config_space=config_space, loop_count=loop_count, ssh=ssh)
        optimizer_random_search_syne_tune.reset()

        ssh.close()


    # open box transfer
    elif algo == "tlbo_rgpe_prf_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_sgpr_prf_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_topov3_prf_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_rgpe_gp_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_sgpr_gp_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_topov3_gp_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_rgpe_prf_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_sgpr_prf_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_topov3_prf_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_rgpe_gp_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_sgpr_gp_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "tlbo_topov3_gp_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "openbox", loop_count=loop_count, training_data_per_env=training_data_per_env)

    #syne tune transfer
    elif algo == "zero_shot_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "syne_tune", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "quantile_all":
        algo = algo.replace("_all","")
        transfer_all(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "syne_tune", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "zero_shot_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "syne_tune", loop_count=loop_count, training_data_per_env=training_data_per_env)

    elif algo == "quantile_exc":
        algo = algo.replace("_exc","")
        transfer_exc(ssh_host=ssh_host,environments_to_run=[environment],config_space=config_space, metric=metric, mode=mode,underlying=algo, library = "syne_tune", loop_count=loop_count, training_data_per_env=training_data_per_env)


def queue_function(queue, ssh_host):
    while not queue.empty():
        environment, algo, config_space, metric, mode, loop_count,training_data_per_env = queue.get()
        try:
            run_algo_for_environment(ssh_host=ssh_host, environment=environment, algo=algo, config_space=config_space, metric=metric, mode=mode, loop_count=loop_count, training_data_per_env=training_data_per_env)
        finally:
            queue.task_done()
            print(f"\n\n[{datetime.today().strftime('%H:%M:%S')}] Optimization Runs left in Queue : {queue.qsize()}\n\n")


def execute_optimization_runs_multi_threaded(ssh_hosts, environments_to_run, algorithms_to_run, config_space, metric, mode, loop_count,training_data_per_env=-1):

    queue = Queue()

    for environment in environments_to_run:
        for algo in algorithms_to_run:
            queue.put((environment, algo, config_space, metric, mode, loop_count,training_data_per_env))

    total_number_of_transfers = len(environments_to_run) * len(algorithms_to_run) * loop_count
    total_time_estimate = 0

    for env in environments_to_run:
        if env == environment_1:                        # average run time per transfer in seconds, calculated from random samples.
            total_time_estimate = total_time_estimate + ((41 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_2:
            total_time_estimate = total_time_estimate + ((66 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_3:
            total_time_estimate = total_time_estimate + ((118 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_4:
            total_time_estimate = total_time_estimate + ((40 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_5:
            total_time_estimate = total_time_estimate + ((500 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_6:
            total_time_estimate = total_time_estimate + ((42 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_7:
            total_time_estimate = total_time_estimate + ((285 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_8:
            total_time_estimate = total_time_estimate + ((41 + 15) * len(algorithms_to_run) * loop_count)
        if env == environment_9:
            total_time_estimate = total_time_estimate + ((466 + 15) * len(algorithms_to_run) * loop_count)

    print(total_time_estimate)

    total_time_estimate = total_time_estimate * 1.6 # overhead for boilerplate stuff, timeoutes, logging etc...
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
        time.sleep(2.5) # so no threads start at the same time, bc filenames depend on timestamp
        thread.start()
        threads.append(thread)

    # wait for threads to finish
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
