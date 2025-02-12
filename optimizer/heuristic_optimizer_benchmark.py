import os
from datetime import datetime

from experiments.experiment_scheduler.ssh_handler import SSHConnection
from optimizer import runner_ssh
from optimizer.config import loader
from optimizer.optimize import optimize
from optimizer.test_envs import test_envs


def test(environment_name):

    #environment_name = 'environment_1'
    
    
    ssh = SSHConnection("cloud-7.dima.tu-berlin.de", "bene")


    #perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

    perf_dir = "C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements_new"

    mode = 2
    sleep = 2


    start = datetime.now()

    envs = extract_unique_envs(test_envs)

    best_config = loader.default_config

    for full_env in test_envs:
        print(full_env['name'])

        if full_env['name'] == environment_name:
            for compression in ['nocomp', 'zstd', 'snappy', 'lz4', 'lzo']:
                best_config['compression_lib'] = compression
                if compression != 'nocomp' or full_env['env']['src_format'] == 2 or full_env['env']['target_format'] == 2:
                    best_config['format'] = 2

                if full_env['env']['table'] == 'iotm':
                    best_config['buffer_size'] = 256
                    best_config['server_buffpool_size'] = best_config['deser_par'] * 30000
                    best_config['client_buffpool_size'] = best_config['buffer_size'] * 3 * 10
                if full_env['env']['target'] == 'postgres':
                    best_config['server_buffpool_size'] = 120000
                    best_config['format'] = 1
                print("----------------------------------------")
                print("Run on env:")
                print(full_env['env'])
                print("With config:")
                print(best_config)

                t = runner_ssh.run_xdbserver_and_xdbclient(config=best_config, env=full_env['env'], mode=mode, perf_dir=perf_dir, ssh=ssh, sleep=sleep,
                                                           show_output=(False, False) )

                print(f"Run took: {t}s")

    runtime, best_config, estimated_thr, opt_time = optimize(environment_name, 'xdbc', 'heuristic',ssh=ssh)



    end = datetime.now()

    print(best_config)
    print("achieved runtime : " + str(runtime))
    print(opt_time)

    print(f"total optimization time took {(end - start).total_seconds()} seconds")
    ssh.close()
    return runtime, ((end - start).total_seconds()), best_config





def extract_unique_envs(test_envs):
    unique_envs = {}

    for env in test_envs:

        env_data = env['env']
        unique_env_key = (env_data['server_cpu'], env_data['client_cpu'], env_data['network'])

        src_target_pair = (env_data['src'], env_data['target'])

        if unique_env_key not in unique_envs:
            unique_envs[unique_env_key] = set()  # Use a set to avoid duplicate src/target pairs

        unique_envs[unique_env_key].add(src_target_pair)

    unique_envs = {env: list(pairs) for env, pairs in unique_envs.items()}
    return unique_envs


if __name__ == '__main__':
    for environment_name in ["environment_1", "environment_2", "environment_3", "environment_4", "environment_5", "environment_6", "environment_7", "environment_8", "environment_9"]:
        result_time, opt_time, best_config = test(environment_name)
        with open(f"C:/Users/bened/Desktop/Uni/repos/xdbc-client/experiments/model_optimizer/results_heuristic.csv",'a') as file:
            file.write("\n")
            file.write(f"{environment_name},{result_time},{opt_time},{best_config}")
