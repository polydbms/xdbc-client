from experiment_envs import test_envs
from optimizer.config import loader
from optimizer.runner import run_xdbserver_and_xdbclient
import os
import copy

# run in top-level dir: `export PYTHONPATH=$(pwd):$PYTHONPATH`
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

for env in test_envs:
    env_specs = env['env']
    if env['active'] == 1:
        for table in env_specs['tables']:
            for compression in ['nocomp', 'zstd', 'snappy', 'lz4', 'lzo']:
                env_specs['table'] = table
                print(f"Running on env: {env['name']}")
                print(f"with specs: {env_specs}")

                config = copy.deepcopy(loader.default_config)

                # TODO: check
                if env_specs['src'] == 'parquet':
                    config = copy.deepcopy(loader.parquet_config)

                config['compression_lib'] = compression

                if env_specs['target'] == 'pandas' or config['compression_lib'] != 'nocomp':
                    config['format'] = 2

                t = run_xdbserver_and_xdbclient(config, env_specs, perf_dir)
                print(f"Run took {t}")

                if (env_specs['src'] == env_specs['target'] == 'csv'
                        or env_specs['src'] == env_specs['target'] == 'parquet'):
                    config['skip_ser'] = 1
                    print(f"and config: {config}")
                    t = run_xdbserver_and_xdbclient(config, env_specs, perf_dir)
                    print(f"Run took {t}")
