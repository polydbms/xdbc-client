import sys
from syne_tune.config_space import choice, ordinal, Integer, Float
from openbox import space as sp



DEFAULT_LOGGING = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '[%(asctime)s] [%(name)s] [%(levelname)s] : %(message)s',
            'datefmt': '%H:%M:%S' },
    },
    'handlers': {
        'console':  {'class': 'logging.StreamHandler',
                     'formatter': "standard",
                     'level': 'DEBUG',
                     'stream': sys.stdout}

    },
    'loggers': {
        __name__:   {'level': 'DEBUG',
                     'handlers': ['console'],
                     'propagate': False,
                     'force': True},
    }
}

#main environments :

#cloud-cloud
environment_1 = {
    "server_cpu": 16,
    "client_cpu": 16,
    "network": 1000,
    "timeout": 250
}

#cloud-fog
environment_2 = {
    "server_cpu": 16,
    "client_cpu": 8,
    "network": 100,
    "timeout": 300
}

#fog-edge
environment_3 = {
    "server_cpu": 8,
    "client_cpu": 1,
    "network": 50,
    "timeout": 400
}



# more environemts to sample from :

# internal transfer
environment_4 = {
    "server_cpu": 16,
    "client_cpu": 16,
    "network": 10000,
    "timeout": 250
}

# cloud endge, very slow connection
environment_5 = {
    "server_cpu": 16,
    "client_cpu": 4,
    "network": 10,
    "timeout": 1500
}

#fog fog
environment_6 = {
    "server_cpu": 8,
    "client_cpu": 8,
    "network": 500,
    "timeout": 300
}

# edge edge
environment_7 = {
    "server_cpu": 2,
    "client_cpu": 2,
    "network": 20,
    "timeout": 900
}

#cloud edge with fast connection
environment_8 = {
    "server_cpu": 16,
    "client_cpu": 1,
    "network": 1000,
    "timeout": 250
}


environment_9 = {
    "server_cpu": 16,
    "client_cpu": 1,
    "network": 10,
    "timeout": 1500
}


environments_list = [environment_1,environment_2,environment_3,environment_4,environment_5,environment_6,environment_7,environment_8,environment_9]

all_algorithms = [
"bayesian_open_box",
"bayesian_syne_tune",
"random_search_syne_tune",
"hyperband_syne_tune",
"asha_syne_tune",
"grid_search_syne_tune",
"tlbo_rgpe_prf_all",
"tlbo_sgpr_prf_all",
"tlbo_topov3_prf_all",
"tlbo_rgpe_gp_all",
"tlbo_sgpr_gp_all",
"tlbo_topov3_gp_all",
"tlbo_rgpe_prf_exc",
"tlbo_sgpr_prf_exc",
"tlbo_topov3_prf_exc",
"tlbo_rgpe_gp_exc",
"tlbo_sgpr_gp_exc",
"tlbo_topov3_gp_exc",
"zero_shot_all",
"quantile_all",
"zero_shot_exc",
"quantile_exc"
]


env_S16_C16_N1000 = environment_1
env_S16_C8_N100 = environment_2
env_S8_C1_N50 = environment_3
env_S16_C16_N10000 = environment_4
env_S16_C4_N10 = environment_5
env_S8_C8_N500 = environment_6
env_S2_C2_N20 = environment_7
env_S16_C1_N1000 = environment_8
env_S16_C1_N10 = environment_9

fixed_parameters = {
    "xdbc_version": 10,
    "run": 1,
    "client_readmode": 1,
    "network_latency": 0,
    "network_loss": 0,
    "table": "lineitem_sf10",
    'src': 'csv',
    'src_format': 1,
    'target': 'csv',
    'target_format': 1,
    'server_container': 'xdbcserver',
    'client_container': 'xdbcclient'
}

variable_parameter_baseline = {
    "compression_lib" : 'nocomp',
    "bufpool_size": 16,
    "buffer_size": 256,
    "send_par": 2,
    "write_par": 2,
    "decomp_par": 2,
    "read_partitions": 2,
    "read_par": 2,
    "deser_par": 2,
    "ser_par": 2,
    "comp_par": 2
}


'''
config_space_variable_parameters_gpyopt = \
    [{'name': "compression_lib", 'type': 'categorical', 'domain': (0, 1, 2, 3, 4)},
     {'name': "bufpool_size", 'type': 'discrete', 'domain': (8, 16, 24, 32)},
     {'name': "buff_size", 'type': 'discrete', 'domain': (64, 256, 512, 1024)},
     {'name': "send_par", 'type': 'discrete', 'domain': (1, 2, 4)},
     {'name': "client_write_par", 'type': 'discrete', 'domain': (1, 2, 4)},
     {'name': "client_decomp_par", 'type': 'discrete', 'domain': (1, 2, 4)},
     {'name': "server_read_partitions", 'type': 'discrete', 'domain': (1, 2, 4)},
     {'name': "server_read_par", 'type': 'discrete', 'domain': (1, 2, 4)},
     {'name': "server_deser_par", 'type': 'discrete', 'domain': (1, 2, 4)},
     {'name': "server_comp_par", 'type': 'discrete', 'domain': (1, 2, 4)},
     ]
'''


'''
config_space_variable_parameters_skopt = \
    [Categorical(["nocomp", "zstd", "lz4", "lzo", "snappy"], name="compression_lib"),
     Integer(8, 32, name="bufpool_size"),
     Integer(64, 1024, name="buff_size"),
     Integer(1, 4, name="send_par"),
     Integer(1, 4, name="client_write_par"),
     Integer(1, 4, name="client_decomp_par"),
     Integer(1, 4, name="server_read_partitions"),
     Integer(1, 4, name="server_read_par"),
     Integer(1, 4, name="server_deser_par"),
     Integer(1, 4, name="server_comp_par"),
     ]
'''



config_space_variable_parameters_generalized_test_search_space = \
    [{'name': "compression_lib", 'type': 'categorical', 'domain': ["nocomp", "zstd", "lz4", "lzo", "snappy"]},
     {'name': "bufpool_size", 'type': 'discrete', 'domain': [32, 64, 96, 128]},
     {'name': "buffer_size", 'type': 'discrete', 'domain': [64, 256, 512, 1024]},
     {'name': "send_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "write_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "decomp_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "read_partitions", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "read_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "deser_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "ser_par", 'type': 'discrete', 'domain': [1]},
     {'name': "comp_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     ]

# ?? search space size
config_space_variable_parameters_generalized_175k = \
    [{'name': "compression_lib", 'type': 'categorical', 'domain': ["nocomp", "zstd", "lz4", "lzo", "snappy"]},
     {'name': "bufpool_size", 'type': 'discrete', 'domain': [32,64,96,128]},
     {'name': "buffer_size", 'type': 'discrete', 'domain': [64, 256, 512, 1024]},
     {'name': "send_par", 'type': 'discrete', 'domain': [1, 2, 4]},
     {'name': "write_par", 'type': 'discrete', 'domain': [1, 2, 4]},
     {'name': "decomp_par", 'type': 'discrete', 'domain': [1, 2, 4]},
     {'name': "read_partitions", 'type': 'discrete', 'domain': [1, 2, 4]},
     {'name': "read_par", 'type': 'discrete', 'domain': [1, 2, 4]},
     {'name': "deser_par", 'type': 'discrete', 'domain': [1, 2, 4]},
     {'name': "ser_par", 'type': 'discrete', 'domain': [1]},    # this parameter currently has no influence on the perfoamnce, so including for completness.
     {'name': "comp_par", 'type': 'discrete', 'domain': [1, 2, 4]},
     ]

# ?? search space size
config_space_variable_parameters_generalized_1310k = \
    [{'name': "compression_lib", 'type': 'categorical', 'domain': ["nocomp", "zstd", "lz4", "lzo", "snappy"]},
     {'name': "bufpool_size", 'type': 'discrete', 'domain': [32, 64, 96, 128]},
     {'name': "buffer_size", 'type': 'discrete', 'domain': [64, 256, 512, 1024]},
     {'name': "send_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "write_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "decomp_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "read_partitions", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "read_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "deser_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     {'name': "ser_par", 'type': 'discrete', 'domain': [1]},    # this parameter currently has no influence on the perfoamnce, so including for completness.
     {'name': "comp_par", 'type': 'discrete', 'domain': [1, 2, 4, 8]},
     ]

config_space_variable_parameters_generalized_6250k = \
    [{'name': "compression_lib", 'type': 'categorical', 'domain': ["nocomp", "zstd", "lz4", "lzo", "snappy"]},
     {'name': "bufpool_size", 'type': 'discrete', 'domain': [32, 64, 96, 128]},
     {'name': "buffer_size", 'type': 'discrete', 'domain': [64, 256, 512, 1024]},
     {'name': "send_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     {'name': "write_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     {'name': "decomp_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     {'name': "read_partitions", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     {'name': "read_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     {'name': "deser_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     {'name': "ser_par", 'type': 'discrete', 'domain': [1]},    # this parameter currently has no influence on the perfoamnce, so including for completness.
     {'name': "comp_par", 'type': 'discrete', 'domain': [1, 2, 4, 8, 16]},
     ]

config_space_variable_parameters_generalized_96636M = \
    [{'name': "compression_lib", 'type': 'categorical', 'domain': ["nocomp", "zstd", "lz4", "lzo", "snappy"]},
     {'name': "bufpool_size", 'type': 'discrete', 'domain': [32, 48, 64, 80, 96, 112, 128, 144, 160]},
     {'name': "buffer_size", 'type': 'discrete', 'domain': [64, 128, 256, 384, 512, 768, 1024, 1280]},
     {'name': "send_par", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     {'name': "write_par", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     {'name': "decomp_par", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     {'name': "read_partitions", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     {'name': "read_par", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     {'name': "deser_par", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     {'name': "ser_par", 'type': 'discrete', 'domain': [1]},    # this parameter currently has no influence on the perfoamnce, so including for completness.
     {'name': "comp_par", 'type': 'discrete', 'domain': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
     ]

username_cloud_7 = "bene"
all_hosts_cloud_7 = ["cloud-7.dima.tu-berlin.de",
                     "cloud-8.dima.tu-berlin.de",
                     "cloud-9.dima.tu-berlin.de",
                     "cloud-10.dima.tu-berlin.de"]

username_cloud_11 = "bdidrich-ldap"
all_hosts_cloud_11 = ["cloud-11.dima.tu-berlin.de",
                      "cloud-12.dima.tu-berlin.de",
                      #"cloud-13.dima.tu-berlin.de",
                      "cloud-14.dima.tu-berlin.de",
                      "cloud-15.dima.tu-berlin.de",
                      #"cloud-16.dima.tu-berlin.de",
                      #"cloud-17.dima.tu-berlin.de",
                      #"cloud-18.dima.tu-berlin.de",
                      "cloud-19.dima.tu-berlin.de",
                      "cloud-20.dima.tu-berlin.de",
                      "cloud-21.dima.tu-berlin.de",
                      "cloud-22.dima.tu-berlin.de",
                      "cloud-23.dima.tu-berlin.de",
                      "cloud-24.dima.tu-berlin.de",
                      "cloud-25.dima.tu-berlin.de",
                      "cloud-26.dima.tu-berlin.de",
                     # "cloud-27.dima.tu-berlin.de",
                      "cloud-28.dima.tu-berlin.de",
                     # "cloud-29.dima.tu-berlin.de",
                    # cloud 30 exclusive for nils schubert
                      "cloud-31.dima.tu-berlin.de"]#,
                      #"cloud-32.dima.tu-berlin.de",
                     # "cloud-33.dima.tu-berlin.de",
                      #"cloud-34.dima.tu-berlin.de",
                      #"cloud-35.dima.tu-berlin.de"] #23 nodes

def get_username_for_host(ssh_host):
    if ssh_host in all_hosts_cloud_7:
        return username_cloud_7
    if ssh_host in all_hosts_cloud_11:
        return username_cloud_11
    else:
        return "unknown ssh host"


def create_complete_config(environment, metric, library="syne_tune",config=None):
    """
    Takes a configuration space and adds all other needed parameters for a data transfer.
    The optimization algorithms only know and supply the variable parameters, so all fixed parameters need to be added
    for it to be a complete configuration

    Current supported libraries:
        - our generalized format
        - aws syne-tune
        - openbox
        - gpyopt

    Parameters:
        environment (list[dicts]): The generalized configuration space to convert.
        metric (str): the metric to be optimized.
        library (str): The name of the library to convert the configuration space to.
        config (str): The configuration to be completed.

    Returns:
        : Library-specific complete configuration.
    """
    if library == "syne_tune" or library == "syne-tune" or library == "synetune":

        config_space = convert_from_general(config_space=config_space_variable_parameters_generalized_175k,
                                            library='syne_tune')

        # add fixed parameters to the config space
        for key, value in fixed_parameters.items():
            config_space[key] = choice([value])

        # add environment parameters to the config space
        for key, value in environment.items():
            config_space[key] = choice([value])

        # add the metric to return as a parameter
        config_space['metric'] = choice([metric])
        #config_space['timeout'] = choice([240])

        return config_space

    '''
    if library == "skopt" or library == "scikit_optimize":
        return config_space

    if library == "gpyopt" or library == "gpy_opt":

        # add fixed parameters to the config space
        for key, value in fixed_parameters.items():
            config_space_variable_parameters_gpyopt.append({'name': key, 'type': 'discrete', 'domain': (value)})

        # add environment parameters to the config space
        for key, value in environment.items():
            config_space[key] = choice([value])

        # add the metric to return as a parameter
        config_space['metric'] = choice([metric])
        #config_space['timeout'] = choice([240])

        return config_space
    '''

    if library == "openbox" or library == "open_box":

        config_space = convert_from_general(config_space=config_space_variable_parameters_generalized_175k, library='open_box')

        for key, value in fixed_parameters.items():
            config_space.add_variable(sp.Categorical(key, [value]))

        # add environment parameters to the config space
        for key, value in environment.items():
            config_space.add_variable(sp.Categorical(key, [value]))

        config_space.add_variable(sp.Categorical('metric', [metric]))

        return config_space

    if library == "dict_add" or library == 'dict':

        config_copy = dict(config)

        for key, value in fixed_parameters.items():
            config_copy[key] = value

        # add environment parameters to the config space
        for key, value in environment.items():
            config_copy[key] = value

        config_copy['metric'] = metric


        #add any missing variable parameters :
        for key, value in variable_parameter_baseline.items():
            if config_copy[key] is None:
                config_copy[key] = value

        return config_copy


def convert_from_general(config_space, library):
    """
    Converts a config_space from a generalized format to the proprietary configuration spaces needed for different libraries.
    Current supported libraries:
        - aws syne-tune
        - openbox

    Parameters:
        config_space (list[dicts]): The generalized configuration space to convert.
        library (str): The name of the library to convert the configuration space to.

    Returns:
        : Library-specific configuration space.
    """
    library_config_space = None

    if library == 'syne_tune':
        library_config_space = {}
        for elem in config_space:
            if elem['type'] == 'categorical':
                library_config_space[elem['name']] = choice(elem['domain'])
            if elem['type'] == 'discrete':
                library_config_space[elem['name']] = ordinal(elem['domain'])
            if elem['type'] == 'integer':
                library_config_space[elem['name']] = Integer(elem['lower'], elem['upper'])
            if elem['type'] == 'real':
                library_config_space[elem['name']] = Float(elem['lower'], elem['upper'])

    if library == 'open_box':
        library_config_space = sp.Space()
        for elem in config_space:
            if elem['type'] == 'categorical':
                library_config_space.add_variable(sp.Categorical(elem['name'], elem['domain']))
            if elem['type'] == 'discrete':
                library_config_space.add_variable(sp.Ordinal(elem['name'], elem['domain']))
            if elem['type'] == 'integer':
                library_config_space.add_variable(sp.Int(elem['name'], elem['lower'], elem['upper']))
            if elem['type'] == 'real':
                library_config_space.add_variable(sp.Real(elem['name'], elem['lower'], elem['upper']))

    return library_config_space


def get_config_space_string(config_space):
    config_space_string = ""
    if config_space == config_space_variable_parameters_generalized_1310k:
        config_space_string = "1310k"
    elif config_space == config_space_variable_parameters_generalized_175k:
        config_space_string = "175k"
    elif config_space == config_space_variable_parameters_generalized_6250k:
        config_space_string = "6250k"
    elif config_space == config_space_variable_parameters_generalized_96636M:
        config_space_string = "96636M"
    elif config_space == config_space_variable_parameters_generalized_test_search_space:
        config_space_string = "test_search_space"
    return  config_space_string

def environment_to_string(environment):
    return f"S{environment['server_cpu']}_C{environment['client_cpu']}_N{environment['network']}"

def environment_values_to_string(server_cpu,client_cpu,network):
    return f"S{int(round(server_cpu,0))}_C{int(round(client_cpu,0))}_N{int(round(network,0))}"

def unparse_environment(env_string):
    try:
        parts = env_string.split('_')
        server_cpu = int(parts[0][1:])
        client_cpu = int(parts[1][1:])
        network = int(parts[2][1:])
        return {
            "server_cpu": server_cpu,
            "client_cpu": client_cpu,
            "network": network
        }
    except (IndexError, ValueError):
        raise ValueError("Invalid input format. Expected format: 'SXX_CXX_NXXX'.")
