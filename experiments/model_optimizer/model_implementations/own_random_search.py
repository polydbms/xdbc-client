import random


class Own_Random_Search():

    available_optimization_algorithms = ["own_random_search"]

    available_algorithms = available_optimization_algorithms

    def __init__(self, config_space, metric='time', mode='min', underlying='own_random_search'):
        self.config_space = config_space
        self.metric = metric
        self.mode = mode
        self.underlying = underlying + "_bene"

    def reset(self):
        pass

    def suggest(self):
        random_config = {}

        for param in self.config_space:

            name = param['name']
            param_type = param['type']
            domain = param['domain']

            if param_type == 'categorical' or param_type == 'discrete':
                random_config[name] = random.choice(domain)
            elif param_type == 'integer':
                random_config[name] = random.randint(domain[0], domain[1])
            elif param_type == 'float':
                random_config[name] = random.uniform(domain[0], domain[1])
            else:
                raise ValueError(f"Unsupported parameter type: {param_type}")

        return random_config

    def report(self, config, result):
       pass