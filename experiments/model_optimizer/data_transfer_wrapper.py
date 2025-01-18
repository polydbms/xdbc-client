import datetime
import threading

import pandas as pd
from experiments.experiment_scheduler.ssh_handler import SSHConnection
from Metrics import add_all_metrics_to_result
from datetime import datetime
from func_timeout import func_timeout, FunctionTimedOut

from experiments.model_optimizer.Configs import get_username_for_host
from optimizer import runner_ssh


def transfer(config,i=0,max_retries=1, ssh=None):
    """
    Description of Funtion. #todo

    Parameters:
        param_1 (datatype): description.
        param_2 (datatype): description.

    Returns:
        datatype: descritpion.
    """

    #remove_rows_with_string("C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements/xdbc_client_timings_bene.csv",'transfer_id')
    #remove_rows_with_string("C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements/xdbc_server_timings_bene.csv",'transfer_id')


    if config['timeout'] is None:
        config['timeout'] = 500

    # the extremely long runs are only when nocomp, so all others can get a smaller timeout to save some time
    # todo change back if too many timeouts are observed
    if config['timeout'] > 1000 and config["compression_lib"] is not "nocomp":
        config['timeout'] = config['timeout'] / 2

    result = train_method(config, ssh)

    retries = 0
    #max_retries = 1

    while not result and retries < max_retries:
        print(
            f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] transfer #{i} failed for {retries + 1} times, retrying for {retries + 1} time")
        result = train_method(config, ssh)
        retries = retries + 1

    if not result:
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] transfer #{i} failed for {retries + 1} times, reporting dummy result")

        try:
            save_failed_config(config)
        except:
            pass
        '''
        dummy_result = {
            "date": -1,
            "time": config['timeout'],
            "size": -1,
            "avg_cpu_server": -1,
            "avg_cpu_client": -1
        }
        '''
        dummy_result = {
            "transfer_id": -1,
            "total_time_x": -1,
            "rcv_wait_time": -1,
            "rcv_proc_time": -1,
            "rcv_throughput": -1,
            "rcv_throughput_pb": -1,
            "rcv_load": -1,
            "decomp_wait_time": -1,
            "decomp_proc_time": -1,
            "decomp_throughput": -1,
            "decomp_throughput_pb": -1,
            "decomp_load": -1,
            "write_wait_time": -1,
            "write_proc_time": -1,
            "write_throughput": -1,
            "write_throughput_pb": -1,
            "write_load": -1,
            "total_time_y": -1,
            "read_wait_time": -1,
            "read_proc_time": -1,
            "read_throughput": -1,
            "read_throughput_pb": -1,
            "read_load": -1,
            "deser_wait_time": -1,
            "deser_proc_time": -1,
            "deser_throughput": -1,
            "deser_throughput_pb": -1,
            "deser_load": -1,
            "comp_wait_time": -1,
            "comp_proc_time": -1,
            "comp_throughput": -1,
            "comp_throughput_pb": -1,
            "comp_load": -1,
            "send_wait_time": -1,
            "send_proc_time": -1,
            "send_throughput": -1,
            "send_throughput_pb": -1,
            "send_load": -1,
            "date": -1,
            "xdbc_version": -1,
            "host": -1,
            "run": -1,
            "source_system": -1,
            "target_system": -1,
            "table": config["table"],
            "compression": config["compression_lib"],
            "format": -1,
            "send_par": config["send_par"],
            "rcv_par": -1,
            "server_bufferpool_size": -1,
            "client_bufferpool_size": -1,
            "buffer_size": config["buffer_size"],
            "network": config["network"],
            "network_latency": -1,
            "network_loss": -1,
            "client_readmode": -1,
            "client_cpu": config["client_cpu"],
            "write_par": config["write_par"],
            "decomp_par": config["decomp_par"],
            "server_cpu": config["server_cpu"],
            "read_par": config["read_par"],
            "read_partitions": config["read_partitions"],
            "deser_par": config["deser_par"],
            "comp_par": config["comp_par"],
            "time": -1,
            "datasize": -1,
            "avg_cpu_server": -1,
            "avg_cpu_client": -1,
            "timestamp_end": -1,
            "throughput": -1,
            "average_throughput": -1,
            "resource_utilization": -1,
            "wait_to_proc_time_ratio": -1,
            "even_load_distribution_mse": -1
        }
        #ssh.close()
        return dummy_result
    else:
        #ssh.close()
        return result


def save_failed_config(config):
    """
    Description of Funtion. #todo

    Parameters:
        param_1 (datatype): description.
        param_2 (datatype): description.

    Returns:
        datatype: descritpion.
    """
    filename = f"failed_configs.csv"

    df = pd.DataFrame(config, index=[0])

    df.to_csv(filename, mode='a', header=False)

def train_method(config,ssh=None):
    return train_method_seperate_params(xdbc_version=config['xdbc_version'], run= config['run'],client_readmode= config['client_readmode'], client_cpu=config['client_cpu'],server_cpu= config['server_cpu'],
                                        network=config['network'],network_latency= config['network_latency'],network_loss= config['network_loss'],

                                        #config['system'], config['format'],
                                        src=config['src'], src_format=config['src_format'], target=config['target'], target_format=config['target_format'],
                                        server_container= config['server_container'], client_container= config['client_container'],
    table=config['table'],
    compression_lib=config['compression_lib'], bufpool_size=config['bufpool_size'], buff_size=config['buffer_size'], send_par=config['send_par'], client_write_par=config['write_par'],
    client_decomp_par=config['decomp_par'], server_read_partitions=config['read_partitions'], server_read_par=config['read_par'], server_deser_par=config['deser_par'], client_ser_par=config['ser_par'],
    server_comp_par=config['comp_par'],
    metric=config['metric'], timeout=config['timeout'],ssh=ssh)


def train_method_seperate_params(xdbc_version: int, run: int, client_readmode: int, client_cpu: int, server_cpu: int,
                          network: int, network_latency: int, network_loss: int,
                                 #system: str,  format: int,
                                 table: str, src:str, src_format:int, target:str, target_format:int,server_container:str,client_container:str,

                                 compression_lib: str, bufpool_size: int, buff_size: int, send_par: int,
                          client_write_par: int, client_decomp_par: int, server_read_partitions: int,
                          server_read_par: int, server_deser_par: int, client_ser_par: int, server_comp_par: int,

                          metric = 'time', timeout = 240,ssh=None):
    """
    Description of Funtion. #todo

    Parameters:
        param_1 (datatype): description.
        param_2 (datatype): description.

    Returns:
        datatype: descritpion.
    """

    config = {
        # fixded / enviroment params
        "xdbc_version": xdbc_version,
        "run": run,
        "client_readmode": client_readmode,
        "client_cpu": client_cpu,
        "server_cpu": server_cpu,
        "network": network,
        "network_latency": network_latency,
        "network_loss": network_loss,
        "table": table,
        'src': src,
        'src_format': src_format,
        'target': target,
        'target_format': target_format,
        'server_container': server_container,
        'client_container': client_container,

        # varying params
        "compression_lib": compression_lib,

        "server_buffpool_size": int(bufpool_size * buff_size),
        "client_buffpool_size": int(bufpool_size * buff_size),

        "buffer_size": buff_size,

        "send_par":send_par,
        "rcv_par":send_par,

        "write_par":client_write_par,
        "decomp_par":client_decomp_par,
        "server_read_partitions":server_read_partitions,
        "read_par":server_read_par,
        "deser_par":server_deser_par,
        #"ser_par":client_ser_par,
        "comp_par":server_comp_par,
    }


    if ssh is None:
        ssh = SSHConnection("cloud-7.dima.tu-berlin.de", get_username_for_host("cloud-7.dima.tu-berlin.de"))


    # if file is not in dev/shm, copy it there
    ssh.execute_cmd(f"cp -R -u -p /home/{get_username_for_host(ssh.hostname)}/datasets/lineitem_sf10.csv /dev/shm/lineitem_sf10.csv")

    ssh.execute_cmd("docker compose -f xdbc-client/docker-xdbc.yml down")
    ssh.execute_cmd("docker compose -f xdbc-client/docker-xdbc.yml up -d")
    ssh.execute_cmd("docker compose -f xdbc-client/docker-tc.yml up -d")
    ssh.execute_cmd("docker exec xdbcclient bash -c \"ldconfig\"")
    ssh.execute_cmd('docker exec xdbcserver bash -c "[ ! -f /tmp/xdbc_server_timings.csv ] && echo \'transfer_id,total_time,read_wait_time,read_time,deser_wait_time,deser_time,compression_wait_time,compression_time,network_wait_time,network_time\' > /tmp/xdbc_server_timings.csv"')
    ssh.execute_cmd('docker exec xdbcclient bash -c "[ ! -f /tmp/xdbc_client_timings.csv ] && echo \'transfer_id,total_time,rcv_wait_time,rcv_time,decomp_wait_time,decomp_time,write_wait_time,write_time\' > /tmp/xdbc_client_timings.csv"')


    try:
                                                                                             #(config, env, mode, perf_dir, ssh=None, return_transfer_id=False, sleep=2, show_output=(False, False),include_setup=True):
        result = func_timeout(timeout=timeout, func=runner_ssh.run_xdbserver_and_xdbclient, args=[config, config, config['client_readmode'], "C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements", ssh, True,2,(False,False),False])

        complete_result = load_complete_result(result)
        return complete_result


    except FunctionTimedOut:
        print(f"[{datetime.today().strftime('%H:%M:%S')}] [{ssh.hostname}] transfer  could not be completet within {timeout} seconds and was terminated")
        ssh.execute_cmd("docker compose -f xdbc-client/docker-xdbc.yml down")
        return {}

    return result


def load_complete_result(result):
    """
    Description of Funtion. #todo

    Parameters:
        param_1 (datatype): description.
        param_2 (datatype): description.

    Returns:
        datatype: descritpion.
    """
    file_lock = threading.Lock()

    with file_lock:

        client_timings = pd.read_csv("C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements/xdbc_client_timings_bene.csv")
        server_timings = pd.read_csv("C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements/xdbc_server_timings_bene.csv")
        general_stats = pd.read_csv("C:/Users/bened/Desktop/Uni/repos/xdbc-client/optimizer/local_measurements/xdbc_general_stats_bene.csv")

        df_both_timings = pd.merge(client_timings,server_timings,on='transfer_id')

        df_complete = pd.merge(df_both_timings,general_stats,left_on="transfer_id",right_on="date")

        df_result = df_complete[(df_complete.transfer_id == result['transfer_id'])]

        df_result['timestamp_end'] = str(datetime.now())

        #print(df_complete['transfer_id'].dtype)
        #print(type(result['transfer_id'])) s
        #print(result['transfer_id'] in df_complete['transfer_id'].values)

        df_result['transfer_id'] = df_result['transfer_id'].astype(str)
        result['transfer_id'] = str(result['transfer_id'])


        result = df_result.iloc[0].to_dict()

        result = add_all_metrics_to_result(result)

        return result