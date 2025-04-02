import subprocess
import os
import csv


def set_env(env):
    subprocess.run(['docker', 'update', '--cpus', f'{env["server_cpu"]}', env['server_container']])
    subprocess.run(['docker', 'update', '--cpus', f'{env["client_cpu"]}', env['client_container']])

    subprocess.run(["curl", "-X", "DELETE", f"localhost:4080/{env['client_container']}"])
    subprocess.run(["curl", "-X", "PUT", f"localhost:4080/{env['client_container']}"])

    if env['network'] != 0:
        subprocess.run(["curl", "-s", "-d", f"rate={env['network']}mbps", f"localhost:4080/{env['client_container']}"])


def create_file(csv_file_path):
    if not os.path.exists(csv_file_path):
        with open(csv_file_path, mode="w", newline="") as file:
            writer = csv.writer(file)

            writer.writerow(["timestamp", "env", "repetition", "system", "table", "time"])


def create_conf(
        read_par=1,
        deser_par=1,
        comp_par=1,
        send_par=1,
        rcv_par=1,
        decomp_par=1,
        ser_par=1,
        write_par=1,
        buffer_size=1024,
        server_buffpool_size=1,
        client_buffpool_size=1,
        format=2,
        compression_lib='nocomp',
        skip_ser=0,
        skip_deser=0,
):
    return {
        'read_par': read_par,
        'deser_par': deser_par,
        'comp_par': comp_par,
        'send_par': send_par,
        'rcv_par': rcv_par,
        'decomp_par': decomp_par,
        'ser_par': ser_par,
        'write_par': write_par,
        'buffer_size': buffer_size,
        'server_buffpool_size': server_buffpool_size,
        'client_buffpool_size': client_buffpool_size,
        'format': format,
        'compression_lib': compression_lib,
        'skip_ser': skip_ser,
        'skip_deser': skip_deser,
    }
