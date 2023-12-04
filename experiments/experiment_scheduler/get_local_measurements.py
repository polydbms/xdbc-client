import os

from main import concatenate_timings_files
from ssh_handler import create_ssh_connections, execute_ssh_cmd
from configuration import hosts

ssh_connections = create_ssh_connections(hosts)

current_directory = os.getcwd()
for host, ssh in ssh_connections.items():
    execute_ssh_cmd(ssh,
                    f"docker cp xdbcserver:/tmp/xdbc_server_timings.csv {current_directory}/measurements/{host}_server_timings.csv")

    execute_ssh_cmd(ssh,
                    f"docker cp xdbcclient:/tmp/xdbc_client_timings.csv {current_directory}/measurements/{host}_client_timings.csv")

concatenate_timings_files("measurements")
