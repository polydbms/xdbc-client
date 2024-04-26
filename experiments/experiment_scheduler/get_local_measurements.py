import os

from main import concatenate_timings_files
from ssh_handler import create_ssh_connections
from configuration import hosts

ssh_connections = create_ssh_connections(hosts, 58)

concatenate_timings_files("measurements", ssh_connections)

for host, ssh in ssh_connections.items():
    print(f"Closing ssh connection to: {host}")
    ssh.close()
