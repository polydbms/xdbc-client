import paramiko
import time
import socket


class SSHExecutionError(Exception):
    """Raised when there's an error executing a command over SSH."""
    pass


def establish_ssh_connection(hostname, username=None, password=None, private_key_path=None):
    """
    Establish an SSH connection to a remote host.

    Args:
        hostname (str): The hostname or IP address of the remote host.
        username (str): The SSH username.
        password (str, optional): The SSH password. Use key-based authentication if not provided.
        private_key_path (str, optional): The path to the private key file for key-based authentication.

    Returns:
        paramiko.SSHClient: An established SSH connection.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        if password:
            ssh.connect(hostname, username=username, password=password)
        else:
            ssh.connect(hostname, username=username, key_filename=private_key_path)

        return ssh
    except Exception as e:
        print(f"Failed to establish SSH connection: {str(e)}")
        return None


def close_ssh_connection(ssh):
    """
    Close an SSH connection.

    Args:
        ssh (paramiko.SSHClient): The SSH connection to close.
    """
    if ssh:
        ssh.close()


def execute_ssh_cmd(ssh, cmd, background=False):
    if not ssh.get_transport() or not ssh.get_transport().is_active():
        raise SSHExecutionError(f"SSH connection is not active for cmd: {cmd}")
    if background:
        background_command = f"nohup {cmd} &"
        ssh.exec_command(background_command)
        return None
    else:
        stdin, stdout, stderr = ssh.exec_command(cmd)

        error_output = stderr.read().decode().strip()
        if error_output:
            ip_address = ssh.get_transport().getpeername()[0]
            try:
                hostname = socket.gethostbyaddr(ip_address)[0]
            except socket.herror:
                hostname = ip_address
            raise SSHExecutionError(f"error on host: {hostname}, cmd: {cmd}, error: {error_output}")
    return stdout.read().decode().strip()


def create_ssh_connections(hosts):
    """
    Create SSH connections for a list of hosts without username/password.

    Args:
        hosts (list): A list of hostnames or IP addresses.

    Returns:
        dict: A dictionary with hostnames as keys and SSH connections as values.
    """
    ssh_connections = {}

    for host in hosts:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(host, username='harry-ldap')
            ssh_connections[host] = ssh
        except Exception as e:
            print(f"Failed to establish SSH connection to {host}: {str(e)}")

    return ssh_connections
