#!/bin/bash

sudo_password="$1"

echo "$sudo_password" | sudo -S bash -c 'echo "RemoveIPC=no" >> /etc/systemd/logind.conf'
echo "$sudo_password" | sudo -S systemctl restart systemd-logind.service
