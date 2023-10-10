#!/bin/bash
#set -x
#pip install parallel-ssh
#parallel-ssh -h hostsfile.txt -l harry-ldap -P -i 'bash xdbc/xdbc-client/experiments/prepare.sh' > parallel-ssh-output.log 2>&1
#cat hostsfile.txt | parallel -j 0 ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} 'bash xdbc/xdbc-client/experiments/prepare.sh passwd' > parallel-output.log 2>&1

#alternatively
#for host in sr630-wn-a-06.dima.tu-berlin.de; do ssh harry-ldap@$host 'bash xdbc/xdbc-client/experiments/prepare.sh'; done
#parallel --nonall --sshloginfile hostsfile.txt --tag -- 'bash xdbc/xdbc-client/experiments/prepare.sh' > parallel-output.log 2>&1
#cat measurements/1695682802_runs_comp.csv | column -t -s, | less -S

if [ $# -ne 1 ]; then
  echo "Usage: $0 <sudo_password>"
  exit 1
fi

# Store the password argument in a variable
sudo_password="$1"

echo "$sudo_password" | sudo -S apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 42D5A192B819C5DA

sudo pip3 install --upgrade pyOpenSSL

pip3 install paramiko

for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do sudo apt-get remove $pkg; done
sudo apt-get update
yes | sudo apt-get install ca-certificates curl gnupg jq
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add the repository to Apt sources:
echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" |
  sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
yes | sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin docker-compose

# Check if daemon.json exists
if [ ! -f /etc/docker/daemon.json ]; then

  echo '{
      "data-root": "/local-ssd/docker"
    }' | sudo tee /etc/docker/daemon.json >/dev/null

  sudo service docker stop
  sudo rsync -aP /var/lib/docker/ /local-ssd/docker

  sudo mv /var/lib/docker /var/lib/docker.old

  sudo service docker start

  docker --version

  # Check if Docker started successfully
  if [ $? -eq 0 ]; then
    sudo rm -rf /var/lib/docker.old
  else
    echo "Docker setup failed. Please check and resolve any issues."
  fi
else
  echo "Docker is already configured. Skipping setup."
fi

cd ~/xdbc/xdbc-client && make &
cd ~/xdbc/xdbc-server && make &

(cd ~/clickhouse-jdbc-bridge/ && docker build -t my/clickhouse-all-in-one -f all-in-one.Dockerfile .) &
(cd ~/polydb/docker/postgres && make) &
(cd ~/polydb/docker/tpch_generator && make) &
wait
docker run --rm -v test-data:/data --name tpch-generator tpch-generator

cd ~/xdbc/xdbc-client/ && docker-compose -f docker-tc.yml up -d
cd ~/xdbc/xdbc-client/ && docker-compose -f docker-xdbc.yml up -d

docker cp ~/test_10000000.csv xdbcserver:/tmp/ && docker exec xdbcserver bash -c "mv /tmp/test_10000000.csv /dev/shm"

docker exec xdbcclient bash -c "yes | apt-get install plocate && updatedb"
