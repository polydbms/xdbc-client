#!/bin/bash
#set -x

#$1 name filter

NAMEFILTER="-f name=$1"

TOTAL_BYTES=0
CID="$(docker ps $NAMEFILTER --format '{{.ID}}' --no-trunc)"
STATS=$(curl -s --unix-socket /var/run/docker.sock "http://localhost/v1.41/containers/$CID/stats?stream=false")
RX=$(echo $STATS | jq --raw-output '.networks.eth0.rx_bytes')
TX=$(echo $STATS | jq --raw-output '.networks.eth0.tx_bytes')

TOTAL_BYTES=$((TOTAL_BYTES + RX))

echo $TOTAL_BYTES
