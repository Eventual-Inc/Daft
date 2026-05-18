#!/bin/bash
IP="$1"
KEY="$HOME/ray_bootstrap_key.pem"
SSH_OPTS="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -o LogLevel=ERROR"
ssh $SSH_OPTS "ec2-user@$IP" 'bash -s' <<'REMOTE'
set -e
HOSTIP=$(hostname -I | awk '{print $1}')
DEV=/dev/nvme2n1
if mountpoint -q /mnt2; then
  echo "$HOSTIP already-mounted $(df -h /mnt2 | tail -1 | awk '{print $2}')"
  exit 0
fi
if [ ! -b "$DEV" ]; then
  echo "$HOSTIP NO-DEVICE-$DEV"; exit 1
fi
sudo mkfs.xfs -f "$DEV" >/dev/null
sudo mkdir -p /mnt2
sudo mount "$DEV" /mnt2
sudo chown ec2-user:ec2-user /mnt2
mkdir -p /mnt2/spill /tmp/ray/spill
size=$(df -h /mnt2 | tail -1 | awk '{print $2}')
echo "$HOSTIP mounted $DEV size=$size"
REMOTE
