#!/usr/bin/env bash
# create_loader_vm.sh - small VM for the multi-hour full download (Cloud Shell sessions time out).
# Delete it with scripts/delete_loader_vm.sh as soon as the full load finishes.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
gcloud compute instances create flights-loader \
    --zone="$ZONE" --machine-type=e2-small \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size=20GB --boot-disk-type=pd-standard \
    --scopes=storage-rw \
    --metadata=startup-script='apt-get update && apt-get install -y unzip tmux'
scripts/log_time.sh CREATE vm/flights-loader
cat << MSG
VM created. Wait about a minute for its startup script, then from this folder:
  gcloud compute scp --recurse --zone=$ZONE . flights-loader:~/pipeline
  gcloud compute ssh flights-loader --zone=$ZONE
On the VM:
  tmux new -s load
  cd ~/pipeline && scripts/run_stage1.sh full
Detach with Ctrl-b then d; reattach later with: tmux attach -t load
MSG
