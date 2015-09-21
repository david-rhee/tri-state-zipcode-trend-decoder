#!/bin/bash

IP_ADDR=$1
OPTION=$2
NUM_SPAWNS=$3
SESSION=$4

tmux new-session -s $SESSION -n bash -d

for ID in `seq 1 $NUM_SPAWNS`;

do

    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'python kafka_producer.py '"$IP_ADDR"' '"$ID"' '"$OPTION"'' C-m

done