#!/bin/bash

IP_ADDR=$1
TOPIC=$2
OPTION=$3
NUM_SPAWNS=$4
SESSION=$5

tmux new-session -s $SESSION -n bash -d

for ID in `seq 1 $NUM_SPAWNS`;

do

    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'python kafka_producer.py '"$IP_ADDR"' '"$ID"' '"$TOPIC"' '"$OPTION"'' C-m

done