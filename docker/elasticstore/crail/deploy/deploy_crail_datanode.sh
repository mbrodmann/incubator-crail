#!/bin/bash


export END=$1

for i in $(seq 1 $END); do
export NUM=$i;
echo "Starting Datanode-$NUM";
envsubst < yamls/crail-datanode.yaml | kubectl apply -n crail -f - ; done
