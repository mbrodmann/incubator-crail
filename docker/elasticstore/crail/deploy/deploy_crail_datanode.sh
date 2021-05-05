#!/bin/bash


export NUM=$1

echo "Starting Datanode-$NUM"

envsubst < yamls/crail-datanode.yaml | kubectl apply -n crail -f -
