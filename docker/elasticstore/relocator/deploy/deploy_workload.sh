#!/bin/bash


export NUM=$1

echo "Starting Workload"

envsubst < yamls/crail-workload.yaml | kubectl apply -n crail -f -
#kubectl apply -n crail -f yamls/crail-relocator.yaml
