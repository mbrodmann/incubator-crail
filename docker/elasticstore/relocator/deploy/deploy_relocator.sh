#!/bin/bash


export NUM=$1

echo "Starting Relocator"

envsubst < yamls/crail-relocator.yaml | kubectl apply -n crail -f -
#kubectl apply -n crail -f yamls/crail-relocator.yaml
