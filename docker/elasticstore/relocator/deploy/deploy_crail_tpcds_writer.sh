#!/bin/bash


export NUM=$1

echo "Starting Crail-Tpcds-Writer"

envsubst < yamls/crail-tpcds-writer.yaml | kubectl apply -n crail -f -
#kubectl apply -n crail -f yamls/crail-relocator.yaml
