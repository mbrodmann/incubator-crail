#!/bin/bash


#create the .kube/config file as Secret so that
#the container can access the certificate and cluster
#information

echo "
apiVersion: v1
kind: Secret
metadata:
  name: kubeconfig-secret
data:
  config: `kubectl config view --raw --flatten|base64 -w 0 `
" | kubectl apply -f yamls/metricsserver.yaml
