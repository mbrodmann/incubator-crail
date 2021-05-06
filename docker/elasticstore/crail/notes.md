
## Starting a Kubernetes Cluster

### Disable systemd-resolved
Suggested in the tutorial, however unlikely that this step is actually required. Be careful when disabling the systemd-resolved service, this can cause several issues.

First point to a running DNS server.
Before disabling systemd-resolved, it is necessary to manually set the IP address of a DNS server.
Edit the /etc/resolv.conf file and set the DNS IP:

    nameserver 10.11.12.13
    search your.domain.tld

Also disable the management of /etc/resolv.conf by the NetworkManager by creating the file
/etc/NetworkManager/conf.d/no-dns.conf with this content:

    [main]
    dns=none

Finally, stop the relevant services.

    sudo systemctl disable systemd-resolved.service
    sudo systemctl stop systemd-resolved



## Commands
### All nodes: installing required software

As root, enter the following commands:

    aptitude install docker.io
    aptitude install apt-transport-https
    curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
    echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" > /etc/apt/sources.list.d/kubernetes.list
    aptitude update
    aptitude install kubelet kubeadm kubernetes-cni


### On the Kubernetes master node: Starting the Cluster

The default configuration uses 10.244.0.0/16 for POD networks and 10.96.0.1 for the service network.
If this conflicts with IPs used on hardware network interfaces, change the range with --pod-network-cidr
and --service-cidr as well as in the kube-flannel.yaml configuration file (see below).

It is better to run with the default configuration when possible (see below for example).

As root, enter the following commands (kubernetesuser is a regular Unix username or your perosnal user id):

    sudo kubeadm init --pod-network-cidr=10.244.0.0/16 
    su kubernetesuser
    mkdir -p $HOME/.kube
    cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
    chown $(id -u):$(id -g) $HOME/.kube/config
    exit

It is important to specify the pod-network-cidr as above. Otherwise flannel will have problems initializing - if changes are required also update the flannel configuration yaml file.

### On the kubernetes slave nodes

Join the cluster, corresponding information is also printed by the cluster master:

    kubeadm join 192.144.144.13:6443 --token 123456.kjdjdhj \
        --discovery-token-ca-cert-hash sha256:1234567890abcdef1234567890abcdef1234567890abcdef


### Again on the Kubernetes master node
Install the flannel network:

    kubectl get nodes
    kubectl apply -f  \
        https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml



## Some links

- [Adrians Tutorial for configuring a Kubernetes Cluster](https://github.com/asqasq/docs/blob/master/kubernetes/kubernetes.md)
- [Debug Kubernetes DNS](https://kubernetes.io/docs/tasks/administer-cluster/dns-debugging-resolution/)
- [Potential fix for IPTables](https://github.com/coredns/coredns/issues/2693)
- [Reset IPTables](https://serverfault.com/questions/200635/best-way-to-clear-all-iptables-rules)
- [Get shell to running Container](https://kubernetes.io/docs/tasks/debug-application-cluster/get-shell-running-container/)
- [Rebuild Docker Image from specific step](https://stackoverflow.com/questions/35154219/rebuild-docker-image-from-specific-step)
- [Fix Docker unable to use DNS](https://stackoverflow.com/questions/24991136/docker-build-could-not-resolve-archive-ubuntu-com-apt-get-fails-to-install-a)
- [Setup flannel](https://github.com/flannel-io/flannel/blob/master/Documentation/kubernetes.md)
- [Clean CNI and solve related issues](https://github.com/kubernetes/kubernetes/issues/39557)
- [Schedule pods on master node](https://stackoverflow.com/questions/43147941/allow-scheduling-of-pods-on-kubernetes-master)
- [Run metrics server](https://stackoverflow.com/questions/52224829/kubernetes-metrics-unable-to-fetch-pod-node-metrics)