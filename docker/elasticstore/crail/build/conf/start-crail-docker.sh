#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#substitude env variables in core-site.xml
envsubst < $CRAIL_HOME/conf/core-site.xml.env > $CRAIL_HOME/conf/core-site.xml

# for GKE we have to prepare instances to use hugepages in a datanode
# taken from pocket
#if [ $@ = "datanode" ]; then
#    mkdir -p /dev/hugepages
    # mount -t hugetlbfs nodev /dev/hugepages
#    chmod a+rw /dev/hugepages
#    mkdir /dev/hugepages/cache
#    mkdir /dev/hugepages/data
    
    # this sets the number of hugepages
#    OUTPUT1 = $(echo 5120  > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
#    echo $OUTPUT1 >> /crail/setup.log

#    echo 5120 > /crail/nr_hugepages
#    OUTPUT2 = $(cp /crail/nr_hugepages /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
#    echo $OUTPUT2 >> /crail/setup.log
#fi

if [ $@ = "datanode" ]; then
    ADDR="http://${CRAIL_RELOCATOR_PORT##*/}/log"
    DATA="Starting `hostname` at `date`"

    curl --request POST --data "$DATA" $ADDR

    mkdir -p /mnt/tmpfs
    chmod a+rw /mnt/tmpfs

    mount -t tmpfs -o size=${STORAGELIMIT_IN_G}G none /mnt/tmpfs

    mkdir -p /mnt/tmpfs/cache
    mkdir -p /mnt/tmpfs/data

fi

# start crail with logging to file
crail $@ &> output.log
