# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.)

FROM ubuntu:18.04

# TODO: automate update version on new release
ARG GIT_COMMIT="v1.2"
ENV LOG_COMMIT=$GIT_COMMIT


RUN echo "Crail-$LOG_COMMIT install openjdk8, git and envsubst" && \
    apt-get update && apt-get install --no-install-recommends -y \
    openjdk-8-jdk-headless \
    git \
    gettext-base \
    libxml2-utils \
    openssh-server \
    collectl
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=${PATH}:${JAVA_HOME}/bin

# We need to install maven separately since it otherwise pulls in java 10
RUN echo "Crail-$LOG_COMMIT install maven" && \
    apt-get install --no-install-recommends -y maven

# for fast rebuilds only
COPY marker /dev/null

RUN echo "Crail-$LOG_COMMIT clone & build Crail repo" && \
    git clone https://github.com/mbrodmann/incubator-crail.git && \
    cd /incubator-crail && \
    git checkout retryClient && \
    mvn -DskipTests package

RUN echo "Move crail to /crail" && \
    rm -rf /crail && \
    v=`xmllint --xpath "string(/*[local-name()='project']/*[local-name()='version'])" /incubator-crail/pom.xml` && \
    mv /incubator-crail/assembly/target/apache-crail-${v}-bin/apache-crail-${v} /crail

ENV CRAIL_HOME /crail
ENV PATH=${PATH}:${CRAIL_HOME}/bin


ENV NAMENODE_HOST=localhost
ENV NAMENODE_PORT=9070
ENV INTERFACE=eth0

# using /crail/data will use docker's default storage volume which is mounted on disk
# ENV DATAPATH=/crail/data

# experiment: use hugepages allocated by Kubernetes
ENV DATAPATH=/hugepages-2Mi

ENV STORAGELIMIT=8589934592
ENV CACHEPATH=/crail/cache
ENV CACHELIMIT=0

COPY ./conf/start-crail-docker.sh $CRAIL_HOME/bin
COPY ./conf/crail-env.sh $CRAIL_HOME/conf
COPY ./conf/core-site.xml.env $CRAIL_HOME/conf
COPY ./conf/crail-site.conf $CRAIL_HOME/conf

ENTRYPOINT ["start-crail-docker.sh"]
