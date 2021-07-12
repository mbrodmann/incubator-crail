FROM ubuntu:18.04

RUN echo "Install openjdk8, git and envsubst" && \
    apt-get update && apt-get install --no-install-recommends -y \
    openjdk-8-jdk-headless \
    git \
    gettext-base \
    libxml2-utils \
    openssh-server \
    collectl \
    nano \
    python3 \
    python3-pip \
    curl \
    gnupg2 \
    fuse

# This configures collectl for larger bandwidths to not reject data
RUN echo "DefNetSpeed=-1" >> /etc/collectl.conf

RUN pip3 install requests

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=${PATH}:${JAVA_HOME}/bin

# We need to install maven separately since it otherwise pulls in java 10
RUN echo "Install maven" && \

    apt-get install --no-install-recommends -y maven

COPY marker /dev/null

RUN echo "Crail-$LOG_COMMIT clone & build Crail repo" && \
    git clone https://github.com/mbrodmann/incubator-crail.git && \
    cd /incubator-crail && \
    git checkout gke && \
    mvn -DskipTests install

COPY ./crail-S3 /crail-S3

RUN cd /crail-S3 && \
    bash crail-s3.sh build

# include gcsfuse
RUN export GCSFUSE_REPO=gcsfuse-focal && \
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list && \
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

RUN apt-get update && \
apt-get install --no-install-recommends -y gcsfuse


#RUN echo "Clone & build Crail repo" && \
#    git clone https://github.com/mbrodmann/incubator-crail.git && \
#    cd /incubator-crail && \
#    git checkout retryClient && \
#    mvn -DskipTests package

RUN echo "Move crail to /crail" && \
    rm -rf /crail && \
    v=`xmllint --xpath "string(/*[local-name()='project']/*[local-name()='version'])" /incubator-crail/pom.xml` && \
    mv /incubator-crail/assembly/target/apache-crail-${v}-bin/apache-crail-${v} /crail

ENV CRAIL_HOME /crail
ENV PATH=${PATH}:${CRAIL_HOME}/bin


ENV NAMENODE_HOST=localhost
ENV NAMENODE_PORT=9070
ENV INTERFACE=eth0
ENV DATAPATH=/crail/data
ENV STORAGELIMIT=1073741824
ENV CACHEPATH=/crail/cache
ENV CACHELIMIT=0

RUN mkdir -p $CRAIL_HOME/bin
RUN mkdir -p $CRAIL_HOME/conf

COPY ./conf/start-crail-docker.sh $CRAIL_HOME/bin
COPY ./conf/crail-env.sh $CRAIL_HOME/conf
COPY ./conf/core-site.xml $CRAIL_HOME/conf
COPY ./conf/crail-site.conf $CRAIL_HOME/conf
COPY ./conf/log4j.properties $CRAIL_HOME/conf

COPY ./tpcds-writer/run.sh $CRAIL_HOME

# for fast rebuilds only
COPY marker /dev/null
