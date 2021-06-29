# Saves tpcds data from Google Cloud Storage to Crail using gcsfuse mount

cd /crail

mkdir /tpcds-data
gcsfuse crail-tpcds-data /tpcds-data

INPUT_DIR=$1
bin/crail fs -copyFromLocal /tpcds-data/$INPUT_DIR /tpcds
