#!/bin/bash

RELOCATOR_DIR="/home/bro/repos/crail-S3"

if [[ -z $RELOCATOR_DIR ]]; then
	echo "Make sure to set RELOCATOR_DIR variable to corresponding path"
	exit
fi

echo "Copying $RELOCATOR_DIR to ."
cp -r $RELOCATOR_DIR .

#(cd . && docker build -t crail-relocator .)
#(cd . && docker build --no-cache -t crail-elastic .)
(cd . && date > marker && docker build -t crail-relocator .)


