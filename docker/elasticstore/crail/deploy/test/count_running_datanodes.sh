while [ 1 ]; do
	NUM=`kubectl -n crail get pods --selector=app=datanode-tcp \
	--output=jsonpath='{.items[*].status.phase}' |  grep -o Running | wc -l`;
	#echo "`date` $NUM"
	echo "`date +%s%N | cut -b1-13` $NUM"
	sleep 1
done
