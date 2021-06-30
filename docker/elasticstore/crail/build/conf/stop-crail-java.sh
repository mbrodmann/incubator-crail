PID=`ps axf | grep java | grep -v grep | awk '{print $1}'`
kill $PID
echo "Killed java process with PID $PID"
