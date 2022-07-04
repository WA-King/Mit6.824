#! /bin/bash

for((i=1;i<=$1;i++));do
    echo "test $i" > output.txt
    go test -run $2 -race >> output.txt
    fail=`grep FAIL output.txt | wc -l`
    if [ $fail != "0" ] 
    then
        break
    fi
    sleep 1
done
