#!/bin/bash
username=$(whoami)

/hadoop/bin/hdfs dfs -mkdir /user/$username/stream/
/hadoop/bin/hdfs dfs -rm /user/$username/stream/*

# checkpointing is needed for updateStateByKey
/hadoop/bin/hdfs dfs -mkdir /user/$username/checkpoints/
/hadoop/bin/hdfs dfs -rm /user$username/checkpoints/*

i=0
while [ $i -ne 5 ]; do
    i=$((i+1))
    tmplog="words.`date +'%s'`.txt"
    python3 data.py;
    /hadoop/bin/hdfs dfs -put words.txt /user/$username/stream/$tmplog
    echo "`date +"%F %T"` generating $tmplog succeed"
    rm words.txt
    sleep 56;
done
