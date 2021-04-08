#!/bin/bash

OUTPUTPATH="od_res/"

echo ===== OutDegree JAVA VERSION =====

echo ===== Compile =====
javac -classpath `/hadoop/bin/yarn classpath` OutDegreeOptional.java
jar cf wc.jar OutDegreeOptional*.class
echo
echo ===== Clear old output files on HDFS =====
/hadoop/bin/hdfs dfs -rm -r $OUTPUTPATH
echo
echo ===== RUN CASE1=====
/hadoop/bin/yarn jar wc.jar OutDegreeOptional /hw5_data/case1 $OUTPUTPATH"case1" 2 $OUTPUTPATH"case1limited"
echo
echo ===== RUN CASE2=====
/hadoop/bin/yarn jar wc.jar OutDegreeOptional /hw5_data/case2 $OUTPUTPATH"case2" 20 $OUTPUTPATH"case2limited"
echo

echo DONE!

echo You can use "/hadoop/bin/hdfs dfs -get od_res/ {your_local_path}" to get the result file
echo
