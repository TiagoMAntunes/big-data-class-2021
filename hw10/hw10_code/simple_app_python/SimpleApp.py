from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="Py_HDFSWordCount")
ssc = StreamingContext(sc, 60)
ssc.checkpoint('hdfs://intro00:9000/user/2020280599/checkpoints')

def update(newValues, prev):
    if prev is None:
        prev = 0
    return sum(newValues, prev)

lines = ssc.textFileStream("hdfs://intro00:9000/user/2020280599/stream")  #  you should change path to your own directory on hdfs
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

runningCounts = counts.updateStateByKey(update).transform(lambda x: x.sortBy(lambda y: -y[1]))

runningCounts.pprint(100)

ssc.start()
ssc.awaitTermination()


