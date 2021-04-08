import re
import sys
from pyspark import SparkConf, SparkContext
import time
import re


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])
    
    first = time.time()

    # Students: Implement Word Count!
    words = lines.flatMap(lambda x: re.split(r'[^\w]+', x))   \
                                    .map(lambda x: (x,1))           \
                                    .sortByKey() \
                                    .reduceByKey(lambda a,b: a + b) \
                                    .sortBy(lambda x: -x[1]) \
                                    .take(10)

    print(words)

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
