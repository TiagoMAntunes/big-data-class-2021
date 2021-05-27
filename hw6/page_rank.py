import sys
from pyspark import SparkConf, SparkContext
import time

if __name__ == '__main__':

    # Create Spark context.
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.setCheckpointDir('./checkpoints/')
    lines = sc.textFile(sys.argv[1])
    first = time.time()

    # Students: Implement PageRank!

    # (origin, (dst1,dst2,...))
    edges = lines.map(lambda x: x.split()) \
                .map(lambda edge: (int(edge[0]), int(edge[1]))) \
                .groupByKey() \
                .map(lambda x: (x[0], list(set(x[1]))))\
                .cache()

    num_nodes = edges.count()
    

    # (node, rank)
    ranks = edges.map(lambda x: (x[0], 1.0 / num_nodes))

    for iter in range(100):
                        # (origin, ((dst1,dst2,...), rank))
        computation = edges.join(ranks) \
            .flatMap(lambda el: [(dst, el[1][1] / len(el[1][0])) for dst in el[1][0]])\
            .reduceByKey(lambda a,b: a + b)
            # (node, score)

        # no dead ends in the graph
        ranks = computation.mapValues(lambda v: v * 0.8 + 0.2 / num_nodes)

        if iter % 10 == 0:
            ranks.checkpoint()

    highest = ranks.sortBy(lambda x: -x[1]).take(5)
    print('Highest: ', highest)
    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
