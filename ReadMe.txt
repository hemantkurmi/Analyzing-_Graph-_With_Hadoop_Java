Imagine that your boss gives you a large dataset which contains an entire email communication network from a popular social network site. The network is organized as a directed graph where each node represents an email address and the edge between two nodes (e.g., Address A and Address B) has a weight stating how many times A wrote to B. You have been tasked with finding which people have received the most emails. Your task is to write a MapReduce program in Java to report, for each node in the graph, the sum of weights among all of the node’s inbound edges.

code accept two arguments upon running. The first argument (args[0]) is a path for the input graph file on HDFS (e.g., data/graph1.tsv), and the second argument (args[1]) is a path for output directory on HDFS (e.g., data/q1output1). The default output mechanism of Hadoop will create multiple files on the output directory such as part-00000, part-00001, which will be merged and downloaded to a local directory by the supplied run script. Please use the run1.sh and run2.sh scripts for your convenience.


create the data as

src        tgt        weight

10        110        3

10        200        1

200        150        30

100        110        10

110        200        15

130        110        67

