#!/bin/bash
./compile.sh InvertedIndex

# Usage:
# hadoop jar myfile.jar Class input_dir output_dir n_reducers combiner compression

hadoop jar InvertedIndex.jar InvertedIndex data/corpus/ out 10 1 0;
hdfs dfs -getmerge out invertedindex.csv
