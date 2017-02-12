#!/bin/bash
./compile.sh StopWords

# Usage:
# hadoop jar myfile.jar Class input_dir output_dir n_reducers combiner compression

# 10 reducers no combiner
hadoop jar StopWords.jar StopWords data/corpus/ out 10 0 0;
hdfs dfs -getmerge out stopwords.csv;

# 10 reducers, 1 combiner
hadoop jar StopWords.jar StopWords data/corpus/ out 10 1 0;
hdfs dfs -getmerge out stopwords.csv;

# 10 reducers, 1 combiner, compression
hadoop jar StopWords.jar StopWords data/corpus/ out 10 1 1;
hdfs dfs -getmerge out stopwords.csv;

# 50 reducers, 1 combiner, compression
hadoop jar StopWords.jar StopWords data/corpus/ out 50 1 1;
hdfs dfs -getmerge out stopwords.csv;
