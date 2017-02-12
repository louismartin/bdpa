#!/bin/bash
./compile.sh StopWords

# Usage:
# hadoop jar myfile.jar Class input_dir output_dir n_reducers combiner compression

# 10 reducers no combiner
rm -rf .out* out*
hadoop jar StopWords.jar StopWords data/corpus/ out 10 0 0;
hadoop fs -getmerge out stopwords.csv;

# 10 reducers , 1 combiner
rm -rf .out* out*
hadoop jar StopWords.jar StopWords data/corpus/ out 10 1 0;
hadoop fs -getmerge out stopwords.csv;

# 10 reducers , 1 combiner, compression
rm -rf .out* out*
hadoop jar StopWords.jar StopWords data/corpus/ out 10 1 1;
hadoop fs -getmerge out stopwords.csv;

# 50 reducers , 1 combiner, compression
rm -rf .out* out*
hadoop jar StopWords.jar StopWords data/corpus/ out 10 1 1;
hadoop fs -getmerge out stopwords.csv;
