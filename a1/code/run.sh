#!/bin/bash
rm -rf *.class *.jar;
hadoop com.sun.tools.javac.Main $1.java;
jar cf run.jar $1*.class;

# Usage:
# hadoop jar myfile.jar Class input_dir output_dir n_reducers combiner compression

# 10 reducers no combiner
rm -rf .out* out*
hadoop jar run.jar $1 ../data/ out 10 0 0;
hadoop fs -getmerge out out.csv;

# 10 reducers , 1 combiner
rm -rf .out* out*
hadoop jar run.jar $1 ../data/ out 10 1 0;
hadoop fs -getmerge out out.csv;

# 10 reducers , 1 combiner, compression
rm -rf .out* out*
hadoop jar run.jar $1 ../data/ out 10 1 1;
hadoop fs -getmerge out out.csv;

# 50 reducers , 1 combiner, compression
rm -rf .out* out*
hadoop jar run.jar $1 ../data/ out 10 1 1;
hadoop fs -getmerge out out.csv;
