#!/bin/bash
./compile.sh InvertedIndex

# Usage:
# hadoop jar myfile.jar Class input_dir output_dir n_reducers combiner compression

rm -rf .out* out*
hadoop jar InvertedIndex.jar InvertedIndex ../example/ out 10 0 0;
hadoop fs -getmerge out invertedindex.csv;
