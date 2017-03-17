#!/bin/bash
./run.sh WordCount data/corpus
./run.sh Preprocess data/corpus
# Keep only first 5000 lines for faster execution
sed -i "5001,$ d" preprocess.csv
./run.sh FastSimilarity preprocess.csv
