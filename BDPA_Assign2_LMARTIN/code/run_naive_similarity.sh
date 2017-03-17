#!/bin/bash
./run.sh WordCount data/corpus
./run.sh Preprocess data/corpus
# Keep only first 1000 lines for faster execution
# sed -i "1001,$ d" preprocess.csv
./run.sh NaiveSimilarity preprocess.csv
