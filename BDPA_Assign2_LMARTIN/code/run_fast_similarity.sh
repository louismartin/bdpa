#!/bin/bash
./run.sh WordCount data/corpus
./run.sh Preprocess data/corpus
./run.sh FastSimilarity preprocess.csv
