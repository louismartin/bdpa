#!/bin/bash
./compile.sh $1;
hadoop jar $1.jar $1 data/corpus/ out/;
hdfs dfs -getmerge out/ $1.csv;
rm .*.crc;
