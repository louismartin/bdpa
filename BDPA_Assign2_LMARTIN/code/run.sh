#!/bin/bash
./compile.sh $1;
hadoop jar $1.jar $1 $2 out/;
hdfs dfs -getmerge out/ ${1,,}.csv;
rm .*.crc;
