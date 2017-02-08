#!/bin/bash
rm -rf .out* out* *.class *.jar;
hadoop com.sun.tools.javac.Main $1.java;
jar cf run.jar $1*.class;
hadoop jar run.jar $1 ../data/ out;
hadoop fs -getmerge out out.csv;
cat out.csv;
