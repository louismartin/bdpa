#!/bin/bash
rm -rf out *.class *.jar;
hadoop com.sun.tools.javac.Main $1.java;
jar cf run.jar $1*.class;
hadoop jar run.jar $1 ../data/ out;
cat out/part-r-00000
