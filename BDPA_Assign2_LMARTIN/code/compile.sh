#!/bin/bash
rm -rf *.class *.jar;
hadoop com.sun.tools.javac.Main $1.java;
jar cf $1.jar $1*.class;
rm $1*.class;
