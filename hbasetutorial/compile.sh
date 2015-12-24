#!/bin/bash
rm -rf *.class
export CLASSPATH=$(hbase classpath)
javac CreateTable.java ListTables.java
