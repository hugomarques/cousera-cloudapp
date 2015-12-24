#!/bin/bash
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar;

rm -rf build/*;

hadoop fs -rm -r /mp2/*-output/;

hadoop com.sun.tools.javac.Main PopularityLeague.java -d build;

jar -cvf PopularityLeague.jar -C build/ ./;

hadoop jar PopularityLeague.jar PopularityLeague -Dleague=/mp2/misc/league.txt /mp2/links /mp2/F-output





