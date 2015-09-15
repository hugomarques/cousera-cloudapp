export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar;

rm -rf build/*;

hadoop fs -rm -r /mp2/*-output/;

hadoop com.sun.tools.javac.Main TopPopularLinks.java -d build;

jar -cvf TopPopularLinks.jar -C build/ ./;

hadoop jar TopPopularLinks.jar TopPopularLinks -D N=5 /mp2/links /mp2/D-output;





