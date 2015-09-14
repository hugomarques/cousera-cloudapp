export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar;

rm -rf build/*;

hadoop fs -rm -r /mp2/*-output/;

hadoop com.sun.tools.javac.Main OrphanPages.java -d build;

jar -cvf OrphanPages.jar -C build/ ./;

hadoop jar OrphanPages.jar OrphanPages /mp2/links /mp2/D-output;





