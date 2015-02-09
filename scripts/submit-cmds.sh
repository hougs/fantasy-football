!#/bin/bash

#need to explicitly set hive jars on drivers, seemingly also executors
HIVE_CLASSPATH=$(find /opt/cloudera/parcels/CDH-5.3.0-1.cdh5.3.0.p0.30/lib/hive/lib/ -name '*.jar' \
-not -name 'guava*' -print0 | sed 's/\x0/:/g')

#Also need to set env var for picking up the hive configuration
export HADOOP_CONF_DIR=/etc/hive/conf

#And use a special command line option for setting the driver classpath
spark-submit --class com.cloudera.ds.PlayerPortfolios --master yarn-client \
--driver-class-path $HIVE_CLASSPATH --conf spark.executor.extraClassPath=$HIVE_CLASSPATH \
/home/juliet/fantasy-football-0.0.1-SNAPSHOT.jar
