#!/bin/bash
SPARK_HOME='/was/spark-1.3.1-bin-hadoop2.6'
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/conf
LOGS_DIR=$DEPLOY_DIR/logs
if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi

TODAY=`date +"%F"`
STDOUT_FILE=$LOGS_DIR/stdout-$TODAY.log

LIB_DIR=$DEPLOY_DIR/lib
LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ":"`

echo -e "Starting...\c"

nohup $SPARK_HOME/bin/spark-submit --class com.moneylocker.scheduler.collector.AdActionDataCollector --master spark://122.144.134.82:7077  --jars $LIB_DIR/spark-cassandra-connector-java_2.10-1.3.0-M1.jar,$LIB_DIR/spark-cassandra-connector_2.10-1.3.0-M1.jar,$LIB_DIR/cassandra-driver-core-2.1.6.jar,$LIB_DIR/guava-14.0.1.jar,$LIB_DIR/cassandra-thrift-2.1.3.jar,$LIB_DIR/joda-time-2.3.jar  --driver-class-path  $CONF_DIR:$LIB_JARS $LIB_DIR/ml-schedulers-0.0.1-SNAPSHOT.jar $1 $2 >> $STDOUT_FILE 2>&1 &

echo "OK!"
PIDS=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
echo "PID: $PIDS"
echo "STDOUT: $STDOUT_FILE"
