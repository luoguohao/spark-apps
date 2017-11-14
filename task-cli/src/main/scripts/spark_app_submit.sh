#!/bin/sh

SUBMITTER=/usr/local/spark/bin/spark-submit

JAR_PATH=$(cd `dirname $0`/..; pwd)
JAR_NAME="luogh-spark-1.0-SNAPSHOT.jar"


if [ $# -ne 4 ]; then
        echo "expected <CLASS_NAME> <data_parquet_path> <tdid_data_gzip_path> <output_path> !,
        but current param length=$# and params[$@]"
        exit 1
fi

CLASS_NAME=$1
DATA_PARQUET_PATH=$2
TDID_DATA_GZIP_PATH=$3
OUTPUT_PATH=$4
echo "class_name:"$CLASS_NAME
echo "data_parquet_path:"$DATA_PARQUET_PATH
echo "tdid_data_gzip_path:"$TDID_DATA_GZIP_PATH
echo "output_path:"$OUTPUT_PATH


${SUBMITTER} --class ${CLASS_NAME} \
--master yarn \
--deploy-mode cluster \
--name trip_stat \
--queue fedev \
--num-executors 20 \
--driver-memory 4g \
--executor-memory 6g \
--executor-cores 1 \
--conf spark.akka.frameSize=64 \
--conf spark.executor.extraJavaOptions="-XX:PermSize=128M -XX:MaxPermSize=128m -XX:+UseParNewGC
-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
$JAR_PATH/$JAR_NAME \
$DATA_PARQUET_PATH $TDID_DATA_GZIP_PATH $OUTPUT_PATH
