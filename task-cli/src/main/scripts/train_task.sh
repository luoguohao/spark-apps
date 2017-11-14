#!bin/bash

source /etc/profile
source ~/.bashrc

JAR_PATH=$(cd `dirname $0`/..; pwd)
JAR_NAME="task-cli-1.0.jar"
LOG_DIR=$JAR_PATH/logs
VAR_DIR=$JAR_PATH/var

if [ ! -d $LOG_DIR ]; then
  mkdir -p $LOG_DIR
fi

if [ ! -d $VAR_DIR ]; then
  mkdir -p $VAR_DIR
fi

TASK_TYPE=1
AIR_PATH='/data/datacenter/locLib/railway/2017-02'
MONTH_ID='201702'
TABLE_NAME='trip_tourist_travel_detail_monthly'

nohup java -jar $JAR_PATH/$JAR_NAME $TASK_TYPE $MONTH_ID $AIR_PATH $TABLE_NAME > $LOG_DIR/execute_train.log 2>&1 &

echo $$ > $VAR_DIR/pid_train
