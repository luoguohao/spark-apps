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

TASK_TYPE=2
AIR_PATH='/user/hadoop/user/guohao.luo/talkingtrip/travel_task_result_201702_7af179c16f17470b96efe3347c266529'
TABLE_NAME='trip_tourist_travel_detail_monthly'

nohup java -jar $JAR_PATH/$JAR_NAME $TASK_TYPE $AIR_PATH $TABLE_NAME > $LOG_DIR/execute_load_vertica.log 2>&1 &

echo $$ > $VAR_DIR/pid_load_vertica
