#!/bin/bash
RED='\033[0;31m'
NC='\033[0m' # No Color

argpath="$1"
if [[ -z $argpath ]]; then
   echo "You need to pass the path!"
   echo -e "Example: ./script.sh ${RED}/your/awesome/path${NC}"
   exit
fi

${HADOOP_HOME}/bin/hdfs dfs -test -e "$argpath"
check_exist=$?

if [[ $check_exist == 0 ]]; then
   echo "The passed path exists in HDFS"
else
   echo "The passed path does not exists in HDFS"
   exit
fi

${HADOOP_HOME}/bin/hdfs dfs -test -d "$argpath"
check_is_dir=$?

if [[ $check_is_dir == 0 ]]; then
   echo "The passed path is directory. Execute dfs -ls"
   ${HADOOP_HOME}/bin/hdfs dfs -ls $1
else
   echo "The passed path is file. Execute dfs -text"
   ${HADOOP_HOME}/bin/hdfs dfs -text $1
fi