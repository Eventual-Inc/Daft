#!/bin/bash


echo "Formatting namenode directory"
rm -rf /tmp/hadoop-hadoop/dfs/name
rm -rf /tmp/hadoop-hadoop/dfs/data
/opt/hadoop/bin/hdfs namenode -format

# 执行原始的 entrypoint 命令
exec "$@"
