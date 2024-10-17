#!/bin/bash


echo "Formatting namenode directory"
/opt/hadoop/bin/hdfs namenode -format

# 执行原始的 entrypoint 命令
exec "$@"
