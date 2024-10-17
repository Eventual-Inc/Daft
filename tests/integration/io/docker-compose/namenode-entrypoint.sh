#!/bin/bash

# 检查是否需要格式化 namenode
if [ ! -d "/hadoop/dfs/name" ] || [ -z "$(ls -A /hadoop/dfs/name)" ]; then
  echo "Formatting namenode directory"
  $HADOOP_HOME/bin/hdfs namenode -format
fi

# 执行原始的 entrypoint 命令
exec "$@"