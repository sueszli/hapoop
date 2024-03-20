if [[ "$OSTYPE" != "darwin"* ]]; then echo "this script is for macos only"; exit 1; fi
if ! command -v hadoop > /dev/null; then echo "hadoop command not found"; exit 1; fi 

HADOOP_VERSION=$(hadoop version | head -1 | awk '{print $2}')
JAR_PATH="/opt/homebrew/Cellar/hadoop/$HADOOP_VERSION/libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar"

echo "hadoop version: $HADOOP_VERSION"
echo "hadoop jar path: $JAR_PATH"
hadoop jar $JAR_PATH grep input output 'dfs[a-z.]+'

echo -e "\n\n\n\n"
echo "final output:"
cat output/*
