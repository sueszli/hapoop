if [[ "$OSTYPE" != "darwin"* ]]; then echo "this script is for macos only"; exit 1; fi
if ! command -v hadoop > /dev/null; then echo "hadoop command not found"; exit 1; fi 
if ! command -v java > /dev/null; then echo "java command not found"; exit 1; fi

green=`tput setaf 2`
reset=`tput sgr0`

HADOOP_VERSION=$(hadoop version | head -1 | awk '{print $2}')
USERNAME=$(whoami)

# quit running process
echo "${green}stopping dfs and yarn...${reset}"
/opt/homebrew/Cellar/hadoop/${HADOOP_VERSION}/libexec/sbin/stop-dfs.sh
/opt/homebrew/Cellar/hadoop/${HADOOP_VERSION}/libexec/sbin/stop-yarn.sh

echo "${green}resetting namenode...${reset}"
jps | grep " NameNode" | awk '{print $1}' | xargs kill -9
jps | grep " SecondaryNameNode" | awk '{print $1}' | xargs kill -9
jps

# remove tmp files
echo "${green}removing tmp files...${reset}"
rm -rf /tmp/hadoop
rm -rf /tmp/hadoop-${USERNAME}
rm -rf /tmp/hadoop-yarn-${USERNAME}

# format namenode
echo "${green}formatting namenode...${reset}"
hdfs namenode -format

# start dfs and yarn
echo "${green}starting dfs and yarn...${reset}"
/opt/homebrew/Cellar/hadoop/${HADOOP_VERSION}/libexec/sbin/start-dfs.sh
/opt/homebrew/Cellar/hadoop/${HADOOP_VERSION}/libexec/sbin/start-yarn.sh

# creating user directory
echo "${green}creating user directory...${reset}"
hdfs dfs -mkdir -p /user/${USERNAME}

# open web ui
echo "${green}done!${reset}"
open http://localhost:9870/explorer.html#/user/${USERNAME}
# open http:/localhost:9870/dfshealth.html#tab-overview
# open http:/localhost:9864/
# open http:/localhost:8042/node
# open http:/localhost:8088/

# you can start using the cli now
# see: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CommandsManual.html
# see: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html
