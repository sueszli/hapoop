green=`tput setaf 2`
reset=`tput sgr0`

# quit running process
echo "${green}stopping dfs and yarn...${reset}"
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/stop-dfs.sh
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/stop-yarn.sh

echo "${green}resetting namenode...${reset}"
jps | grep " NameNode" | awk '{print $1}' | xargs kill -9
jps | grep " SecondaryNameNode" | awk '{print $1}' | xargs kill -9
jps

# remove tmp files
echo "${green}removing tmp files...${reset}"
rm -rf /tmp/hadoop
rm -rf /tmp/hadoop-sueszli
rm -rf /tmp/hadoop-yarn-sueszli

# format namenode
echo "${green}formatting namenode...${reset}"
hdfs namenode -format

# start dfs and yarn
echo "${green}starting dfs and yarn...${reset}"
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-dfs.sh
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-yarn.sh

# creating user directory
echo "${green}creating user directory...${reset}"
hdfs dfs -mkdir -p /user/sueszli

# open web ui
echo "${green}done!${reset}"
# open http://localhost:9870/explorer.html#/
# open http://localhost:8088/
