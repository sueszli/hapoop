# https://www.youtube.com/watch?v=H999fIuymqc
# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

green=`tput setaf 2`
reset=`tput sgr0`

if [[ "$OSTYPE" != "darwin"* ]]; then echo "this script is for macOS only"; exit 1; fi
if ! command -v java &>/dev/null; then echo "java is not installed"; exit 1; fi # use https://sdkman.io/install

# install dependencies
brew install ssh > /dev/null
brew install pdsh > /dev/null
brew install hadoop > /dev/null

if ! command -v hadoop &>/dev/null; then echo "hadoop is not installed"; exit 1; fi

# write java home path into hadoop config
JAVA_HOME_PATH=$(echo $JAVA_HOME)
HADOOP_VERSION=$(hadoop version | head -1 | awk '{print $2}')
HADOOP_CONFIG_PATH="/opt/homebrew/Cellar/hadoop/$HADOOP_VERSION/libexec/etc/hadoop/hadoop-env.sh"
echo "${green}writing JAVA_HOME into: $HADOOP_CONFIG_PATH...${reset}"

WRITTEN=$(sed -i '' '/^export JAVA_HOME/d' $HADOOP_CONFIG_PATH)
if ! [ -z "$WRITTEN" ]; then
    echo "JAVA_HOME already written in $HADOOP_CONFIG_PATH"
else
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> $HADOOP_CONFIG_PATH
    # echo "export HADOOP_OPTS=-Djava.net.preferIPv4Stack=true" >> $HADOOP_CONFIG_PATH
fi
# bat $HADOOP_CONFIG_PATH
