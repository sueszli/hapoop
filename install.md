docs: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

tutorial: https://www.youtube.com/watch?v=H999fIuymqc

# install binary

- install sdkman through https://sdkman.io/ to manage java version 11
- install java 11 (system breaks with latest java based on my experience)

```bash
brew install pdsh
brew install hadoop
hadoop version

sdk current # must be java 11
echo $JAVA_HOME
code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/hadoop-env.sh # add JAVA_HOME to `export JAVA_HOME=` line
```

# setup: pseudo-distributed mode

by default hadoop is set up to be in the "standalone" mode: single node cluster, emulated by a single java process.

but we want to set up hadoop in the "pseudo-distributed" mode: single node cluster, but each hadoop daemon runs in a separate java process. see website for details.

_configure_

update the xml files:

```bash
code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/core-site.xml
# <configuration>
#     <property>
#         <name>fs.defaultFS</name>
#         <value>hdfs://localhost:9000</value>
#     </property>
# </configuration>

code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/hdfs-site.xml
# <configuration>
#     <property>
#         <name>dfs.replication</name>
#         <value>1</value>
#     </property>
# </configuration>

code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/mapred-site.xml
# <configuration>
#     <property>
#         <name>mapreduce.framework.name</name>
#         <value>yarn</value>
#     </property>
#     <property>
#         <name>mapreduce.application.classpath</name>
#         <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
#     </property>
# </configuration>

code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/yarn-site.xml
# <configuration>
#     <property>
#         <name>yarn.nodemanager.aux-services</name>
#         <value>mapreduce_shuffle</value>
#     </property>
#     <property>
#         <name>yarn.nodemanager.env-whitelist</name>
#         <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
#     </property>
# </configuration>
```

_enable passphraseless ssh:_

```bash
sudo systemsetup -setremotelogin on

rm -rf ~/.ssh/id_rsa.pub
code ~/.ssh/authorized_keys

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa # don't rename the file
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

ssh localhost # check if works
exit # kill session
```

_test run_

```bash
# ----------------------------------- reset
jps | grep " NameNode" | awk '{print $1}' | xargs kill -9
jps | grep " SecondaryNameNode" | awk '{print $1}' | xargs kill -9
jps

rm -rf /tmp/hadoop
rm -rf /tmp/hadoop-sueszli
rm -rf /tmp/hadoop-yarn-sueszli

hdfs namenode -format

/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/stop-yarn.sh
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/stop-dfs.sh

# ----------------------------------- start
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-dfs.sh
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-yarn.sh

# import data
hdfs dfs -mkdir -p /user/sueszli
hdfs dfs -mkdir input
hdfs dfs -put /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/*.xml input

hdfs dfs -df -h
hdfs dfs -ls /user/sueszli
hdfs dfs -ls /user/sueszli/input

# run mapreduce
hadoop jar /opt/homebrew/Cellar/hadoop/3.3.6/libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar grep input output 'dfs[a-z.]+'

# print output
hdfs dfs -get output ./tmp
cat ./tmp/*
rm -rf ./tmp

# ----------------------------------- monitor
open http://localhost:9870/explorer.html#/
open http://localhost:8088/

# ----------------------------------- stop
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/stop-dfs.sh
/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/stop-yarn.sh
```
