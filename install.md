docs: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

tutorial: https://www.youtube.com/watch?v=H999fIuymqc

## install binary

- install java 11 (system breaks with latest java based on my experience)
     - i recommend using "sdk man" to install and manage your java versions (https://sdkman.io/)
- `brew install pdsh` (parallel distributed shell)
- `brew install hadoop`
- update `hadoop-env.sh`: `code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/hadoop-env.sh` (hint: call `echo $JAVA_HOME` to find out the path)
- run `hadoop version` to check if it's installed

## setup: standalone mode

default mode after hadoop is installed.

single node cluster, emulated by a single java process.

## setup: pseudo-distributed mode

single node cluster, but each hadoop daemon runs in a separate java process.

see website for details.

**configuration:**

- update `core-site.xml`: `code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/core-site.xml`
- update `hdfs-site.xml`: `code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/hdfs-site.xml`

**passphraseless ssh:**

- enable remote login: `sudo systemsetup -setremotelogin on`
- generate ssh keys:

     - `rm -rf ~/.ssh/id_rsa.pub`
     - `cat ~/.ssh/id_rsa.pub`
     - `ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa`
     - `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`
     - `chmod 0600 ~/.ssh/authorized_keys`
     - test if it works:`ssh localhost` → don't forget to `exit`

**execution:**

- `hdfs namenode -format`
- `/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-dfs.sh`
- check out: http://localhost:9870/explorer.html#/

     - `hdfs dfs -mkdir -p /user/sueszli`
     - then the new user should show up in the web interface

- `hdfs dfs -mkdir input`
- `hdfs dfs -put /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/*.xml input`
- `hadoop jar /opt/homebrew/Cellar/hadoop/3.3.6/libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar grep input output 'dfs[a-z.]+'`

- update `mapred-site.xml`: `code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/mapred-site.xml`
- update `yarn-site.xml`: `code /opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/yarn-site.xml`

---

- format the hdfs: `hdfs namenode -format`
- start: `/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-all.sh`
- start: `/opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-yarn.sh`
