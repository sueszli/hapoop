load gutenberg books into hdfs:

```bash
# load the books into hdfs
hdfs dfs -mkdir books
hdfs dfs -put ./exercise_0/gutenberg_books/* books

# check if the files are there
hdfs dfs -ls /user/sueszli/books

# run python script to count the words
pip install mrjob

# run the script
python3 ./exercise_0/WordCount/src/DIC_runner.py --hadoop-streaming-jar

# /opt/homebrew/Cellar/hadoop/3.3.6/libexec/sbin/start-yarn.sh
```
