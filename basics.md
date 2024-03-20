# hadoop

- NameNode: stores all metadata and block locations in memory
- DataNodes: stores and fetches blocks for client or nameNode

writing into a hdfs file

```bash
hadoop fs –put /temp/test hdfs://foo.com:9000/bar/test
```

# mapreduce

```python
def map(docid a, doc d):
    for each word w in doc d:
        emitIntermediate(w, 1)

def reduce(word w, list<count> counts):
    int result = 0
    for each v in counts:
        result += v
    emit(w, result)
```

mapreduce flow:

- map:
     - `[(k, v)] -> [(k', v')]`
     - process one key-value pair at a time, return a list of key-value pairs
     - ingest data
- shuffle & sort:
     - `[(k, v)] -> [(k', [v'])]`
     - group and sort the key-value pairs by key
- reduce:
     - `[(k, [v])] -> [(k', v')]`
     - process one key and its list of values at a time, return a list of key-value pairs

jobs (jar files) without YARN (yet another resource negotiator):

- master node: JobTracker
- worker node: TaskTracker, storage of intermediate data
