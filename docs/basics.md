# hadoop

- NameNode: stores all metadata and block locations in memory
- DataNodes: stores and fetches blocks for client or nameNode

jobs (jar files) without YARN (yet another resource negotiator):

- master node: JobTracker
- worker node: TaskTracker, storage of intermediate data

# mapreduce pattern

pseudo code:

```
def map(docid a, doc d):
    for each word w in doc d:
        emit_intermediate(w, 1)

def combine(word w, list<count> counts):
    # do more local processing before passing to reduce
    ...

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

# ir information retrieval

preprocessing:

- tokenization: take corpus (collection of documents) and break them into a set of tokens (bag of words). we don't care about the order of the tokens, just their presence.
- stopping: remove common and irrelevant words (stop words) from the tokens.
- case folding: convert all tokens to lowercase.

representation:

- vector space model VSM: add weight to each token in the bag of words (ie. term frequency-inverse document frequency TF-IDF).
