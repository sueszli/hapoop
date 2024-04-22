from mrjob.job import MRJob
from mrjob.step import MRStep

import numpy as np
import pandas as pd

from timeit import default_timer as timer
import functools
import itertools
import re
import json
import os
from pathlib import Path
from collections import Counter
import heapq
from sklearn.feature_extraction.text import CountVectorizer
from scipy.stats import chi2


# $ python3.11 ./src/run.py ./data/reviews_devset.json --stopwords ./data/stopwords.txt


class ChiSquareJob(MRJob):
    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def preprocessing_mapper(self, _, line):
        # tokenizing
        tokens = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', line)

        # case folding
        tokens = [token.lower() for token in tokens]

        # stopword filtering
        stopwords = None
        with open(self.options.stopwords, "r") as f:
            stopwords = set(line.strip() for line in f)
        assert stopwords is not None
        is_valid = lambda t: t not in stopwords and len(t) > 1
        filtered_tokens: list = list(filter(is_valid, tokens))
        yield None, filtered_tokens

    def preprocessing_reducer(self, _, tokens):
        # flatmap, count tokens
        flat_tokens = list(itertools.chain(*tokens))
        counter = Counter(flat_tokens)
        yield None, counter

    def steps(self):
        # fmt: off
        return [
            MRStep(mapper=self.preprocessing_mapper, reducer=self.preprocessing_reducer)
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
