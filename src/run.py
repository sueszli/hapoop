from mrjob.job import MRJob
from mrjob.step import MRStep

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
        filtered_tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
        assert isinstance(filtered_tokens, list)
        yield None, filtered_tokens

    def preprocessing_reducer(self, _, tokens):
        # count tokens
        token_counts = Counter()
        for token_list in tokens:
            token_counts.update(token_list)
        assert isinstance(token_counts, Counter)
        yield None, token_counts

    def steps(self):
        # fmt: off
        return [
            MRStep(mapper=self.preprocessing_mapper, reducer=self.preprocessing_reducer)
            # MRStep(mapper=self.chi_square_mapper, reducer=self.chi_square_reducer)
        ]
        # fmt: on


if __name__ == "__main__":
    ChiSquareJob.run()
