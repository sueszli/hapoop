from mrjob.job import MRJob
from mrjob.step import MRStep

import re
import json
import os
from pathlib import Path


import re
from mrjob.job import MRJob


# $ python3.11 ./src/run.py ./data/reviews_devset.json --stopwords ./data/stopwords.txt


class PreprocessJob(MRJob):
    def configure_args(self):
        super(PreprocessJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def mapper(self, _, line):
        # tokenizing
        tokens = re.split(r'[ \t\d()\[\]{}.!?,;:+=-_"\'~#@&*%€$§\/]+', line)

        # case folding
        tokens = [token.lower() for token in tokens]

        # stopword filtering
        stopwords = None
        with open(self.options.stopwords, "r") as f:
            stopwords = set(line.strip() for line in f)
        assert stopwords is not None
        filtered_tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
        yield None, list(filtered_tokens)

    def reducer(self, _, tokens):
        # just pass through, as we're not reducing in this step
        yield None, list(tokens)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == "__main__":
    PreprocessJob.run()
