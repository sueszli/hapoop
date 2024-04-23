from mrjob.job import MRJob
from mrjob.step import MRStep

import numpy as np
import pandas as pd

from timeit import default_timer as timer
import math
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

# debug: assert False, f"{tokens=}"


# - Calculate chi-square values for all unigram terms for each category
# - Order the terms according to their value per category and preserve the top 75 terms per category
# - Merge the lists over all categories

# Produce a file output.txt from the development set that contains the following:
# - One line for each product category (categories in alphabetic order), that contains the top 75 most discriminative terms for the category according to the chi-square test in descending order, in the following format:
# - <category name> term_1st:chi^2_value term_2nd:chi^2_value ... term_75th:chi^2_value
# - One line containing the merged dictionary (all terms space-separated and ordered alphabetically)


# TODO: Calculate chi-square values for all unigram terms for each category
class ChiSquareJob(MRJob):
    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init_category_counter(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def get_termfreq_from_review(self, _, line):
        assert _ is None

        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        # tokenize, case fold, and filter out stopwords
        tokens = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        tokens = [token.lower() for token in tokens if token not in self.stopwords and len(token) > 1]

        # case fold
        tokens = [token.lower() for token in tokens]

        # get term frequency: {term: occurences}
        termfreqdict = Counter(tokens)
        yield category, termfreqdict

    def reducer(self, category, termfreqdicts: list[Counter]):
        # merge termfreqdicts
        merged = functools.reduce(lambda x, y: x + y, termfreqdicts)
        yield category, merged

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init_category_counter,
                mapper=self.get_termfreq_from_review,
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
