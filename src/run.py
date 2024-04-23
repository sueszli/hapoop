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


"""
chi2 = sum((O_i - E_i)^2 / E_i)
where:
- O_i = observed frequency of term i in category j
- E_i = expected frequency of term i in category j
      = frequency of i * probability of i
      = total frequency of term i in all categories * (total frequency of all terms in category j / total frequency of all terms in all categories)
"""


# TODO: Calculate chi-square values for all tokens for each category
class ChiSquareJob(MRJob):
    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def get_line_tokenfreq(self, _: None, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        tokens = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        tokens = [token.lower() for token in tokens if token not in self.stopwords and len(token) > 1]

        tokenfreqs = Counter(tokens)
        yield category, tokenfreqs

    def get_category_tokenfreq(self, category: str, tokenfreqs: list[Counter]):
        # merge all counters
        category_tokenfreq = Counter()
        for tokenfreq in tokenfreqs:
            category_tokenfreq += tokenfreq

        # combine
        yield "all", {category: category_tokenfreq}

    def get_chi2(self, _, category_tokenfreqs: list[dict[str, Counter]]):
        # merge all counters
        cat_token_freqs = {}
        for ctfs in category_tokenfreqs:
            for category, tokenfreqs in ctfs.items():
                cat_token_freqs[category] = tokenfreqs

        # calculate total frequency of all terms in all categories
        total_freq_all_terms_all_categories = sum(sum(tokenfreq.values()) for tokenfreq in cat_token_freqs.values())
        yield "all", total_freq_all_terms_all_categories

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.get_line_tokenfreq,
                reducer=self.get_category_tokenfreq
            ),
            MRStep(
                reducer=self.get_chi2
            )
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
