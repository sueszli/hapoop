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


# in output.txt:
# - One line for each product category (categories in alphabetic order), that contains the top 75 terms with highest chi2 in category in ascending order, in this format:
# - <category name> term_1st:chi^2_value term_2nd:chi^2_value ... term_75th:chi^2_value
# - One line containing the merged dictionary (all terms space-separated and ordered alphabetically)


class ChiSquareJob(MRJob):
    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def get_line_termfreq(self, _: None, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = [t.lower() for t in terms if t not in self.stopwords and len(t) > 1]

        termfreq = Counter(terms)  # {term: freq} of a review
        yield category, termfreq

    def get_category_termfreq(self, category: str, termfreq: list[Counter]):
        cat_termfreq = Counter()  # flatmap
        for tf in termfreq:
            cat_termfreq.update(tf)
        yield None, {category: cat_termfreq}  # send to a single reducer

    def get_chi2(self, _, cat_termfreqs: list[dict[str, Counter]]):
        ctf: dict[str, Counter] = {}  # {category: {term: freq}}
        for ct in cat_termfreqs:
            ctf.update(ct)

        tf: Counter = Counter()  # {term: freq} for all categories
        for termfreq in ctf.values():
            tf.update(termfreq)

        # 1) calculate chi2 of all terms for each category
        chi2_values = {}  # {category: {term: chi2}}
        for cat, termfreq in ctf.items():
            chi2_values[cat] = {}
            total_in_cat = sum(termfreq.values())
            total_in_all = sum(tf.values())

            for term, freq in termfreq.items():
                observed = freq
                expected = tf[term] * total_in_cat / total_in_all
                chi2_values[cat][term] = (observed - expected) ** 2 / expected

        # 2) only keep top 75 terms per category with highest chi2 values, sort terms based on chi2 values
        top_terms = {}
        for cat, term_chi2 in chi2_values.items():
            top_terms[cat] = heapq.nlargest(75, term_chi2, key=term_chi2.get)

        # 3) merge all terms, over all categories
        all_terms = sorted(set(itertools.chain.from_iterable(top_terms.values())))
        all_terms_str = " ".join(all_terms)

        yield None, {"top_terms": top_terms, "all_terms": all_terms_str}

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.get_line_termfreq,
                reducer=self.get_category_termfreq
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
