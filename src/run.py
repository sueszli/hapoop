from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol

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


class ChiSquareJob(MRJob):

    # OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol  # get rid of quotes

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

        # 1) calculate chi2 of all terms for each category
        chi2_values = {}  # {category: {term: chi2}}
        total_in_all = sum(sum(tf.values()) for tf in ctf.values())

        for cat in list(ctf.keys()):
            chi2_values[cat] = {}
            total_in_cat = sum(ctf[cat].values())
            for term, freq in ctf[cat].items():
                total_of_term = sum(ctf[cat].get(term, 0) for cat in ctf.keys())

                observed = freq
                expected = total_of_term * total_in_cat / total_in_all
                chi2_values[cat][term] = (observed - expected) ** 2 / expected

        # 2) filter by top 75 highest chi2 values, sorted in ascending order
        top75_chi2 = {}
        for cat in chi2_values.keys():
            top75_chi2[cat] = dict(heapq.nlargest(75, chi2_values[cat].items(), key=lambda x: x[1]))

        # 3) discard empty categories
        top75_chi2 = {cat: terms for cat, terms in top75_chi2.items() if terms}

        # 4) sort categories in alphabetic order
        top75_chi2 = dict(sorted(top75_chi2.items(), key=lambda x: x[0]))

        # 5) yield results for each category
        for cat, terms in top75_chi2.items():
            # <category name> term1:chi2 term2:chi2 ... term75:chi2
            yield None, str(cat) + " " + " ".join(f"{term}:{chi2:.4f}" for term, chi2 in terms.items())

            # one line with the merged dictionary (all terms space-separated and ordered alphabetically)
            yield None, " ".join(sorted(terms.keys()))

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
