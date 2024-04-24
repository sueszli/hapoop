from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol
import mrjob

import logging
from timeit import default_timer as timer
import functools
import itertools
import re
import json
from collections import Counter, defaultdict
import heapq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChiSquareJob(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def mapper(self, _: None, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = set(terms) - self.stopwords
        terms = [t.lower() for t in terms if len(t) > 1]

        yield ("__category_count__", category), 1

        for term in terms:
            yield (term, category), 1

    def combiner(self, key, count):
        yield None, (key, sum(count))

    def reducer(self, _, counts):
        # N = 0
        # term_total = defaultdict(int)
        # cat_total = defaultdict(int)
        # for key, count in counts:
        #     if key[0] == "__category_count__":
        #         cat_total[key[1]] = count
        #         N += count
        #     else:
        #         term, category = key
        #         term_total[term] += count
        category_counts = defaultdict(int)
        term_counts = defaultdict(int)
        total_review_count = 0
        total_review_term_count = defaultdict(int)

        for key, count in counts:
            if key[0] == "__category_count__":
                category_counts[key[1]] = count
                total_review_count += count
            else:
                term, category = key
                term_counts[(term, category)] = count
                total_review_term_count[term] += count

        chi2_cat_term = {}
        for term, category in term_counts:
            A = term_counts[(term, category)]
            B = total_review_term_count[term] - A
            C = category_counts[category] - A
            D = total_review_count - (A + B + C)
            N = total_review_count

            numerator = N * (A * D - B * C) ** 2
            denominator = (A + B) * (A + C) * (B + D) * (C + D)
            chi_square = numerator / denominator if denominator != 0 else 0

            if category not in chi2_cat_term:
                chi2_cat_term[category] = {}
            chi2_cat_term[category][term] = chi_square

        # sort, get top 75
        for cat, terms in chi2_cat_term.items():
            chi2_cat_term[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda x: x[1]))

            if not chi2_cat_term[cat]:
                del chi2_cat_term[cat]

        for cat, terms in chi2_cat_term.items():
            yield None, str(cat) + " " + " ".join(f"{term}:{chi2}" for term, chi2 in terms.items())

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    # print(f"runtime: {delta:.4f} seconds")
