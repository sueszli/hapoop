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

logging.basicConfig(level=logging.DEBUG)
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

        # channel for category count
        yield ("__chan__", category), 1

        # channel for term count (a line has multiple terms, so we can't use a single yield)
        for term in terms:
            yield (term, category), 1

    def combiner(self, key: str | tuple[str, str], counts: list[int]):
        # avoid channel key conflict
        # if isinstance(key, tuple):
        #     assert key[0] != "__chan__"
        # else:
        #     assert key == "__chan__"

        # chunk multiple yields from the same mapper, send all to the same reducer
        yield None, (key, sum(counts))

    def reducer(self, _: None, vals: list[tuple[str | tuple[str, str], int]]):
        N = 0
        term_count = defaultdict(int)
        cat_count = defaultdict(int)
        term_cat_count = defaultdict(int)

        for elem in vals:
            assert len(elem) == 2
            key: str | tuple[str, str] = elem[0]
            val: int = elem[1]

            if isinstance(key, str):
                N += val
            else:
                term, category = key
                term_cat_count[(term, category)] += val
                term_count[term] += val
                cat_count[category] += val

        chi2_cat_term = {}
        for term, category in term_cat_count:
            A = term_cat_count[(term, category)]
            B = term_count[term] - A
            C = cat_count[category] - A
            D = N - A + B + C

            chi2 = (N * (A * D - B * C) ** 2) / ((A + B) * (A + C) * (B + D) * (C + D))
            if category not in chi2_cat_term:
                chi2_cat_term[category] = {}
            chi2_cat_term[category][term] = chi2

        # sort, get top 75
        for cat, terms in chi2_cat_term.items():
            chi2_cat_term[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda x: x[1]))

            if not chi2_cat_term[cat]:
                del chi2_cat_term[cat]

        # sort by category
        chi2_cat_term = dict(sorted(chi2_cat_term.items(), key=lambda x: x[0]))

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
