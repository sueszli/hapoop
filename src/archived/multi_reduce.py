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
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


"""
discarded implementation: multi-reduce approach

- slower than single-reduce
- consumes more memory than single-reduce
- more complex than single-reduce (in terms of code)
"""


class ChiSquareJob(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def chi2_mapper(self, _: None, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        cat = json_dict["category"]

        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = set(terms) - self.stopwords
        terms = [t.lower() for t in terms if len(t) > 1]

        for term in terms:
            yield (term, cat), 1

        # each line / review has exactly one category
        yield (None, cat), 1

    def chi2_combiner(self, key: tuple[Optional[str], str], counts: list[int]):
        # aggregate counts from both channels, send to single reducer
        yield None, (key, sum(counts))

    def chi2_reducer(self, _: None, key_count: list[tuple[tuple[Optional[str], str], int]]):
        logger.info("reached chi2_reducer")
        N = 0
        cat_count = defaultdict(int)
        term_count = defaultdict(int)
        term_cat_count = defaultdict(int)

        for key, count in key_count:
            term, cat = key
            if term == None:
                N += count
                cat_count[cat] = count
            else:
                term_count[term] += count
                term_cat_count[(term, cat)] = count

        # can't merge with previous loop because we need to calculate N first
        for term, cat in term_cat_count:
            A = term_cat_count[(term, cat)]
            B = term_count[term] - A
            C = cat_count[cat] - A
            D = N - A + B + C

            chi2 = (N * (A * D - B * C) ** 2) / ((A + B) * (A + C) * (B + D) * (C + D))
            yield cat, (term, chi2)

    def top75_reducer(self, cat: str, term_chi2s: list[tuple[str, float]]):
        top75_term_chi2 = heapq.nlargest(75, term_chi2s, key=lambda x: x[1])

        yield cat, " ".join([f"{term}:{chi2}" for term, chi2 in top75_term_chi2])  # channel 1: "cat term:chi2"
        yield None, [term for term, _ in top75_term_chi2]  # channel 2: [term]

    def output_reducer(self, key: Optional[str], vals: list):
        vals = list(vals)
        if key != None:
            assert len(vals) == 1
            yield key, str(vals[0])  # channel 1: keep as is
        else:
            yield None, " ".join(sorted(list(itertools.chain(*vals))))  # channel 2: sort terms

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.chi2_mapper,
                combiner=self.chi2_combiner,
                reducer=self.chi2_reducer,
            ),
            MRStep(
                reducer=self.top75_reducer,
            ),
            MRStep(
                reducer=self.output_reducer,
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    runtime = t2 - t1
    logger.info(f"time: {runtime:.2f}s")
