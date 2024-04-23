from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol

from timeit import default_timer as timer
import functools
import itertools
import re
import json
from collections import Counter
import heapq

# 1) calculate chi2 of all terms per category
# tc = {term: {category: count}

# N … total num of lines                    = tc[i][j] for all terms i and categories j
# A … num of lines with term, in C          = tc[term][cat]
# B … num of lines with term, not in C      = sum(tc[term].values()) - A
# C … num of lines without term, in C       = sum(c[cat].get(term, 0) for c in tc.values())
# D … num of lines without term, not in C   = N - A - B - C

# mapper_init=self.init, <- share state with previous step


class ChiSquareJob(MRJob):

    # OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol  # get rid of quotes

    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def emit_term_category(self, _: None, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = [t.lower() for t in terms if t not in self.stopwords and len(t) > 1]

        for term in terms:
            yield term, category

    def emit_term_category_count(self, term: str, categories: list[str]):
        yield None, (term, Counter(categories))

    def gather_term_category_count(self, _, elems):
        term_category_counts = {}
        for term, category_count in elems:
            term_category_counts[term] = category_count

        yield None, term_category_counts

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.emit_term_category,
                reducer=self.emit_term_category_count
            ),
            MRStep(
                reducer=self.gather_term_category_count
            )
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
