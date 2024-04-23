from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol
import mrjob


from timeit import default_timer as timer
import functools
import itertools
import re
import json
from collections import Counter
import heapq


class ChiSquareJob(MRJob):

    # OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol  # get rid of quotes
    SORT_VALUES = True  # sort values

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
        terms = [t.lower() for t in terms if t not in self.stopwords and len(t) > 1]

        for term in terms:
            yield term, category

    def reducer(self, term: str, categories: list[str]):
        # N … total num of lines                     = tc[i][j] for all terms i and categories j
        # A … num of lines with term, in cat         = tc[term][cat]
        # B … num of lines with term, not in cat     = sum(tc[term][j] for j in tc[term].keys()) - A
        # C … num of lines without term, in cat      = sum(tc[i][cat] for i in tc.keys()) - A
        # D … num of lines without term, not in act  = N - A - B - C

        tc = Counter(categories)
        N = sum(tc.values())
        for cat, count in tc.items():
            A = count
            B = total - A
            C = sum(tc.values()) - A
            D = total - A - B - C

            nominator = total * (A * D - B * C) ** 2
            denominator = (A + B) * (A + C) * (B + D) * (C + D)
            chi2 = 0 if denominator == 0 else nominator / denominator

            yield None, (cat, term, chi2)

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.mapper,
                reducer=self.reducer,
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
