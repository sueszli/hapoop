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

    # INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    # OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol
    # SORT_VALUES = True  # sort values

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

        tc = {}  # {term: {category: count}}
        for term in terms:
            if term not in tc:
                tc[term] = Counter()
            tc[term][category] += 1
        yield None, tc

    def reducer(self, _, tcs: list[Counter]):
        tc = {}  # {term: {category: count}}
        for elem in tcs:
            for term, cat_count in elem.items():
                if term not in tc:
                    tc[term] = Counter()
                tc[term].update(cat_count)

        # 1) calculate chi2 of all categories per term

        # N … total num of lines                     = tc[i][j] for all terms i and categories j
        # A … num of lines with term, in cat         = tc[term][cat]
        # B … num of lines with term, not in cat     = sum(tc[term][j] for j in tc[term].keys()) - A
        # C … num of lines without term, in cat      = sum(tc[i][cat] for i in tc.keys()) - A
        # D … num of lines without term, not in act  = N - A - B - C

        chi2_cat_term = {}
        N = sum(tc[i][j] for i in tc.keys() for j in tc[i].keys())
        for term, cat_count in tc.items():
            for cat, count in cat_count.items():
                A = count
                B = sum(tc[term].values()) - A
                C = sum(tc[i].get(cat, 0) for i in tc.keys()) - A
                D = N - A - B - C

                nominator = N * (A * D - B * C) ** 2
                denominator = (A + B) * (A + C) * (B + D) * (C + D)
                chi2 = 0 if denominator == 0 else nominator / denominator

                if cat not in chi2_cat_term:
                    chi2_cat_term[cat] = {}
                chi2_cat_term[cat][term] = chi2

        yield None, chi2_cat_term

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
