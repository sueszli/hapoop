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

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.emit_term_category,
                reducer=self.emit_term_category_count
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
