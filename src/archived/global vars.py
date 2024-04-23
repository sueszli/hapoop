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

        yield None, category  # --> channel 1

        for term in terms:
            yield term, category  # --> channel 2

    def reducer(self, key, values):
        if key is None:  # --> handle channel 1
            cats = list(values)
            total = len(cats)
            cat_count = Counter(cats)
            yield None, (total, cat_count)
        else:  # --> handle channel 2
            pass

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