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
from multiprocessing import Value


# A … number of elems in category which contain term
# B … number of elems not in category which contain term
# C … number of elems in category without term
# D … number of elems not in category without term
# N … total number of retrieved elems (can be omitted for ranking)


class ChiSquareJob(MRJob):

    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init_mapper(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def mapper(self, _, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = [t.lower() for t in terms if t not in self.stopwords and len(t) > 1]

        tf = Counter(terms)  # {term: freq} of a review
        yield category, tf

    def init_reducer(self):
        self.num_lines = Value("i", 0)
        self.ctf = {}

    def reducer(self, category: str, tfs: list[Counter]):
        # increment counter
        with self.num_lines.get_lock():
            self.num_lines.value += 1

        # flatten
        cat_tf = Counter()
        for tf in tfs:
            cat_tf.update(tf)

        self.ctf[category] = cat_tf

    def final_reducer(self):
        # sort categories in alphabetic order
        self.ctf = dict(sorted(self.ctf.items(), key=lambda x: x[0]))

        # 1) calculate chi2 of all terms for each category
        yield None, self.num_lines

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init_mapper,
                mapper=self.mapper,
                reducer_init=self.init_reducer,
                reducer=self.reducer,
                reducer_final=self.final_reducer
            )
        ]
        # fmt: on


if __name__ == "__main__":
    start = timer()
    ChiSquareJob.run()
    end = timer()
    print(f"execution time: {end - start}s")
