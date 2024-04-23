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

        yield None, category

        for term in terms:
            yield term, category

    def reducer(self, key, values):
        if key is None:
            cats = list(values)
            total = len(cats)
            cat_count = Counter(cats)
            yield None, (total, cat_count)
        else:
            # handle normal key-value pairs
            pass

    # HOW DO I SHARE STATE BETWEEN MAPPER AND REDUCER?

    # def emit_term_category_count(self, term: str, categories: list[str]):
    #     cat_count = Counter(categories)  # term: {category: count}

    #     N = GLOBAL.total

    #     for category in categories:
    #         A = cat_count[category]  # t, c
    #         B = sum(cat_count.values()) - A  # t, ¬c
    #         C = GLOBAL.cat_total[category] - A  # ¬t, c
    #         D = N - A - B - C  # ¬t, ¬c
    #         chi_squared = (N * ((A * D - B * C) ** 2)) / ((A + B) * (A + C) * (B + D) * (C + D))
    #         yield category, (term, chi_squared)

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
