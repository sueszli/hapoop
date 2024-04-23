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


# A … number of lines in category which contain term
# B … number of lines not in category which contain term
# C … number of lines in category without term
# D … number of lines not in category without term
# N … total number of retrieved lines (can be omitted for ranking)


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

        for term in terms:
            yield term, category

    def reducer(self, term: str, categories: list[str]):
    

#     def reducer(self, token: str, categories: list):

#         category_dict = Counter(categories)  # for each category, number of times this token appears
#         token_counts = sum(category_dict.values())  # number of times this token appears in all categories


#         category_count = MRChiSquared.counts_categories # TODO: will this work on Hadoop? are class variables accessible to all noes?
#         N = category_count.get("Total") # total number of reviews

#         for category in category_dict:
#             A = category_dict[category]
#             B = token_counts - A
#             C = category_count[category] - A
#             D = N - A - B - C  # = N - token_counts - category_count[category] + A
#             chi_squared = (N * ((A*D - B*C)**2) ) / ( (A+B)*(A+C)*(B+D)*(C+D) )
#             yield category, (token, chi_squared)
# # 

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init_mapper,
                mapper=self.mapper,
                # reducer_init=self.reducer_init,
                reducer=self.reducer
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    start = timer()
    ChiSquareJob.run()
    end = timer()
    print(f"execution time: {end - start}s")
