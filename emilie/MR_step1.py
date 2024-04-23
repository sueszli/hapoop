"""
Step 1: count the number of reviews in each category, output it as a file

"""

from mrjob.job import MRJob

class MRCategoryCount(MRJob):

    def mapper(self, _, line: dict):
        category = line['category']
        yield category, 1

    def reducer(self, category, counts):
        # TODO: write to file

        yield category, sum(counts)
