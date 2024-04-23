"""
Step 1: count the number of reviews in each category, output it as a file

"""

# test attempt to calculate categories
from mrjob.job import MRJob
import json


class MRWordCount(MRJob):

    def mapper(self, _, line):
        # Parse the JSON object
        data = json.loads(line)
        # Extract the 'category' field
        category = data["category"].lower()
        yield category, 1
        yield "Total", 1  # TODO: change back to how it was without total?

    # A combiner isn't strictly necessary but improves performance:
    def combiner(self, category, counts):
        # Sum up the counts for each category
        yield category, sum(counts)

    def reducer(self, category, counts):
        # Sum up the counts for each word
        yield category, sum(counts)


if __name__ == "__main__":
    MRWordCount.run()

# run in the terminal python TestedVersion/Step1.py reviews_devset.json > TestedVersion/category_count.txt
