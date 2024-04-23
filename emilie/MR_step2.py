"""
Step 2: calculate the chi-squared value for each token in each category

"""

from mrjob.job import MRJob
from collections import Counter
import json

import preprocessing as pp

class MRChiSquared(MRJob):

    def mapper(self, _, line: dict):
        category = line['category']
        reviewText = line['reviewText']
        reviewText = pp.preprocess(reviewText)  # reviewText : list of tokens
        reviewText = set(reviewText)  # ignore duplicates
        for token in reviewText:
            yield token, category

            
    def mapper2(self, token: str, categories: list):
        category_dict = Counter(categories)  # for each category, number of times this token appears
        token_counts = sum(category_dict.values())  # number of times this token appears in all categories
        # Assume there is a file with the number of reviews in each category
        # read the file:
        category_count = json.load(open('category_count.json')) # TODO: make sure this file exists
        N = sum(category_count.values())  # total number of reviews
        for category in category_dict:
            A = category_dict[category]
            B = token_counts - A
            C = category_count[category] - A
            D = N - A - B - C  # = N - token_counts - category_count[category] + A
            chi_squared = (N * ((A*D - B*C)**2) ) / ( (A+B)*(A+C)*(B+D)*(C+D) )
            yield category, (token, chi_squared)

    def reducer1(self, category: str, values: list):
        # sort the tokens by chi-squared value in decreasing order:
        ranked_tokens = sorted(values, key=lambda x: x[1], reverse=True)
        for token in ranked_tokens[:75]: # keep only the top 75 tokens
            yield None, token[0]  # merge the lists over all categories

    def reducer2(self, _, tokens: list):
        for token in set(tokens):  # remove duplicates
            yield token
