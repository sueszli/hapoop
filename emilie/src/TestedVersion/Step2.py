"""
Step 2: calculate the chi-squared value for each token in each category

"""
#Import all libraries 
from mrjob.job import MRJob
from collections import Counter
from mrjob.step import MRStep
import mrjob.protocol
import json
# from sortedcontainers import SortedSet  # sadly we're not allowed to use this module :(

import preprocessing as pp



class MRChiSquared(MRJob):

    # To make sure that our final output is only text without the quotation marks "" we
    # change the default output protocol from mrjob.protocol.JSONProtocol
    # to mrjob.protocol.RawValueProtocol:
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    # read the result of the first Mapreduce from the file and store it to the counts_categories variable
    with open('category_count.txt', 'r') as category_count:
            counts_categories = {}
            for c in category_count:
                counts_categories[c.split()[0].strip('"')] = int(c.split()[1])

    with open('src/stopwords.txt', 'r') as f:
        # define a class variable stopwords
        stopwords = f.read().split()  # stopwords is a list of stings

    # def __init__(self):
    #     super(MRChiSquared, self).__init__()
    #     with open('src/stopwords.txt', 'r') as f:
    #         # define a global variable stopwords
    #         # global stopwords
    #         self.stopwords = f.read().split()  # stopwords is a list of stings

    # define steps of the MapReduce
    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer=self.reducer1),
                MRStep(reducer=self.reducer2)
            ]

    # The first mapper function
    # Input: lines of the json file
    # Output: key(token) : value (category) pairs
    def mapper(self, _, line: dict):
         
        data = json.loads(line)
        category = data['category'].lower()
        reviewText = data['reviewText']

        reviewText = pp.preprocess(reviewText, MRChiSquared.stopwords)  # reviewText : set of tokens

        for token in reviewText:
            yield token, category

    # The first reducer function
    # Input: key(token) : value (category) pairs
    # Output: key(category) : value ((token, chi-square))
    def reducer(self, token: str, categories: list):

        category_dict = Counter(categories)  # for each category, number of times this token appears
        token_counts = sum(category_dict.values())  # number of times this token appears in all categories


        category_count = MRChiSquared.counts_categories # TODO: will this work on Hadoop? are class variables accessible to all noes?
        N = category_count.get("Total") # total number of reviews

        for category in category_dict:
            A = category_dict[category]
            B = token_counts - A
            C = category_count[category] - A
            D = N - A - B - C  # = N - token_counts - category_count[category] + A
            chi_squared = (N * ((A*D - B*C)**2) ) / ( (A+B)*(A+C)*(B+D)*(C+D) )
            yield category, (token, chi_squared)


    def reducer1(self, category: str, values: list):
        # sort the tokens by chi-squared value in decreasing order:
        ranked_tokens = sorted(values, key=lambda x: x[1], reverse=True)[0:75]
        yield None, (category, ranked_tokens)  # send all categories to one node in order to sort them
    
    def reducer2(self, _, tuples: list):

        # we wanted to use a sorted set to automatically remove duplicates and order all tokens
        # instead we use a python set and then sort it later
        final_dict = set()
        # each t corresponds to a category, we need to sort them in alphabetical order
        for t in sorted(tuples, key=lambda x: x[0]):  # t[0] is the category
            category_line = str(t[0])  # add the category name to the beginning of the line
            for token, chi_squared in t[1]:  # t[1] is the list of tuples of tokens and chi-squared values
                category_line += " " + str(token) + ":" + str(chi_squared)
                final_dict.add(str(token))
            yield "", category_line  # add the line to the output file (key is required but won't appear in the output)
        final_dict_str = ""
        for token in sorted(list(final_dict)):
            final_dict_str += token + " "
        yield "", final_dict_str  # add the merged dictionary as the last line the output file


if __name__ == '__main__':
    MRChiSquared.run()


# run in the terminal: python TestedVersion/Step2.py reviews_devset.json > TestedVersion/output2.txt
