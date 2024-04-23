# Our map-reduce function

from mrjob.job import MRJob

import preprocessing as pp


class MRChiSquared(MRJob):

    def mapper(self, _, line: dict):
        # TODO: Implement the mapper function
        category = line['category']
        reviewText = line['reviewText']
        yield category, reviewText  # 1 is to count the number of reviews in the category

    def combiner(self, category: str, reviewText: str):
        reviewText = pp.preprocess(reviewText)  # reviewText : list of tokens
        reviewText = set(reviewText)  # ignore duplicates
        yield category, (reviewText, 1)

    def reducer(self, category: str, reviewTexts: list): # reviewTexts : list of sets
        # TODO: Implement the reducer function

        # reviewTexts should be a set of tokens
        for token in reviewTexts[0]:
            yield category, ("token", token, 1)

        yield category, ("review_count", sum(reviewTexts[1])) # number of reviews in the category

    def reducer2(self, category, values):
        if values[0] == "review_count":
            total_reviews = values[1]
            yield category, ("review_count", total_reviews)

        elif values[0] == "token":
            token = values[1]
            # for this token, how many reviews in this category contain it
            yield token, ("cat_count", (category, values[2]))

    def reducer3(self, token, values):

        yield None

# Important: without these two lines, the job will not work
if __name__ == '__main__':
    MRChiSquared.run()

"""
chi-squared task explained:
 - for each line, extract reviewText and category
 - split reviewText into tokens
 - for each category c:
 --- for each token t:
 ----- count A number of reviews in category c which contain t
 ----- count B number of reviews not in category c which contain t
 ----- count C number of reviews in category c which do not contain t
 ----- count D number of reviews not in category c which do not contain t
 ----- calculate chi-squared value for t in c as (A*D - B*C)^2 / (A+B)(C+D)
How to structure the map-reduce job:
mapper:
    input < _ , line >
    reviewText = preprocess(reviewText)
    output < category , word for word in reviewText >




"""