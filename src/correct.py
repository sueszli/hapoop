from collections import defaultdict

from mrjob.job import MRJob
from mrjob.step import MRStep
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import json
import re


class MRTermFrequencyByCategory(MRJob):
    def configure_args(self):
        super(MRTermFrequencyByCategory, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to the stopwords file")

    def mapper_init(self):
        self.stopwords = set()
        if self.options.stopwords:
            with open(self.options.stopwords, "r") as file:
                for line in file:
                    self.stopwords.add(line.strip())

    def tokenize(self, text):
        pattern = re.compile(r"[^\s\t\d\(\)\[\]\{\}\.!?,;:\+=\-_\"'`~#@&*%€$§\\/]+")
        tokens = pattern.findall(text)
        return set(tokens)

    def preprocess_review_text(self, review_text):
        review_text_lower = review_text.lower()
        tokens = self.tokenize(review_text_lower)
        return tokens - self.stopwords

    def mapper_extract_terms(self, _, line):
        review = json.loads(line)
        category = review["category"]
        reviewText = review["reviewText"]

        preprocessed_text = self.preprocess_review_text(reviewText)

        yield ("__category_count__", category), 1

        for term in preprocessed_text:
            yield (term, category), 1

    def reducer_aggregate_counts(self, key, values):
        yield None, (key, sum(values))

    def reducer_chi_square(self, _, counts):
        category_counts = defaultdict(int)
        cat_term_count = defaultdict(int)
        total_review_count = 0
        total_review_term_count = defaultdict(int)

        for key, count in counts:
            if key[0] == "__category_count__":
                category_counts[key[1]] = count
                total_review_count += count
            else:
                term, category = key
                cat_term_count[(term, category)] = count
                total_review_term_count[term] += count

        chi2_cat_term = {}
        for term, category in cat_term_count:
            A = cat_term_count[(term, category)]
            B = total_review_term_count[term] - A
            C = category_counts[category] - A
            D = total_review_count - (A + B + C)
            N = total_review_count

            numerator = N * (A * D - B * C) ** 2
            denominator = (A + B) * (A + C) * (B + D) * (C + D)
            chi_square = numerator / denominator if denominator != 0 else 0
            if category not in chi2_cat_term:
                chi2_cat_term[category] = {}
            chi2_cat_term[category][term] = chi_square

    def reducer_top(self, category, term_chi_values):
        sorted_terms = sorted(term_chi_values, key=lambda x: x[1], reverse=True)[:75]

        top_terms_str = ", ".join([term + ":" + str(round(chi)) for term, chi in sorted_terms])
        yield category, top_terms_str
        yield "__terms__", sorted_terms

    def reducer_final(self, key, category_values):
        if key != "__terms__":
            yield key, list(category_values)
        else:
            word_list = sorted(set([word for sublist in list(category_values) for word, _ in sublist]))
            yield "dict:", " ".join(word_list)

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_extract_terms,
                reducer=self.reducer_aggregate_counts,
            ),
            MRStep(reducer=self.reducer_chi_square),
            MRStep(mapper=self.reducer_top),
            MRStep(reducer=self.reducer_final),
        ]


if __name__ == "__main__":
    MRTermFrequencyByCategory.run()
