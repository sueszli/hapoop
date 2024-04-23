from collections import defaultdict

from mrjob.job import MRJob
from mrjob.step import MRStep
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import json
import re


# Define the MRJob class
class MRTermFrequencyByCategory(MRJob):
    def configure_args(self):
        super(MRTermFrequencyByCategory, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to the stopwords file")

    # Load stopwords from file
    def mapper_init(self):
        # Load stopwords from file
        self.stopwords = set()
        if self.options.stopwords:
            with open(self.options.stopwords, "r") as file:
                for line in file:
                    self.stopwords.add(line.strip())

    # Tokenize text using regex
    def tokenize(self, text):
        # Define a regex pattern to match sequences that are not the specified delimiters
        pattern = re.compile(r"[^\s\t\d\(\)\[\]\{\}\.!?,;:\+=\-_\"'`~#@&*%€$§\\/]+")
        tokens = pattern.findall(text)
        return set(tokens)

    # Preprocess review text by converting to lowercase, tokenizing, and removing stopwords
    def preprocess_review_text(self, review_text):
        # Convert to lowercase and tokenize
        review_text_lower = review_text.lower()
        tokens = self.tokenize(review_text_lower)

        # Remove stopwords
        return tokens - self.stopwords

    # Extract terms and category from review
    # Input: review JSON object
    # Output key value pairs:
    #      - (term, category), 1
    #      - ('__category_count__', category), 1
    def mapper_extract_terms(self, _, line):
        review = json.loads(line)
        category = review["category"]
        reviewText = review["reviewText"]

        preprocessed_text = self.preprocess_review_text(reviewText)

        # Emit category count
        yield ("__category_count__", category), 1

        # Emit term-category count
        for term in preprocessed_text:
            yield (term, category), 1

    # Aggregate counts in reducer
    # Input:
    #       - (term, category), [1, 1, 1, ...]
    #       - ('__category_count__', category), [1, 1, 1, ...]
    # Output:
    #       - None, ((term, category), count)
    #       - None, (('__category_count__', category), count)
    def reducer_aggregate_counts(self, key, values):
        # Aggregate counts in reducer
        yield None, (key, sum(values))

    # Calculate chi-square values
    # Input:
    #       - None, ((term, category), count)
    #       - None, (('__category_count__', category), count)
    # Output:
    #       - category, (term, chi-square)
    def reducer_chi_square(self, _, counts):
        category_counts = defaultdict(int)
        term_counts = defaultdict(int)
        total_review_count = 0
        total_review_term_count = defaultdict(int)

        for key, count in counts:
            if key[0] == "__category_count__":
                category_counts[key[1]] = count
                total_review_count += count
            else:
                term, category = key
                term_counts[(term, category)] = count
                total_review_term_count[term] += count

        for term, category in term_counts:
            A = term_counts[(term, category)]
            B = total_review_term_count[term] - A
            C = category_counts[category] - A
            D = total_review_count - (A + B + C)
            N = total_review_count

            # Calculate chi-square without redundant calculations
            numerator = N * (A * D - B * C) ** 2
            denominator = (A + B) * (A + C) * (B + D) * (C + D)
            chi_square = numerator / denominator if denominator != 0 else 0

            yield category, (term, chi_square)

    # Select top 75 terms for each category
    # Input:
    #       - category, (term, chi-square value)
    # Output:
    #       - category, string representation of top 75 terms
    #       - '__terms__', (term, chi-square value)
    def reducer_top(self, category, term_chi_values):
        # term_chi_values is an iterable of (term, chi_square_value) tuples
        # Sort by chi-square value in descending order and select the top 75
        sorted_terms = sorted(term_chi_values, key=lambda x: x[1], reverse=True)[:75]

        # Construct the output string
        top_terms_str = ", ".join([term + ":" + str(round(chi)) for term, chi in sorted_terms])
        yield category, top_terms_str
        yield "__terms__", sorted_terms

    # Final reducer to output the top 75 terms for each category and the dictionary
    # Input:
    #       - category, string representation of top 75 terms
    #       - '__terms__', (term, chi-square value)
    # Output:
    #       - category, top 75 terms
    #       - 'dict:', 'word1 word2 word3 ...'
    def reducer_final(self, key, category_values):
        if key != "__terms__":
            yield key, list(category_values)
        else:
            word_list = sorted(set([word for sublist in list(category_values) for word, _ in sublist]))
            yield "dict:", " ".join(word_list)

    # Define the steps for the MRJob

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


# Run the MRJob
if __name__ == "__main__":
    MRTermFrequencyByCategory.run()
