from mrjob.job import MRJob
from mrjob.step import MRStep

import re
import json
import os
from pathlib import Path
from collections import Counter
import heapq
from sklearn.feature_extraction.text import CountVectorizer
from scipy.stats import chi2

# $ python3.11 ./src/run.py ./data/reviews_devset.json --stopwords ./data/stopwords.txt


class ChiSquareJob(MRJob):
    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def preprocessing_mapper(self, _, line):
        # tokenizing
        tokens = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', line)

        # case folding
        tokens = [token.lower() for token in tokens]

        # stopword filtering
        stopwords = None
        with open(self.options.stopwords, "r") as f:
            stopwords = set(line.strip() for line in f)
        assert stopwords is not None
        filtered_tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
        yield None, list(filtered_tokens)

    def preprocessing_reducer(self, _, tokens):
        # count tokens
        token_counts = Counter()
        for token_list in tokens:
            token_counts.update(token_list)
        yield None, token_counts

    # ------------------------

    #   def reducer(self, category, tokens_list):
    #     # Flatten tokens to create a corpus per category
    #     corpus = [token for tokens in tokens_list for token in tokens]

    #     # Vectorize for chi-square calculation
    #     vectorizer = CountVectorizer()
    #     X = vectorizer.fit_transform(corpus)
    #     term_freqs = X.toarray().sum(axis=0)

    #     # Calculate expected frequencies assuming uniform distribution
    #     n_docs = len(corpus)
    #     expected_freqs =  [n_docs / len(vectorizer.get_feature_names_out())] * len(term_freqs)

    #     # Compute chi-square values
    #     chi2_values = chi2.stat(term_freqs, expected_freqs)[0]

    #     # Build a list of (term, chi^2) tuples
    #     term_chi2 = list(zip(vectorizer.get_feature_names_out(), chi2_values))

    #     # Top 75 terms with highest chi-square
    #     top_terms = heapq.nlargest(75, term_chi2, key=lambda x: x[1])

    #     yield category, top_terms

    #   def merge_and_output(self, category, top_terms_list):
    #     # Build the output lines
    #     output_lines = []

    #     for category, top_terms in top_terms_list:
    #         line = f"{category} {' '.join(f'{term}:{chi2:.2f}' for term, chi2 in top_terms)}"
    #         output_lines.append(line)

    #     # Merge all terms, order, and create dictionary line
    #     all_terms = set(term for terms in top_terms_list for term, _ in terms)
    #     dictionary_line = ' '.join(sorted(all_terms))
    #     output_lines.append(dictionary_line)

    #     # Write the output to a file
    #     with open("output.txt", "w") as f:
    #         f.writelines('\n'.join(output_lines))

    # ------------------------

    # def chi_square_mapper(self, _, token_counts):
    #     # calculate chi-square values for each token
    #     total_tokens = sum(token_counts.values())
    #     chi_square_values = {token: (token_counts[token] * (total_tokens - token_counts[token])) / total_tokens for token in token_counts}
    #     yield None, chi_square_values

    # def chi_square_reducer(self, _, chi_square_values):
    #     # merge all chi-square values and keep top 75 per category
    #     all_chi_square_values = Counter()
    #     for values in chi_square_values:
    #         all_chi_square_values.update(values)
    #     # sort by chi-square value and keep top 75
    #     top_75_terms = all_chi_square_values.most_common(75)
    #     yield None, top_75_terms

    # def finalize(self):
    #     # merge lists over all categories and write to output.txt
    #     all_terms = []
    #     for _, terms in self.chi_square_reducer(None, None):
    #         all_terms.extend(terms)
    #     all_terms = sorted(all_terms, key=lambda x: x[1], reverse=True)
    #     with open("output.txt", "w") as f:
    #         for term, chi_square in all_terms:
    #             f.write(f"{term}\t{chi_square}\n")

    def steps(self):
        # fmt: off
        return [
            MRStep(mapper=self.preprocessing_mapper, reducer=self.preprocessing_reducer)
            # MRStep(mapper=self.chi_square_mapper, reducer=self.chi_square_reducer)
        ]
        # fmt: on


if __name__ == "__main__":
    ChiSquareJob.run()
