from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol
import mrjob

import logging
from timeit import default_timer as timer
import functools
import itertools
import re
import json
from collections import Counter, defaultdict
import heapq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChiSquareJob(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

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
        terms = set(terms) - self.stopwords
        terms = [t.lower() for t in terms if len(t) > 1]

        for term in terms:
            yield term, category

    def combiner(self, term: str, categories: list[str]):
        yield None, (term, Counter(categories))  # {term: {category: count}}

    def reducer(self, _: None, t_c_list: list[Counter]):
        logger.info(f"reached final reducer")
        t_c_list = list(t_c_list)  # [term, {category: count}]

        # can't merge two loops because we need to calculate N first
        N = 0  # total count
        term_count = defaultdict(int)  # {term: total count}
        cat_count = defaultdict(int)  # {category: total count}
        for term, cat_count in t_c_list:
            for cat, count in cat_count.items():
                N += count
                term_count[term] += count
                cat_count[cat] += count

        # 1) calculate chi2 of all terms for each category
        chi2_cat_term = {}
        for term, cat_count in t_c_list:
            for cat, count in cat_count.items():
                A = count
                B = term_count[term] - A
                C = cat_count[cat] - A
                D = N - A - B - C

                chi2 = (N * ((A * D - B * C) ** 2)) / ((A + B) * (A + C) * (B + D) * (C + D))
                if cat not in chi2_cat_term:
                    chi2_cat_term[cat] = {}
                chi2_cat_term[cat][term] = chi2

        # 2) sort chi2_cat_term by category
        chi2_cat_term = dict(sorted(chi2_cat_term.items(), key=lambda x: x[0]))

        # 3) filter by top 75 highest chi2 values, sorted in ascending order
        for cat, terms in chi2_cat_term.items():
            chi2_cat_term[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda x: x[1]))

            # discard if no terms
            if not chi2_cat_term[cat]:
                del chi2_cat_term[cat]

        # 4) yield results
        # <category name> term1:chi2 term2:chi2 ... term75:chi2
        for cat, terms in chi2_cat_term.items():
            yield None, str(cat) + " " + " ".join(f"{term}:{chi2}" for term, chi2 in terms.items())
        # merged dictionary (all terms space-separated and ordered alphabetically)
        yield None, " ".join(sorted(list(itertools.chain.from_iterable(chi2_cat_term.values()))))

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
            ),
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    # print(f"runtime: {delta:.4f} seconds")
