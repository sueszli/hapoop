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


class ChiSquareJob(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol  # get rid of quotes

    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")

    def init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)

    def get_line_termfreq(self, _: None, line: str):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]

        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = [t.lower() for t in terms if t not in self.stopwords and len(t) > 1]

        termfreq = Counter(terms)  # {term: freq} of a review
        yield category, termfreq

    def get_category_termfreq(self, category: str, termfreq: list[Counter]):
        cat_termfreq = Counter()  # flatmap
        for tf in termfreq:
            cat_termfreq.update(tf)
        yield None, {category: cat_termfreq}  # send to a single reducer

    def get_chi2(self, _, cat_termfreqs: list[dict[str, Counter]]):
        ctf: dict[str, Counter] = {}  # {category: {term: freq}}
        for ct in cat_termfreqs:
            ctf.update(ct)

        # sort categories in alphabetic order
        ctf = dict(sorted(ctf.items(), key=lambda x: x[0]))

        # 1) calculate chi2 of all terms for each category
        chi2_values = {}  # {category: {term: chi2}}
        total_in_all = sum(sum(tf.values()) for tf in ctf.values())

        for cat in list(ctf.keys()):
            chi2_values[cat] = {}
            total_in_cat = sum(ctf[cat].values())
            for term, freq in ctf[cat].items():
                total_of_term = sum(ctf[cat].get(term, 0) for cat in ctf.keys())

                observed = freq
                expected = total_of_term * total_in_cat / total_in_all
                chi2_values[cat][term] = (observed - expected) ** 2 / expected

        # 2) filter by top 75 highest chi2 values, sorted in ascending order
        for cat, terms in chi2_values.items():
            chi2_values[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda x: x[1]))

            # discard if no terms
            if not chi2_values[cat]:
                del chi2_values[cat]

        # 4) yield results
        # <category name> term1:chi2 term2:chi2 ... term75:chi2
        for cat, terms in chi2_values.items():
            yield None, str(cat) + " " + " ".join(f"{term}:{chi2}" for term, chi2 in terms.items())
        # merged dictionary (all terms space-separated and ordered alphabetically)
        yield None, " ".join(sorted(list(itertools.chain.from_iterable(chi2_values.values()))))

    def steps(self):
        # fmt: off
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.get_line_termfreq,
                reducer=self.get_category_termfreq
            ),
            MRStep(
                reducer=self.get_chi2
            )
        ]
        # fmt: on


if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    delta = t2 - t1
    print(f"runtime: {delta:.4f} seconds")
