Submission Deadline: **April 25, 2024 (23:59)**

You need at least 35/100 points to pass the exercise.

# running locally

to debug, use `assert True, "message"` to stop the program and print the message. printing to stdout will not be visible.

```bash
# python3.11 ./src/run.py ./data/reviews_devset.json --jobconf mapred.map.tasks=12 --jobconf mapred.reduce.tasks=12 --stopwords ./data/stopwords.txt > output.txt
python3.11 ./src/run.py ./data/reviews_devset.json --jobconf mapred.map.tasks=12 --jobconf mapred.reduce.tasks=12 --stopwords ./data/stopwords.txt > output.txt
```

# running on cluster

enable vpn, then open the following link: https://jupyter01.lbd.hpc.tuwien.ac.at/user/e11912007/lab?redirects=1

```bash
# dev dataset
python3 run.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json --stopwords stopwords.txt > output.txt

# full dataset
python3 run.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json --stopwords stopwords.txt > output.txt
```

# submission

`<groupID>_DIC2024_Ex1.zip` must contain:

-   `output.txt`: results obtained
-   `report.pdf`: written report

    -   max 8 pdf pages of A4 size in total
    -   section 1: introduction
    -   section 2: problem overview: chi squared measures significance of a term in a category. it can help with feature selection / dimensionality reduction in text classification.
    -   section 3: methodology and approach, must include a figure illustrating the strategy and pipeline in one figure must show the data flow clearly and indicate the chosen `<key,value>` design (all input, intermediary, and output pairs).
    -   section 4: conclusions

-   `src/`: subdirectory with source code of MapReduce implementation + script to run all jobs in the correct order with all necessary parameters.
    -   use arguments to pass the hdfs input path and the local output path to the script because the paths will change when the code is run on the cluster.
    -   code must be correct, well documented, and efficient (you will have a runtime limit - the best times last term were <20 minutes).

# task

-   try to make my solution faster
-   look at other dudes solution

-   implement based on assignment
-   check if you can generate `output.txt` on the server -> use `–jobconf` to increase parallelism: `–jobconf mapreduce.job.maps=50 –jobconf mapreduce.job.reduces=50`
-   maybe also ask pia for her solutions
-   write a report

-   ping people from tuwel that still have a free place in their group

```python
# how to solve?

def emit_term_category_count(self, term: str, categories: list[str]):
    cat_count = Counter(categories)  # term: {category: count}

    N = GLOBAL.total

    for category in categories:
        A = cat_count[category]  # t, c
        B = sum(cat_count.values()) - A  # t, ¬c
        C = GLOBAL.cat_total[category] - A  # ¬t, c
        D = N - A - B - C  # ¬t, ¬c
        chi_squared = (N * ((A * D - B * C) ** 2)) / ((A + B) * (A + C) * (B + D) * (C + D))
        yield category, (term, chi_squared)
```
