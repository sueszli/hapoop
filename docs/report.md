# team unknown

everyone left lol - had to do all of this by myself

# pearson's chi square statistic

see: https://web.pdx.edu/~newsomj/pa551/lectur11.htm

$\chi^{2}$ test measures dependence between categorical stochastic variables.

the $\chi^{2}_{t,c}$ value is the lack of independence of term $t$ from category $c$.

$$
\chi_{tc}^2=\frac{N(AD-BC)^2}{(A+B)(A+C)(B+D)(C+D)}
$$

where:

-   $N$ = total number of retrieved documents (can be left out if you only care about ranking order, not scale)
-   $A$ = number of documents that are: in $c$, contain $t$
-   $B$ = number of documents that are: not in $c$, contain $t$
-   $C$ = number of documents that are: in $c$, don't contain $t$
-   $D$ = number of documents that are: not in $c$, don't contain $t$

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

# test in cluster

keep in mind that print statements will break mrjob. use `assert False, msg` instead.

```bash
# dev dataset

python3 run.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json --jobconf mapred.map.tasks=50 --jobconf mapred.reduce.tasks=50 --stopwords stopwords.txt > output.txt

# full dataset

python3 run.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json --jobconf mapred.map.tasks=50 --jobconf mapred.reduce.tasks=50 --stopwords stopwords.txt > output.txt
```
