Submission Deadline: **April 25, 2024 (23:59)**

You need at least 35/100 points to pass the exercise.

# Task

Use mapreduce with MRjob.

Preprocessing line\[reviewText]:

-   **Tokenization** to unigrams, using whitespaces, tabs, digits, and the characters ()\[\]{}.!?,;:+=-\_\"\'\`\~#@&\*%€\$§\\/ as delimiters
-   **Case folding**
-   **Stopword filtering**: use `stopwords.txt`. In addition, filter out all tokens consisting of only one character.

Calculate the chi-squared value for each term in each category.

Write MapReduce jobs that efficiently

-   **Calculate chi-square values** for all unigram terms for each category
-   **Order the terms** according to their value per category and preserve the top 75 terms per category
-   **Merge the lists** over all categories

Produce a file `output.txt` from the development set that contains the following:

-   One line for each product category (categories in alphabetic order), that contains the top 75 most discriminative terms for the category according to the chi-square test in descending order, in the following format:

    `<category name> term_1st:chi^2_value term_2nd:chi^2_value ... term_75th:chi^2_value`

-   One line containing the merged dictionary (all terms space-separated and ordered alphabetically)

# Submission

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

Run tests on: https://jupyter01.lbd.hpc.tuwien.ac.at/user/e11912007/lab?redirects=1
