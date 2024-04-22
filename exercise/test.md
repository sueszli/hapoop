Submission Deadline: **April 25, 2024 (23:59)**

You need to achieve a minimum of 35 points on every individual exercise.

The total number of points for the exercises is 100.

# File format

Json format:

-   **reviewerID** - `string` - the ID of the author of the review
-   **asin** - `string` - unique product identifier
-   **reviewerName** - `string` - name of the reviewer
-   **helpful** - `array of two integers [a,b]` - helpfulness rating of the review: `a` out of `b` customers found the review helpful
-   **reviewText** - `string` - the content of the review; this is the text to be processed
-   **overall** - `float` - rating given to product **asin** by reviewer **reviewerID**
-   **summary** - `string` - the title of the review
-   **unixReviewTime** - `integer` - timestamp of when review was created in UNIX format
-   **reviewTime** - `string` - date when review was created in human readable format
-   **category** - `string` - the category that the product belongs to

example

```
{"reviewerID": "A2VNYWOPJ13AFP", "asin": "0981850006", "reviewerName": "Amazon Customer \"carringt0n\"", "helpful": [6, 7], "reviewText": "This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons.  Yum!!", "overall": 5.0, "summary": "Delish", "unixReviewTime": 1259798400, "reviewTime": "12 3, 2009", "category": "Patio_Lawn_and_Garde"}
```

# Task

Use mrjob: https://mrjob.readthedocs.io/en/latest/

As a preparation step for text classification, we want to select terms
that discriminate well between categories. Write MapReduce jobs that
calculate chi-square values for the terms in the review dataset.

For preprocessing, make sure to perform the following steps:

-   **Tokenization** to unigrams, using whitespaces, tabs, digits, and
    the characters ()\[\]{}.!?,;:+=-\_\"\'\`\~#@&\*%€\$§\\/ as
    delimiters
-   **Case folding**
-   **Stopword filtering**: use the stop word list \[on TUWEL\]
    (stopwords.txt) . In addition, filter out all tokens consisting of
    only one character.

Write MapReduce jobs that efficiently

-   **Calculate chi-square values** for all unigram terms for each category
-   **Order the terms** according to their value per category and preserve the top 75 terms per category
-   **Merge the lists** over all categories

Produce a file `output.txt` from the development set that contains the following:

-   One line for each product category (categories in alphabetic
    order), that contains the top 75 most discriminative terms for
    the category according to the chi-square test in descending
    order, in the following format:
    `<category name> term_1st:chi^2_value term_2nd:chi^2_value ... term_75th:chi^2_value`
-   One line containing the merged dictionary (all terms
    space-separated and ordered alphabetically)

# Submission

`<groupID>_DIC2024_Ex1.zip` must contain:

-   `output.txt`: results obtained
-   `report.pdf`: written report
    -   max 8 pdf pages of A4 size in total
    -   section 1: introduction
    -   section 2: problem overview
    -   section 3: methodology and approach, must include a figure illustrating the strategy and pipeline in one figure. must show the data flow clearly and indicate the chosen `<key,value>` design (all input, intermediary, and output pairs).
    -   section 4: conclusions
-   `src/`: subdirectory with source code of MapReduce implementation + script to run all jobs in the correct order with all necessary parameters.
    -   use arguments to pass the hdfs input path and the local output path to the script because the paths will change when the code is run on the cluster.
    -   code must be correct, well documented, and efficient (you will have a runtime limit - the best times last term were <20 minutes).
