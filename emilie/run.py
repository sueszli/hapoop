#  Wraps the two map-reduce jobs into one file

from Step1 import MRWordCount
from Step2 import MRChiSquared

# Step 1
# count the number of reviews in each category, output it as a file

step1 = MRWordCount(args=["reviews_devset.json"])
with step1.make_runner() as runner:
    runner.run()
    # TODO: save the output to a file called category_count.txt
    with open("category_count.txt", "w") as file:
        for line in runner.stream_output():
            key, value = step1.parse_output_line(line)
            file.write(f"{key}\t{value}\n")

# Step 2
# calculate the chi-squared values for each token in each category

step2 = MRChiSquared(args=["reviews_devset.json"])
with step2.make_runner() as runner:
    runner.run()
