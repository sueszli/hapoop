# In this file, the functions defined in the other
# files are called to execute the program.

# Custom libraries:
import preprocessing as pp
import MRchi_squared as cs

# External libraries:
import json

# read the reviews_devset.json file
reviews = json.load(open('reviews_devset.json'))

# TODO: preprocess the reviews

# TODO: execute the map-reduce job