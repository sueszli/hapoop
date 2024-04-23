# File for all preprocessing functionality

import re
import os  # TODO: remove this import if not needed

def preprocess(text: str):
    delimiters = r"[ \t\d(){}\[\].!?,;:+=\-_\"\'`~#@&*%€$§\\/]"

    # case folding
    text = text.lower()

    #implement tokenization
    tokens = re.split(delimiters, text)

    #check tokens length
    tokens = [token for token in tokens if token and len(token) > 1]

    #Implement stopword filtering
    # try:
    # print("current directory: ", str(os.getcwd()))
    # TODO: change the path below to the path of the stopwords.txt file
    with open('src/stopwords.txt', 'r') as f:
        stopwords = f.read().split()
    # except FileNotFoundError:
      #   print("If step 2 is running from folder X, stopwords.txt should be in folder X")

    tokens = set(tokens) - set(stopwords)
    
    return tokens


#%% Testing

#if __name__ == "__main__":
#    print(preprocess("Hello, World! This is a test."))