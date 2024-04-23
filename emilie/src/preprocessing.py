# File for all preprocessing functionality

import re

def preprocess(text: str):
    delimiters = r"[ \t\d(){}\[\].!?,;:+=\-_\"\'`~#@&*%€$§\\/]"

    # case folding
    text = text.lower()

    #implement tokenization
    tokens = re.split(delimiters, text)

    #check tokens lenght
    tokens = [token for token in tokens if token and len(token) > 1]

    #Implement stopword filtering
    with open('stopwords.txt', 'r') as f:
        stopwords = f.read().split()
    
    tokens = set(tokens) - set(stopwords)
    
    return tokens


#%% Testing

#if __name__ == "__main__":
#    print(preprocess("Hello, World! This is a test."))