if ! command -v python3 &> /dev/null; then echo "python3 missing"; exit 1; fi
if ! command -v pip3 &> /dev/null; then echo "pip3 missing"; exit 1; fi

python3 -m pip install --upgrade pip > /dev/null

pip3 install black > /dev/null
pip3 install pipreqs > /dev/null

rm -rf requirements.txt > /dev/null
pipreqs . > /dev/null
pip3 install -r requirements.txt > /dev/null

# run locally
time python3 \
    ./src/run.py ./data/reviews_devset.json \
    --jobconf mapred.map.tasks=12 --jobconf mapred.reduce.tasks=12 \
    --stopwords ./data/stopwords.txt
