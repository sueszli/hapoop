if ! command -v python3 &> /dev/null; then echo "python3 missing"; exit 1; fi
if ! command -v pip3 &> /dev/null; then echo "pip3 missing"; exit 1; fi

python3 -m pip install --upgrade pip > /dev/null

pip3 install black > /dev/null
pip3 install pipreqs > /dev/null

rm -rf requirements.txt > /dev/null
pipreqs . > /dev/null
pip3 install -r requirements.txt > /dev/null


# run locally
# print statements won't work, use `assert True, f"{vars}"` to debug
time python3 \
    ./src/run.py ./data/reviews_devset.json \
    --jobconf mapred.map.tasks=12 --jobconf mapred.reduce.tasks=12 \
    --stopwords ./data/stopwords.txt



<< ////
# running in cluster:
# enable vpn, then open the following link: https://jupyter01.lbd.hpc.tuwien.ac.at/user/<matrikelnummer>/lab?redirects=1

# dev dataset
python3 run.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json --jobconf mapred.map.tasks=50 --jobconf mapred.reduce.tasks=50 --stopwords stopwords.txt > output.txt

# full dataset
python3 run.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json --jobconf mapred.map.tasks=50 --jobconf mapred.reduce.tasks=50 --stopwords stopwords.txt > output.txt
////
