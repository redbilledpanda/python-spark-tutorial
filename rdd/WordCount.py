from pyspark import SparkContext, SparkConf
import re

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    
    lines = sc.textFile("in/word_count.text")
    
    # create an RDD that contains all words frm the above text file
    # ignore punctuation 
    words = lines.flatMap(lambda line: re.findall(r'\b\w+\b', line))
    
    wordCounts = words.countByValue()
    
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))

