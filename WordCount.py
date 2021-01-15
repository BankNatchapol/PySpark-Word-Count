from pyspark import SparkContext, SparkConf
import json

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    lines = sc.textFile("in/sample-2mb-text-file.txt")
    
    words = lines.flatMap(lambda line: line.split(" "))
    
    wordCounts = words.countByValue()
    
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))

    with open('out/word_count.json', 'w') as fp:
        json.dump(wordCounts, fp)