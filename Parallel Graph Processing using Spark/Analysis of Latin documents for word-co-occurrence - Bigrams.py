import pyspark
import re, itertools, os, sys
from subprocess import Popen,PIPE
from time import time
from csv import reader

lemma_dict = dict()
def cooccurrence(args):
    sc = pyspark.SparkContext()
    begin = time()
    data = sc.textFile(args[0])
    data = data.filter(lambda line : line!='')
    with open(args[2]) as lemmaFile:
        lemmas = reader(lemmaFile)
        for row in lemmas:
            lemma_dict[row[0]] = filter(None,row[1:])
    pairs = data.flatMap(line_to_pairs).reduceByKey(lambda a, b: a + b)
    pairs.saveAsTextFile(args[1])
    with open('run.txt','a') as f:
	f.write(args[1].split("/")[-1]+", Time: "+str(time() - begin)+"\n")

def line_to_pairs(line):
    metadata = re.compile("<(.*)>").search(line).group(0).encode('utf-8')
    line = re.match('<(.*)>(.+)',line).groups()[1].strip()
    tokens = filter(None,re.split(r'[^a-zA-Z0-9]+',line.encode('utf-8').lower().replace('j','i').replace('v','u')))
    pairs = [(pair,metadata) for i in range(len(tokens)) for j in range(len(tokens)) 
                 if tokens[i]!=tokens[j] for pair in lemmatizer(tokens[i],tokens[j]) if pair[0]!=pair[1]] 
    return pairs

def lemmatizer(word_one,word_two):
    lemma_word_one = lemma_dict[word_one] if word_one in lemma_dict else [word_one]
    lemma_word_two = lemma_dict[word_two] if word_two in lemma_dict else [word_two]
    return list(itertools.product(lemma_word_one,lemma_word_two))
        
if __name__ == "__main__":
    argv = sys.argv
    input_path = os.path.abspath(argv[1])
    output_path = os.path.abspath(argv[2])
    lemmatizer_path = os.path.abspath(argv[3])
    process1 = Popen(['ls',input_path],stdout = PIPE)
    process2 = Popen(['wc','-l'],stdin = process1.stdout, stdout = PIPE)
    process1.stdout.close()
    out,err = process2.communicate()
    if err is not None:
        raise IOError()
    nInputFiles = int(out)
    output_path += '/'+str(nInputFiles)
    cooccurrence([input_path,output_path,lemmatizer_path])
