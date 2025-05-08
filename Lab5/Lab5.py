import sys
from pyspark import SparkContext, SparkConf

conf= SparkConf().setAppName("Lab5")
sc= SparkContext(conf=conf)

inputPath=sys.argv[1]
ouputPath=sys.argv[2]
prefix=sys.argv[3]

wordFrequenciesRDD=sc.textFile(inputPath)

#Task 1 

selectedLinesRDD=wordFrequenciesRDD.filter(lambda line: line.startswith(prefix))

numLines=selectedLinesRDD.count()
print("Number of selected lines: " + str(numLines))

maxfreqRDD=selectedLinesRDD.map(lambda line: int(line.split("\t")[1])) 
maxfreq=maxfreqRDD.reduce(lambda num1, num2: max(num1,num2))
print("Maximum frequency: " + str(maxfreq))

#Task 2

selectedLinesMaxFreqRDD=selectedLinesRDD.filter(lambda line: float(line.split("\t")[1])>0.8*maxfreq)
numLinesMaxfreq = selectedLinesMaxFreqRDD.count()
print("Number of selected lines with freq > 0.8*maxfreq: "+ str(numLinesMaxfreq))

selectedWordsRDD=selectedLinesMaxFreqRDD.map(lambda line: line.split("\t")[0])
selectedWordsRDD.saveAsTextFile(ouputPath)

sc.stop()