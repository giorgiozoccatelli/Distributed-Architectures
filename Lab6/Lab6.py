import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Lab6")
sc = SparkContext(conf=conf)

# Set input and output folders 
inputPath = "/data/students/bigdata-01QYD/Lab6_DBD/Reviews.csv"
outputPath = "Lab6/res_Lab_6"

# Read input data
listAmazonRDD = sc.textFile(inputPath)

# Discard the header
listAmazonRDD_noHeader = listAmazonRDD.filter(lambda line : line.startswith("Id,")==False)

# Create new RDD
def extractUserID_ProductIDs(line):
    columns = line.split(",")
    userID = columns[2]
    productID = columns[1]
    
    return (userID, productID)

UserID_ProductID_RDD = listAmazonRDD_noHeader.map(extractUserID_ProductIDs)
UserID_ProductID_distinctRDD = UserID_ProductID_RDD.distinct()
UserID_ProductID_ProductListRDD = UserID_ProductID_distinctRDD.groupByKey()

ProductID_ValuesRDD = UserID_ProductID_ProductListRDD.values()

def extractPairsOfProducts(transaction):

    products = list(transaction)

    returnedPairs = []
    
    for product1 in products:
        for product2 in products:
            if product1<product2:
                returnedPairs.append(((product1, product2), 1))
                
    return returnedPairs

PairsOfProductsRDD = ProductID_ValuesRDD.flatMap(extractPairsOfProducts)
PairsFrequenciesRDD = PairsOfProductsRDD.reduceByKey(lambda count1, count2: count1 + count2)
atLeast2PairsFrequenciesRDD = PairsFrequenciesRDD.filter(lambda inputTuple: inputTuple[1]>1)
atLeast2PairsFrequenciesSortedRDD = atLeast2PairsFrequenciesRDD.sortBy(lambda inputTuple: inputTuple[1], False)
atLeast2PairsFrequenciesSortedRDD.saveAsTextFile(outputPath)

sc.stop()