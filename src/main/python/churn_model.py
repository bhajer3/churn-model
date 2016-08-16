from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.sql import SQLContext, DataFrame, Column, Row
from pyspark.sql.types import *

# KS,128,415,No,Yes,25,265.1,110,45.07,197.4,99,16.78,244.7,91,11.01,10.0,3,2.7,1,False

def f(x):
	print x

conf = ( SparkConf().setAppName('Churn Rate Data Processing') )
sc = SparkContext(conf = conf)

path = '../resources/churn-bigml-80.txt'
rdd1 = sc.textFile(path)

rddWithOutHeader = ( rdd1
                      .zipWithIndex()
                      .filter( lambda (key,index): index != 0 )
                      .map( lambda (key,index): key )
                   )


rddWithOutHeader.foreach(f)
