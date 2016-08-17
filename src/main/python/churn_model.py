from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.sql import SQLContext, DataFrame, Column, Row
from pyspark.sql.types import *
import time

# KS,128,415,No,Yes,25,265.1,110,45.07,197.4,99,16.78,244.7,91,11.01,10.0,3,2.7,1,False

def f(x):
	print x

def makeSqlRow(line):
 	line = line.replace(" ","").split(",")

        state = line[0]
        acctLength = int( line[1] )
        areaCode = line[2]
        internationalPlan = line[3]
        voiceMailPlan = line[4]
        numOfVmailMessages = int( line[5] )
        totalDayMinutes = float( line[6])
        totalDayCalls = int( line[7] )
        totalDayCharge = float( line[8] )
        totalEveningMinutes = float( line[9] )
        totalEveningCalls = int( line[10] )
        totalEveningCharge = float( line[11] )
        totalNightMinutes = float(line[12] )
        totalNightCalls = int(line[13])
        totalNightCharge = float(line[14])
        totalInternationalMinutes = float(line[15])
        totalInternationalCalls = int(line[16])
        totalInternationalCharge = float(line[17])
        customerServiceCalls = int(line[18])
        churn = 1 if line[19] == "True" else 0        
	
	obj = Row(
               state =  state,
               acctLength =  acctLength,
               areaCode  =  areaCode,
               internationalPlan  =  internationalPlan,
               voiceMailPlan =  voiceMailPlan,
               numOfVmailMessages  =  numOfVmailMessages,
               totalDayMinutes  =  totalDayMinutes,
               totalDayCalls  =  totalDayCalls,
               totalDayCharge  =  totalDayCharge,
               totalEveningMinutes  =  totalEveningMinutes,
               totalEveningCalls  =  totalEveningCalls,
               totalEveningCharge =  totalEveningCharge,
               totalNightMinutes =  totalNightMinutes,
               totalNightCalls =  totalNightCalls,
               totalNightCharge =  totalNightCharge,
               totalInternationalMinutes =  totalInternationalMinutes,
               totalInternationalCalls =  totalInternationalCalls,
               totalInternationalCharge =  totalInternationalCharge,
               customerService =  customerServiceCalls,
               churn  =  churn
	)
        return obj
	
conf = ( SparkConf().setAppName('Churn Rate Data Processing') )
sc = SparkContext(conf = conf)

# sc.stop()

path = '../resources/churn-bigml-80.txt'
rdd1 = sc.textFile(path)

rddWithOutHeader = ( rdd1
                      .zipWithIndex()
                      .filter( lambda (key,index): index != 0 )
                      .map( lambda (key,index): makeSqlRow(key) )
                   )

#rddWithOutHeader.foreach(f)

#rddWithOutHeader.collect().foreach(f)


#rddWithOutHeader.filter( lambda obj: obj.state == "NY" ).foreach(f)

#for x in rddWithOutHeader.take(5):
#	print x

def getIterableLength(x,y):
	lenOfGroup = len( y )
	return (x,lenOfGroup)

block1Start = time.time()

#rddx = (
#	rddWithOutHeader
#	 .groupBy( lambda x: (x.state,x.churn) )
#	 .map( lambda (x,y): getIterableLength(x,y) )
#         .sortBy( lambda (x,y): x )
#       )

#for x in rddx.collect():
#	print x


#finalTime = time.time() - block1Start
#print finalTime

#block2Start = time.time()

rddy = ( 
	rddWithOutHeader
	 .map( lambda x: ((x.state,x.churn),1) )
  	 .reduceByKey( lambda x,y: x + y )
 	 .sortBy( lambda (x,y): x )
       )

#for y in rddy.collect():
#	print y

#finalTime2 = time.time() - block2Start
#print(finalTime2)

sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(rddWithOutHeader)
#df.show(5)

df.filter( "churn = 1" ).show(5)
#rddz = df.rdd

