from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.sql import SQLContext, DataFrame, Column, Row
from pyspark.sql.types import *

from pyspark.sql.functions import UserDefinedFunction

import time

# KS,128,415,No,Yes,25,265.1,110,45.07,197.4,99,16.78,244.7,91,11.01,10.0,3,2.7,1,False

binaryMap = { 'Yes' : 1.0, 'No' : 0.0, 'True' : 1.0, 'False' : 0.0  }

stateSet1 = ['WA', 'WI', 'FL', 'WY', 'NH', 'NJ', 'TX', 'ND', 'TN', 'CA', 'NV', 'CO', 'AK', 'VT', 'GA', 'IA', 'MA', 'ME', 'OK', 'MO', 'MI', 'KS', 'MS', 'SC']
stateSet2 = ['KY', 'DE', 'DC', 'WV', 'HI', 'NM', 'LA', 'NC', 'NE', 'NY', 'PA', 'RI', 'VA', 'AL', 'AR', 'IL', 'IN', 'AZ','ID','CT','MD','OH','UT','MN','MT','OR', 'SD']

stateMap = { 1 : stateSet1, 2: stateSet2 }
toState = UserDefinedFunction( lambda state: filter( lambda (region,stateSet): state.upper() in stateSet, stateMap ).keys()[0], IntegerType() )
toNum = UserDefinedFunction( lambda k: binaryMap[k], DoubleType() )

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
        churn = line[19]        
	
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

conf = ( SparkConf().setAppName('Churn Rate Data Processing2') )
sc = SparkContext(conf = conf)

# sc.stop()

path = '../resources/churn-bigml-80.txt'
rdd1 = sc.textFile(path)

rddWithOutHeader = ( rdd1
                      .zipWithIndex()
                      .filter( lambda (key,index): index != 0 )
                      .map( lambda (key,index): makeSqlRow(key) )
                   )

#rddWithOutHeader.filter( lambda obj: obj.state == "NY" ).foreach(f)


def getIterableLength(x,y):
	lenOfGroup = len( y )
	return (x,lenOfGroup)


#rdd1 = (
#	rddWithOutHeader
#	 .groupBy( lambda x: (x.state,x.churn) )
#	 .map( lambda (x,y): getIterableLength(x,y) )
#         .sortBy( lambda (x,y): x )
#       )

#rdd2 = ( 
#	rddWithOutHeader
#	 .map( lambda x: ((x.state,x.churn),1) )
#  	 .reduceByKey( lambda x,y: x + y )
# 	 .sortBy( lambda (x,y): x )
#       )

sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(rddWithOutHeader)

#df2 = ( df
#	 .drop('areaCode')
#	 .drop('totalDayCharge')
#	 .drop('totalEveningCharge')
#	 .drop('totalNightCharge')
#	 .drop('totalInternationalCharge')
#         .withColumn('internationalPlan', toNum( df['internationalPlan'] ) )
#         .withColumn('voiceMailPlan', toNum( df['voiceMailPlan'] ) )
#         .withColumn('state', toState( df['state'] ) )
#         .withColumn('churn', toNum( df['churn'] ) )
#     )

df2  = ( 
	df
	.select( "internationalPlan", 
		  "voiceMailPlan", 
		  "state", 
	          "churn",
                  "acctLength", 
                  "numOfVmailMessages", 
                  "totalDayMinutes",
                  "totalDayCalls",
                  "customerServiceCalls",
		  "totalInternationalMinutes",
		  "totalEveningMinutes",
		  "totalNightMinutes",
		  "totalInternationalCalls")
         .withColumn('internationalPlan', toNum( df['internationalPlan'] ) )
         .withColumn('voiceMailPlan', toNum( df['voiceMailPlan'] ) )
         .withColumn('state', toState( df['state'] ) )
         .withColumn('churn', toNum( df['churn'] ) )
	)
	             
df2.show(5)
#sc.stop()
