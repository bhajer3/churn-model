from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.sql import SQLContext, DataFrame, Column, Row
from pyspark.sql.types import *
from pyspark.sql.functions import UserDefinedFunction
import time

def getRegionFromState(state): 
	region, _  = filter( lambda (region,stateSet): state.upper() in stateSet, stateMap.items() )[0]
	return region

def labelData(data): 
	data =  data.rdd.map( lambda row: LabeledPoint( row[0], row[1:] ) )
	return data

def f(x):
	print x

def makeSqlRow(line):
 	line = line.replace(" ","").split(",")

        state = line[0]
        acctLength = float( line[1] )
        areaCode = line[2]
        internationalPlan = line[3]
        voiceMailPlan = line[4]
        numOfVmailMessages = float( line[5] )
        totalDayMinutes = float( line[6])
        totalDayCalls = float( line[7] )
        totalDayCharge = float( line[8] )
        totalEveningMinutes = float( line[9] )
        totalEveningCalls = float( line[10] )
        totalEveningCharge = float( line[11] )
        totalNightMinutes = float(line[12] )
        totalNightCalls = float(line[13])
        totalNightCharge = float(line[14])
        totalInternationalMinutes = float(line[15])
        totalInternationalCalls = float(line[16])
        totalInternationalCharge = float(line[17])
        customerServiceCalls = float(line[18])
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
               customerServiceCalls =  customerServiceCalls,
               churn  =  churn
	)
        return obj

binaryMap = { 'Yes' : 1.0, 'No' : 0.0, 'True' : 1.0, 'False' : 0.0  }
state0Set = ['CT','MA','ME','NH','NJ','NY','PA','RI','VT'] # NorthEast
state1Set = ['IA','IL','IN','KS','MI','MN','MO','ND','NE','OH','SD','WI'] #MidWest
state2Set = ['AL','AR','DC','DE','FL','GA','KY','LA','MD','MS','NC','OK','SC','TN','TX','VA','WV'] #South
state3Set = ['AK','AZ','CA','CO','HI','ID','MT','NM','NV','OR','UT','WA','WY'] #West

stateMap = { 0.0 : state0Set, 1.0: state1Set, 2.0: state2Set, 3.0: state3Set }
toRegion = UserDefinedFunction(getRegionFromState, DoubleType() )
toNum = UserDefinedFunction( lambda k: binaryMap[k], DoubleType() )

path = '../resources/churn-bigml-80.txt'

SPARK_MASTER='local'
SPARK_APP_NAME='Churn Model'
conf = SparkConf().setMaster(SPARK_MASTER).setAppName(SPARK_APP_NAME).set( 'spark.driver.allowMultipleContexts' , "true")

sc = SparkContext.getOrCreate(conf = conf) 

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
	.select(  "churn",
		  "internationalPlan", 
		  "voiceMailPlan", 
		  "state", 
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
         .withColumn('region', toRegion( df['state'] ) )
         .withColumn('churn', toNum( df['churn'] ) )
	 .drop('state')
	)
df2.printSchema()
df2.show(5)


rddLabeledPoints = labelData(df2)
#rddLabeledPoints.foreach(f)

trainData,testData = rddLabeledPoints.randomSplit([0.80,0.20])

model = DecisionTree.trainClassifier(trainData,numClasses=2,maxDepth=2,categoricalFeaturesInfo={0:2,1:2,11:4}, impurity = 'gini', maxBins = 32)

from pyspark.mllib.evaluation import MulticlassMetrics

def getPredictionsLabels(model, testData):
    predictions = model.predict(testData.map(lambda r: r.features))
    return predictions.zip(testData.map(lambda r: r.label))

def printMetrics(predictions_and_labels):
    metrics = MulticlassMetrics(predictions_and_labels)
    print 'Precision of True ', metrics.precision(1)
    print 'Precision of False', metrics.precision(0)
    print 'Recall of True    ', metrics.recall(1)
    print 'Recall of False   ', metrics.recall(0)
    print 'F-1 Score         ', metrics.fMeasure()
    print 'Confusion Matrix\n', metrics.confusionMatrix().toArray()

predictionsAndLabels = getPredictionsLabels(model, testData)
printMetrics(predictionsAndLabels)
