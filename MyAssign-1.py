# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Row
import datetime
spark = SparkSession.builder.appName('consec').getOrCreate()
sc= spark.sparkContext

# COMMAND ----------

#Created a number of list to test all 5 scenarios
tab1=[(1,2,3,'2010-01-01','2010-12-31'),(1,2,3,'2011-01-01','2011-12-30'),(1,2,3,'2012-01-01','9999-12-31')]
# tab1=[(1,2,3,'2010-01-01','2012-12-30'),(1,2,3,'2012-01-01','9999-12-31')]
# tab1=[(1,2,3,'2010-01-01','2011-12-30'),(1,2,3,'2012-01-01','9999-12-31')]
# tab1=[(1,2,3,'2010-01-01','2010-12-31'),(1,2,3,'2011-01-01','2011-12-30'),(1,2,3,'2012-01-01','9999-12-31')]

# COMMAND ----------

schema_col = ['col1','col2','col3','startend','enddate']

df1 = spark.createDataFrame(data=tab1, schema = schema_col)
df1.show()

df1 = df1.withColumn('startend',f.to_date('startend')).withColumn('enddate',f.to_date('enddate'))

len(df1.columns)

# COMMAND ----------

from pyspark.sql.functions import when

windowSpecAgg  = Window.partitionBy("col1","col1","col3").orderBy('startend','enddate')

df2 = df1.withColumn("isconsec", when(f.date_add(df1.enddate,1)==lead(df1.startend,1).over(windowSpecAgg),"Yes").otherwise("No")).show()



# df2 = df1.withColumn("end_date",lead('enddate').over(windowSpecAgg))
# df2.show()
# # df2.na.drop().select('col1','col2','col3','startend','end_date').show()
# # df2.filter(df2('end_date').isNull)

# COMMAND ----------

# df1.printSchema()
# # df1.groupBy('col1','col2','col3').count().show()

from pyspark.sql.functions import lag, lead, first, last, desc
from pyspark.sql.window import Window
windowSpec = Window.partitionBy('col1','col2','col3').orderBy('startend','enddate')
df2 = df1.withColumn('start_date', lag('startend').over(windowSpec)).withColumn('end_date', lead('enddate').over(windowSpec)).drop('startend','enddate')


df2.show()
