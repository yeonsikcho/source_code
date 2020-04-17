from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import numpy as np
import time #to compute running time

#Initialize Spark
spark = SparkSession.builder.appName('momentum').getOrCreate()

#log start time
start_time = time.time()

#read data
df = spark.read.csv("c:/users/choys1388/desktop/monthly_stockprices.csv", inferSchema = True, header = True)

#Compute Return
w = Window().partitionBy(F.col("stockid")).orderBy(F.col("tradedate"))
df = df.withColumn("return", F.col("adj_prc")/F.lag(F.col("adj_prc")).over(w))

#Compute past return
m = 9 #look back month
gap = 1 #short term reversal
df = df.withColumn("past_ret", F.lag(F.col("adj_prc"),gap+1).over(w)/F.lag(F.col("adj_prc"),m+gap+1).over(w))

#Keep only observations with enough past returns
df = df.where(df.past_ret.isNotNull())

#Custom Function to break into bins
numports = 10
def equal_bin(l, m): 
    N = np.array([n[1] for n in l])
    sep = (N.size/float(m))*np.arange(1,m+1)
    idx = sep.searchsorted(np.arange(N.size))
    bins = idx[N.argsort().argsort()]
    return [[l1[0], str(i1)] for l1,i1 in zip(l, bins)]
#Transform custom function to spark user defined function
equal_bin_udf = F.udf(lambda l: equal_bin(l,numports), ArrayType(StructType([StructField("stockid", StringType()), StructField("bin", StringType())])))

#Divide stocks into portfolio
df2 = df.groupBy("tradedate").agg(F.collect_list(F.struct(F.col("stockid"), F.col("past_ret"))).alias("collection")) #Collect gicode and past_ret
df3 = df2.withColumn("bins", equal_bin_udf(F.col("collection")))
df4 = df3.select("tradedate", F.explode("bins")).select("tradedate","col.stockid", "col.bin").orderBy(["stockid", "tradedate"])
merged_df = df.join(df4, on = ["tradedate","stockid"])

#Compute portfolio average return
monthly_port_ret = merged_df.groupBy(["tradedate", "bin"]).agg(F.mean(F.col("return")).alias("monthly_bin_ret"))
monthly_ret = monthly_port_ret.groupBy("bin").agg(F.mean(F.col("monthly_bin_ret")).alias("bin_ret")).orderBy("bin").show()
print('total running time',time.time() - start_time)