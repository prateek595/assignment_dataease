from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Dataeaze") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    

nonConfidential = spark.read.csv(r"C:\Users\VIJAY PRATAP PANDEY\Desktop\Dataeaze\nonConfidential.csv")

#nonConfidential.take(3)

confidential = spark.read.parquet(r"C:\Users\VIJAY PRATAP PANDEY\Desktop\Dataeaze\confidential.snappy.parquet")
combined = confidential.unionAll(nonConfidential)

def combinedData():
    return combined

def virginiaData():
    virginia_data = combined.filter(combined.State=="VA")
    return virginia_data