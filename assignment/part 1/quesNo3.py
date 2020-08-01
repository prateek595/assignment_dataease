#!/usr/bin/python3
from preRequireData import virginiaData, combinedData
#3. What is the total Gross Square Feet of building space that is LEED-certified in Virginia?
combined = combinedData()
combined.select("GrossSqFoot").dtypes

from pyspark.sql.types import IntegerType

Virginia_data = virginiaData()
ques3 = Virginia_data.withColumn("GrossSqFoot", Virginia_data["GrossSqFoot"].cast(IntegerType())).groupBy("State").sum("GrossSqFoot")

ques3.show()

print("\nSchema")
print(ques3.schema)