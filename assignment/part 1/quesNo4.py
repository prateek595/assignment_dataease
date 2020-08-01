#!/usr/bin/python3
from preRequireData import virginiaData
#4. What Zip Code in Virginia has the highest number of projects? 
import pyspark.sql.functions as f
Virginia_data  = virginiaData()
ques4 = Virginia_data.groupBy("Zipcode").agg(f.count("ProjectName").alias("No_Of_Project"))

ques4 = ques4.groupBy("Zipcode").max("No_Of_Project")
ques4.show()
print("\nSchema")
print(ques4.schema)