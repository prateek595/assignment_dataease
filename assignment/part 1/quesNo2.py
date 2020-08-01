#!/usr/bin/python3
from preRequireData import virginiaData
#2. What is the number of LEED projects in Virginia by owner type? 

Virginia_data = virginiaData()

ques2 = Virginia_data.groupBy("OwnerTypes").count()
ques2.show(truncate=False)

print("\nSchema")
print(ques2.schema)
