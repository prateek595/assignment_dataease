#!/usr/bin/python3
from preRequireData import virginiaData
#1. How many LEED projects are there in Virginia (including all types of project types and versions of LEED)?

Virginia_data = virginiaData()

ques1 = Virginia_data.select("ID","ProjectName","LEEDSystemVersionDisplayName","ProjectTypes","State")
ques1.show(truncate=False)

print("\nSchema")
print(ques1.schema)