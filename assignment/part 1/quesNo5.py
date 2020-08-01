
from preRequireData import virginiaData, combinedData
#5. Is there a significant difference (use a t-test) in the points achieved for projects in Virginia compared to California for LEED NC 2.2?

from pyspark.sql.types import IntegerType
combined = combinedData()
Virginia_data = virginiaData()

ques5_cal = combined.filter("City=='California'" and "LEEDSystemVersionDisplayName =='LEED-NC 2.2'").withColumn("PointsAchieved", Virginia_data["PointsAchieved"].cast(IntegerType())).select("PointsAchieved").collect()
#print(ques5_cal.show())
#print(ques5_cal.schema)

ques5_virginia = Virginia_data.filter("LEEDSystemVersionDisplayName =='LEED-NC 2.2'").withColumn("PointsAchieved", Virginia_data["PointsAchieved"].cast(IntegerType())).select("PointsAchieved").collect()


import numpy as np
import scipy.stats as stats

cal = np.array(ques5_cal).flatten()

cal = cal[cal!=None]

vir = np.array(ques5_virginia).flatten()

print(stats.ttest_ind(a=cal,b=vir, equal_var=True))
