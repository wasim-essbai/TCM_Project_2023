import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, length

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



##### FROM FILES
tedx_dataset_path = "s3://unibg-tcm-tedx-pausaattiva/data/tedx_dataset.csv"
watch_next_dataset_path = "s3://unibg-tcm-tedx-pausaattiva/data/watch_next_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

### READ AND CLEAN TEDx DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

### FILTERING
count_items = tedx_dataset.count()
tedx_dataset_clean = tedx_dataset.filter("idx is not null")
count_items_null = tedx_dataset_clean.count()
tedx_dataset_clean = tedx_dataset_clean.filter(length(col('idx')) == 32)
count_correct_idx_length = tedx_dataset_clean.count()
tedx_dataset_clean = tedx_dataset_clean.filter(~col("idx").contains(" "))
count_no_space_id = tedx_dataset_clean.count()
tedx_dataset_clean = tedx_dataset_clean.dropDuplicates()
count_no_dup = tedx_dataset_clean.count()

print(f"Tedx Dataset: Number of items from RAW DATA: {count_items}")
print(f"Tedx Dataset: Number of items from RAW DATA with NOT NULL KEY: {count_items_null}")
print(f"Tedx Dataset: Number of items from RAW DATA with NO SPACES {count_no_space_id}")
print(f"Tedx Dataset: Number of items from RAW DATA with CORRECT IDX LENGTH {count_correct_idx_length}")
print(f"Tedx Dataset: Number of items from RAW DATA with NO DUPLICATES {count_no_dup}")


### READ AND CLEAN WATCH NEXT DATASET
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path)
    
## FILTERING
count_items = watch_next_dataset.count()
watch_next_dataset_clean = watch_next_dataset.filter("idx is not null")
count_items_null = watch_next_dataset_clean.count()
watch_next_dataset_clean = watch_next_dataset_clean.filter(length(col('idx')) == 32)
count_correct_idx_length = watch_next_dataset_clean.count()
watch_next_dataset_clean = watch_next_dataset_clean.filter(~col("idx").contains(" "))
count_no_space_id = watch_next_dataset_clean.count()
watch_next_dataset_clean = watch_next_dataset_clean.drop("url")
watch_next_dataset_clean = watch_next_dataset_clean.dropDuplicates()
count_no_dup = watch_next_dataset_clean.count()

print(f"Watch Next Dataset: Number of items from RAW DATA: {count_items}")
print(f"Watch Next Dataset: Number of items from RAW DATA with NOT NULL KEY: {count_items_null}")
print(f"Watch Next Dataset: Number of items from RAW DATA with NO SPACES {count_no_space_id}")
print(f"Watch Next Dataset: Number of items from RAW DATA with CORRECT IDX LENGTH {count_correct_idx_length}")
print(f"Watch Next Dataset: Number of items from RAW DATA with NO DUPLICATES {count_no_dup}")

### SAVE CLEAN DATASETS
#tedx_dataset_clean.coalesce(1).write.mode('overwrite').csv("s3://unibg-tcm-tedx-pausaattiva/data/tedx_dataset_clean")
#watch_next_dataset_clean.coalesce(1).write.mode('overwrite').csv("s3://unibg-tcm-tedx-pausaattiva/data/watch_next_dataset_clean")

tedx_dataset_clean.toPandas().to_csv("s3://unibg-tcm-tedx-pausaattiva/data/tedx_dataset_clean.csv",index=False)
tedx_dataset_clean.toPandas().to_csv("s3://unibg-tcm-tedx-pausaattiva/data/watch_next_dataset_clean.csv",index=False)