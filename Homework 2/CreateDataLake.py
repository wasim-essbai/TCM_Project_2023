###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://unibg-tcm-tedx-pausaattiva/data/tedx_dataset_clean.csv"
tags_dataset_path = "s3://unibg-tcm-tedx-pausaattiva/data/tags_dataset.csv"
watch_next_dataset_path = "s3://unibg-tcm-tedx-pausaattiva/data/watch_next_dataset_clean.csv"
num_likes_dataset_path = "s3://unibg-tcm-tedx-pausaattiva/data/num_likes_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", "true") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

## READ WATCH NEXT DATASET
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path)

## READ NUMBER OF LIKES DATASET
num_likes_dataset = spark.read.option("header","true").csv(num_likes_dataset_path)


# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()

tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()

# UPDATE THE AGGREGATE MODEL, ADD WATCH NEXT TO TEDX_DATASET
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("watch_next_idx").alias("watch_next"))
watch_next_dataset_agg.printSchema()

tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg._id == watch_next_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \

tedx_dataset_agg.printSchema()

# UPDATE THE AGGREGATE MODEL, ADD NUMBER OF LIKES TO TEDX_DATASET
num_likes_dataset = num_likes_dataset.withColumnRenamed("idx", "idx_ref")
num_likes_dataset.printSchema()

tedx_dataset_agg = tedx_dataset_agg.join(num_likes_dataset, tedx_dataset_agg._id == num_likes_dataset.idx_ref, "left") \
    .drop("idx_ref") \

tedx_dataset_agg.printSchema()


### SAVE THE AGGREGATE MODEL IN MONGODB

mongo_uri = "mongodb+srv://admin:admin123@cluster1-unibg-tcm-tedx.k0eo6ed.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "tedx_pausaattiva",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
    
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)