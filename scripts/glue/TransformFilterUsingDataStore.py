import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="weather-db", table_name="filtered", transformation_ctx="S3bucket_node1"
)

# Script generated for node Apply Mapping
ApplyMapping_node1653566410410 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("rain", "boolean", "rain", "boolean"),
        ("temp", "double", "temp", "double"),
        ("year", "int", "year", "smallint"),
        ("month", "int", "month", "smallint"),
        ("day", "int", "day", "smallint"),
    ],
    transformation_ctx="ApplyMapping_node1653566410410",
)

# Script generated for node SQL
SqlQuery16 = """
select year, date_part('week', CAST(CONCAT(CAST(year AS VARCHAR(4)), '-' ,
        RIGHT(CONCAT('00', CAST(month AS VARCHAR(2))), 2), '-' ,
        RIGHT(CONCAT('00', CAST(day AS VARCHAR(2))),2)) as date)) as week, 
        sum(CAST(rain as int)) as rain_days, 
        max(temp) as max_temp from weather 
group by year, week having rain_days > 0
"""
SQL_node1653566466912 = sparkSqlQuery(
    glueContext,
    query=SqlQuery16,
    mapping={"weather": ApplyMapping_node1653566410410},
    transformation_ctx="SQL_node1653566466912",
)

# Script generated for node Amazon S3
AmazonS3_node1653566486762 = glueContext.write_dynamic_frame.from_options(
    frame=SQL_node1653566466912,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://data-store-sundeep/processed/weather/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1653566486762",
)

job.commit()
