import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713854823544 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="cleaned_statistic_reference_data", transformation_ctx="AWSGlueDataCatalog_node1713854823544")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713854860681 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1713854860681")

# Script generated for node Join
Join_node1713855002119 = Join.apply(frame1=AWSGlueDataCatalog_node1713854823544, frame2=AWSGlueDataCatalog_node1713854860681, keys1=["id"], keys2=["category_id"], transformation_ctx="Join_node1713855002119")

# Script generated for node Amazon S3
AmazonS3_node1713855246973 = glueContext.getSink(path="s3://dsci6007prjt1-analytics", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1713855246973")
AmazonS3_node1713855246973.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1713855246973.setFormat("glueparquet", compression="snappy")
AmazonS3_node1713855246973.writeFrame(Join_node1713855002119)
job.commit()
