import sys
import boto3
import botocore
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth
from awsglue.job import Job
from botocore.errorfactory import ClientError

args = getResolvedOptions(sys.argv,
                          ['input_bucket_name',
                           'output_bucket_name',
                           'database_name','JOB_NAME'])

input_bucket_name = args['input_bucket_name']
output_bucket_name = args['output_bucket_name']
database_name = args['database_name']


# Script generated for node S3 bucket

def CreateDynamicFrame(input_bucket_name, tablename):
    input_DyF = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3",
                                                              format="json", connection_options={
            "paths": ["s3://" + str(input_bucket_name) + "/Eloqua/" + str(tablename) + "/"], "recurse": True, },
                                                              transformation_ctx="input_DyF-"+tablename)
    input_df = input_DyF.toDF()
    return input_df


def SaveParquetDF(input_df, output_bucket_name, tablename):
    exploded_DF = input_df.withColumn('exploded', explode('data')).select('exploded.*')
    print("*********" + str(tablename) + "***********")
    exploded_DF.show()
    output_DF = exploded_DF.withColumn("Year", year(exploded_DF.exportDate)).withColumn("Month", month(
        exploded_DF.exportDate)).withColumn("Day", dayofmonth(exploded_DF.exportDate)).withColumn("BU_name",
                                                                                                  exploded_DF.businessUnitCode)
    output_DF.write.saveAsTable(str(tablename),
                                path="s3://" + str(output_bucket_name) + "/Eloqua" + "/" + str(tablename) + "/",
                                mode="append", compression="snappy", partitionBy=["BU_name", "Year", "Month", "Day"])


try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    sparksession = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    s3 = boto3.client('s3')
    sparksession.catalog.setCurrentDatabase(database_name)
    Table_names = ["Bounceback", "Campaign", "EmailClickthrough", "EmailOpen", "EmailSend", "PageView", "WebVisit",
                   "LeadScoring"]
    for x in Table_names:
        # s3.head_object(Bucket=input_bucket_name, Key="Eloqua/"+x+"/")
        result = s3.list_objects(Bucket=input_bucket_name, Prefix="Eloqua/" + x)
        if 'Contents' in result:
            print("************")
            input_df = CreateDynamicFrame(input_bucket_name, x)
            SaveParquetDF(input_df, output_bucket_name, x)
        else:
            print("Entity does not exist")
            # Something else has gone wrong.

    job.commit()

except Exception as error:
    raise error
