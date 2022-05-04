import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_database', 'lookup_name', 'tgt_bucket'])
p_src_database = args['src_database']
p_lookup_name = args['lookup_name']
p_src_table_name = "land_" + args['lookup_name']
p_tgt_bucket = args['tgt_bucket']
p_tgt_path = "s3://" + p_tgt_bucket + "/" + p_lookup_name

		
# Get sources types for attributes Code and Description
glue_client = boto3.client('glue')
glue_table = glue_client.get_table(DatabaseName = p_src_database, Name = p_src_table_name)
v_src_code_type = glue_table['Table']['StorageDescriptor']['Columns'][0]['Type']
v_src_desc_type = glue_table['Table']['StorageDescriptor']['Columns'][1]['Type']

#Spark, Glue, Init job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#purge terget locations older than 0 hour
#Attempting to purge S3 path with retention set to 0.
glueContext.purge_s3_path(s3_path = p_tgt_path, options={"retentionPeriod": 0} )

## @type: DataSource
## @args: [database = p_src_database, table_name = p_src_table_name, transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = p_src_database, table_name = p_src_table_name, transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("code", v_src_code_type, "code", "string"), ("description", v_src_desc_type, "description", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("code", v_src_code_type, "code", "string"), ("description", v_src_desc_type, "description", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": p_tgt_path}, format = "parquet", transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = resolvechoice2]
datasink3 = glueContext.write_dynamic_frame.from_options(frame = resolvechoice2, connection_type = "s3", connection_options = {"path": p_tgt_path}, format = "parquet", transformation_ctx = "datasink3")
job.commit()