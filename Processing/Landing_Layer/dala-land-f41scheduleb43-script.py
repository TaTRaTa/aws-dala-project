import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_database', 'src_main_name', 'tgt_bucket'])
p_src_database = args['src_database']
p_main_src_name = args['src_main_name']
p_src_table_name = "land_" + args['src_main_name']
p_tgt_bucket = args['tgt_bucket']
p_tgt_path = "s3://" + p_tgt_bucket + "/" + p_main_src_name

		
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
## @args: [mapping = [("year", "long", "year", "string"), ("carrier", "string", "carrier", "string"), ("carrier_name", "string", "carrier_name", "string"), ("manufacture_year", "long", "manufacture_year", "string"), ("unique_carrier_name", "string", "unique_carrier_name", "string"), ("serial_number", "string", "serial_number", "string"), ("tail_number", "string", "tail_number", "string"), ("aircraft_status", "string", "aircraft_status", "string"), ("operating_status", "string", "operating_status", "string"), ("number_of_seats", "long", "number_of_seats", "string"), ("manufacturer", "string", "manufacturer", "string"), ("aircraft_type", "long", "aircraft_type", "string"), ("model", "string", "model", "string"), ("capacity_in_pounds", "long", "capacity_in_pounds", "string"), ("acquisition_date", "string", "acquisition_date", "string"), ("airline_id", "long", "airline_id", "string"), ("unique_carrier", "string", "unique_carrier", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("year", "long", "year", "string"), ("carrier", "string", "carrier", "string"), ("carrier_name", "string", "carrier_name", "string"), ("manufacture_year", "long", "manufacture_year", "string"), ("unique_carrier_name", "string", "unique_carrier_name", "string"), ("serial_number", "string", "serial_number", "string"), ("tail_number", "string", "tail_number", "string"), ("aircraft_status", "string", "aircraft_status", "string"), ("operating_status", "string", "operating_status", "string"), ("number_of_seats", "long", "number_of_seats", "string"), ("manufacturer", "string", "manufacturer", "string"), ("aircraft_type", "long", "aircraft_type", "string"), ("model", "string", "model", "string"), ("capacity_in_pounds", "long", "capacity_in_pounds", "string"), ("acquisition_date", "string", "acquisition_date", "string"), ("airline_id", "long", "airline_id", "string"), ("unique_carrier", "string", "unique_carrier", "string")], transformation_ctx = "applymapping1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": p_tgt_path }, format = "parquet", transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = applymapping1]
datasink3 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": p_tgt_path}, format = "parquet", transformation_ctx = "datasink3")
job.commit()