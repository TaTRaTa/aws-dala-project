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
## @args: [mapping = [("pilotexpense", "double", "pilotexpense", "double"), ("aircraft_fuel", "double", "aircraft_fuel", "double"), ("other_expenses", "double", "other_expenses", "double"), ("total_direct_less_rent", "double", "total_direct_less_rent", "double"), ("maintenance", "double", "maintenance", "double"), ("depand_rental", "double", "depand_rental", "double"), ("total_direct", "double", "total_direct", "double"), ("flight_attendants", "double", "flight_attendants", "double"), ("traffic", "double", "traffic", "double"), ("departure", "double", "departure", "double"), ("capacity", "double", "capacity", "double"), ("total_indirect", "double", "total_indirect", "double"), ("total_op_expense", "double", "total_op_expense", "double"), ("total_air_hours", "double", "total_air_hours", "double"), ("air_days_assign", "double", "air_days_assign", "double"), ("air_fuels_issued", "double", "air_fuels_issued", "double"), ("aircraft_config", "long", "aircraft_config", "string"), ("aircraft_group", "long", "aircraft_group", "string"), ("aircraft_type", "long", "aircraft_type", "string"), ("airline_id", "long", "airline_id", "string"), ("unique_carrier", "string", "unique_carrier", "string"), ("unique_carrier_name", "string", "unique_carrier_name", "string"), ("carrier", "string", "carrier", "string"), ("carrier_name", "string", "carrier_name", "string"), ("unique_carrier_entity", "long", "unique_carrier_entity", "string"), ("region", "string", "region", "string"), ("carrier_group_new", "long", "carrier_group_new", "string"), ("carrier_group", "long", "carrier_group", "string"), ("year", "long", "year", "string"), ("quarter", "long", "quarter", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("pilotexpense", "double", "pilotexpense", "double"), ("aircraft_fuel", "double", "aircraft_fuel", "double"), ("other_expenses", "double", "other_expenses", "double"), ("total_direct_less_rent", "double", "total_direct_less_rent", "double"), ("maintenance", "double", "maintenance", "double"), ("depand_rental", "double", "depand_rental", "double"), ("total_direct", "double", "total_direct", "double"), ("flight_attendants", "double", "flight_attendants", "double"), ("traffic", "double", "traffic", "double"), ("departure", "double", "departure", "double"), ("capacity", "double", "capacity", "double"), ("total_indirect", "double", "total_indirect", "double"), ("total_op_expense", "double", "total_op_expense", "double"), ("total_air_hours", "double", "total_air_hours", "double"), ("air_days_assign", "double", "air_days_assign", "double"), ("air_fuels_issued", "double", "air_fuels_issued", "double"), ("aircraft_config", "long", "aircraft_config", "string"), ("aircraft_group", "long", "aircraft_group", "string"), ("aircraft_type", "long", "aircraft_type", "string"), ("airline_id", "long", "airline_id", "string"), ("unique_carrier", "string", "unique_carrier", "string"), ("unique_carrier_name", "string", "unique_carrier_name", "string"), ("carrier", "string", "carrier", "string"), ("carrier_name", "string", "carrier_name", "string"), ("unique_carrier_entity", "long", "unique_carrier_entity", "string"), ("region", "string", "region", "string"), ("carrier_group_new", "long", "carrier_group_new", "string"), ("carrier_group", "long", "carrier_group", "string"), ("year", "long", "year", "string"), ("quarter", "long", "quarter", "string")], transformation_ctx = "applymapping1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": p_tgt_path }, format = "parquet", transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = applymapping1]
datasink3 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": p_tgt_path }, format = "parquet", transformation_ctx = "datasink3")
job.commit()