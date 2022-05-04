import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_database', 'src_main_name', 'lookup_name1', 'tgt_bucket'])
p_src_database = args['src_database']
p_main_src_name = args['src_main_name']
p_src_table_name = "land_" + args['src_main_name']
p_src_table_name1 = "land_" + args['lookup_name1']
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

## @type: DataSource1
## @args: [database = p_src_database, table_name = p_src_table_name1, transformation_ctx = "datasource1"]
## @return: datasource1
## @inputs: []
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = p_src_database, table_name = p_src_table_name1, transformation_ctx = "datasource1")
df1 = datasource1.select_fields(["year", "month"]).toDF().distinct()
mapped_datasource1 = DynamicFrame.fromDF(df1, glueContext, 'mapped_datasource1')

## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
transformjoin1 = Join.apply(frame1 = datasource0, frame2 = mapped_datasource1, keys1 = ["year", "month"], keys2 = ["year", "month"], transformation_ctx = "transformjoin1")

## @type: ApplyMapping
## @args: [mapping = [("passengers", "double", "passengers", "double"), ("freight", "double", "freight", "double"), ("mail", "double", "mail", "double"), ("distance", "double", "distance", "double"), ("unique_carrier", "string", "unique_carrier", "string"), ("airline_id", "long", "airline_id", "string"), ("unique_carrier_name", "string", "unique_carrier_name", "string"), ("unique_carrier_entity", "string", "unique_carrier_entity", "string"), ("region", "string", "region", "string"), ("carrier", "string", "carrier", "string"), ("carrier_name", "string", "carrier_name", "string"), ("carrier_group", "long", "carrier_group", "string"), ("carrier_group_new", "long", "carrier_group_new", "string"), ("origin_airport_id", "long", "origin_airport_id", "string"), ("origin_airport_seq_id", "long", "origin_airport_seq_id", "string"), ("origin_city_market_id", "long", "origin_city_market_id", "string"), ("origin", "string", "origin", "string"), ("origin_city_name", "string", "origin_city_name", "string"), ("origin_state_abr", "string", "origin_state_abr", "string"), ("origin_state_fips", "long", "origin_state_fips", "string"), ("origin_state_nm", "string", "origin_state_nm", "string"), ("origin_country", "string", "origin_country", "string"), ("origin_country_name", "string", "origin_country_name", "string"), ("origin_wac", "long", "origin_wac", "string"), ("dest_airport_id", "long", "dest_airport_id", "string"), ("dest_airport_seq_id", "long", "dest_airport_seq_id", "string"), ("dest_city_market_id", "long", "dest_city_market_id", "string"), ("dest", "string", "dest", "string"), ("dest_city_name", "string", "dest_city_name", "string"), ("dest_state_abr", "string", "dest_state_abr", "string"), ("dest_state_fips", "long", "dest_state_fips", "string"), ("dest_state_nm", "string", "dest_state_nm", "string"), ("dest_country", "string", "dest_country", "string"), ("dest_country_name", "string", "dest_country_name", "string"), ("dest_wac", "long", "dest_wac", "string"), ("year", "long", "year", "string"), ("quarter", "long", "quarter", "string"), ("month", "long", "month", "string"), ("distance_group", "long", "distance_group", "string"), ("class", "string", "class", "string"), ("data_source", "string", "data_source", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = transformjoin1]
applymapping1 = ApplyMapping.apply(frame = transformjoin1, mappings = [("passengers", "double", "passengers", "double"), ("freight", "double", "freight", "double"), ("mail", "double", "mail", "double"), ("distance", "double", "distance", "double"), ("unique_carrier", "string", "unique_carrier", "string"), ("airline_id", "long", "airline_id", "string"), ("unique_carrier_name", "string", "unique_carrier_name", "string"), ("unique_carrier_entity", "string", "unique_carrier_entity", "string"), ("region", "string", "region", "string"), ("carrier", "string", "carrier", "string"), ("carrier_name", "string", "carrier_name", "string"), ("carrier_group", "long", "carrier_group", "string"), ("carrier_group_new", "long", "carrier_group_new", "string"), ("origin_airport_id", "long", "origin_airport_id", "string"), ("origin_airport_seq_id", "long", "origin_airport_seq_id", "string"), ("origin_city_market_id", "long", "origin_city_market_id", "string"), ("origin", "string", "origin", "string"), ("origin_city_name", "string", "origin_city_name", "string"), ("origin_state_abr", "string", "origin_state_abr", "string"), ("origin_state_fips", "long", "origin_state_fips", "string"), ("origin_state_nm", "string", "origin_state_nm", "string"), ("origin_country", "string", "origin_country", "string"), ("origin_country_name", "string", "origin_country_name", "string"), ("origin_wac", "long", "origin_wac", "string"), ("dest_airport_id", "long", "dest_airport_id", "string"), ("dest_airport_seq_id", "long", "dest_airport_seq_id", "string"), ("dest_city_market_id", "long", "dest_city_market_id", "string"), ("dest", "string", "dest", "string"), ("dest_city_name", "string", "dest_city_name", "string"), ("dest_state_abr", "string", "dest_state_abr", "string"), ("dest_state_fips", "long", "dest_state_fips", "string"), ("dest_state_nm", "string", "dest_state_nm", "string"), ("dest_country", "string", "dest_country", "string"), ("dest_country_name", "string", "dest_country_name", "string"), ("dest_wac", "long", "dest_wac", "string"), ("year", "long", "year", "string"), ("quarter", "long", "quarter", "string"), ("month", "long", "month", "string"), ("distance_group", "long", "distance_group", "string"), ("class", "string", "class", "string"), ("data_source", "string", "data_source", "string")], transformation_ctx = "applymapping1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": p_tgt_path }, format = "parquet", transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = applymapping1]
datasink3 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": p_tgt_path , "partitionKeys": ["year", "month"] }, format = "parquet", transformation_ctx = "datasink3")
job.commit()