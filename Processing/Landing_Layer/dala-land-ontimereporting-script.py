import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_database', 'src_main_name', 'lookup_name1', 'lookup_name2', 'tgt_bucket'])
p_src_database = args['src_database']
p_main_src_name = args['src_main_name']
p_src_table_name0 = "land_" + args['src_main_name']
p_src_table_name1 = "land_" + args['lookup_name1']
p_src_table_name2 = "land_" + args['lookup_name2']
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
## @args: [database = p_src_database, table_name = p_src_table_name0, transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = p_src_database, table_name = p_src_table_name0, transformation_ctx = "datasource0")

## @type: DataSource1
## @args: [database = p_src_database, table_name = p_src_table_name1, transformation_ctx = "datasource1"]
## @return: datasource1
## @inputs: []
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = p_src_database, table_name = p_src_table_name1, transformation_ctx = "datasource1")
df1 = datasource1.select_fields(["year", "month"]).toDF().distinct()
mapped_datasource1 = DynamicFrame.fromDF(df1, glueContext, 'mapped_datasource1')

## @type: DataSource2
## @args: [database = p_src_database, table_name = p_src_table_name2, transformation_ctx = "datasource2"]
## @return: datasource2
## @inputs: []
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = p_src_database, table_name = p_src_table_name2, transformation_ctx = "datasource2")
df2 = datasource2.select_fields(["year", "month"]).toDF().distinct()
mapped_datasource2 = DynamicFrame.fromDF(df2, glueContext, 'mapped_datasource2')

## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
transformjoin1 = Join.apply(frame1 = mapped_datasource1, frame2 = mapped_datasource2, keys1 = ["year", "month"], keys2 = ["year", "month"], transformation_ctx = "transformjoin1")

## @type: DropFields
## @args: [paths = [<paths>], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
transformdropfields1 = DropFields.apply(frame = transformjoin1, paths = ["`.year`", "`.month`"], transformation_ctx = "transformdropfields1")

## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
transformjoin2 = Join.apply(frame1 = datasource0, frame2 = transformdropfields1, keys1 = ["year", "month"], keys2 = ["year", "month"], transformation_ctx = "transformjoin2")

## @type: ApplyMapping
## @args: [mapping = [("year", "long", "year", "string"), ("quarter", "long", "quarter", "string"), ("month", "long", "month", "string"), ("day_of_month", "long", "day_of_month", "string"), ("day_of_week", "long", "day_of_week", "string"), ("fl_date", "string", "fl_date", "string"), ("op_unique_carrier", "string", "op_unique_carrier", "string"), ("op_carrier_airline_id", "long", "op_carrier_airline_id", "string"), ("op_carrier", "string", "op_carrier", "string"), ("tail_num", "string", "tail_num", "string"), ("op_carrier_fl_num", "long", "op_carrier_fl_num", "string"), ("origin_airport_id", "long", "origin_airport_id", "string"), ("origin_airport_seq_id", "long", "origin_airport_seq_id", "string"), ("origin_city_market_id", "long", "origin_city_market_id", "string"), ("origin", "string", "origin", "string"), ("origin_city_name", "string", "origin_city_name", "string"), ("origin_state_abr", "string", "origin_state_abr", "string"), ("origin_state_fips", "long", "origin_state_fips", "string"), ("origin_state_nm", "string", "origin_state_nm", "string"), ("origin_wac", "long", "origin_wac", "string"), ("dest_airport_id", "long", "dest_airport_id", "string"), ("dest_airport_seq_id", "long", "dest_airport_seq_id", "string"), ("dest_city_market_id", "long", "dest_city_market_id", "string"), ("dest", "string", "dest", "string"), ("dest_city_name", "string", "dest_city_name", "string"), ("dest_state_abr", "string", "dest_state_abr", "string"), ("dest_state_fips", "long", "dest_state_fips", "string"), ("dest_state_nm", "string", "dest_state_nm", "string"), ("dest_wac", "long", "dest_wac", "string"), ("crs_dep_time", "long", "crs_dep_time", "string"), ("dep_time", "long", "dep_time", "string"), ("dep_delay", "double", "dep_delay", "double"), ("dep_delay_new", "double", "dep_delay_new", "double"), ("dep_del15", "double", "dep_del15", "double"), ("dep_delay_group", "long", "dep_delay_group", "string"), ("dep_time_blk", "string", "dep_time_blk", "string"), ("taxi_out", "double", "taxi_out", "double"), ("wheels_off", "long", "wheels_off", "string"), ("wheels_on", "long", "wheels_on", "string"), ("taxi_in", "double", "taxi_in", "double"), ("crs_arr_time", "long", "crs_arr_time", "string"), ("arr_time", "long", "arr_time", "string"), ("arr_delay", "double", "arr_delay", "double"), ("arr_delay_new", "double", "arr_delay_new", "double"), ("arr_del15", "double", "arr_del15", "double"), ("arr_delay_group", "long", "arr_delay_group", "string"), ("arr_time_blk", "string", "arr_time_blk", "string"), ("cancelled", "double", "cancelled", "double"), ("cancellation_code", "string", "cancellation_code", "string"), ("diverted", "double", "diverted", "double"), ("crs_elapsed_time", "double", "crs_elapsed_time", "double"), ("actual_elapsed_time", "double", "actual_elapsed_time", "double"), ("air_time", "double", "air_time", "double"), ("flights", "double", "flights", "double"), ("distance", "double", "distance", "double"), ("distance_group", "long", "distance_group", "string"), ("carrier_delay", "double", "carrier_delay", "double"), ("weather_delay", "double", "weather_delay", "double"), ("nas_delay", "double", "nas_delay", "double"), ("security_delay", "double", "security_delay", "double"), ("late_aircraft_delay", "double", "late_aircraft_delay", "double"), ("first_dep_time", "long", "first_dep_time", "string"), ("total_add_gtime", "double", "total_add_gtime", "double"), ("longest_add_gtime", "double", "longest_add_gtime", "double"), ("div_airport_landings", "long", "div_airport_landings", "string"), ("div_reached_dest", "double", "div_reached_dest", "double"), ("div_actual_elapsed_time", "double", "div_actual_elapsed_time", "double"), ("div_arr_delay", "double", "div_arr_delay", "double"), ("div_distance", "double", "div_distance", "double"), ("div1_airport", "string", "div1_airport", "string"), ("div1_airport_id", "long", "div1_airport_id", "string"), ("div1_airport_seq_id", "long", "div1_airport_seq_id", "string"), ("div1_wheels_on", "long", "div1_wheels_on", "string"), ("div1_total_gtime", "double", "div1_total_gtime", "double"), ("div1_longest_gtime", "double", "div1_longest_gtime", "double"), ("div1_wheels_off", "long", "div1_wheels_off", "string"), ("div1_tail_num", "string", "div1_tail_num", "string"), ("div2_airport", "string", "div2_airport", "string"), ("div2_airport_id", "string", "div2_airport_id", "string"), ("div2_airport_seq_id", "string", "div2_airport_seq_id", "string"), ("div2_wheels_on", "string", "div2_wheels_on", "string"), ("div2_total_gtime", "string", "div2_total_gtime", "string"), ("div2_longest_gtime", "string", "div2_longest_gtime", "string"), ("div2_wheels_off", "string", "div2_wheels_off", "string"), ("div2_tail_num", "string", "div2_tail_num", "string"), ("div3_airport", "string", "div3_airport", "string"), ("div3_airport_id", "string", "div3_airport_id", "string"), ("div3_airport_seq_id", "string", "div3_airport_seq_id", "string"), ("div3_wheels_on", "string", "div3_wheels_on", "string"), ("div3_total_gtime", "string", "div3_total_gtime", "string"), ("div3_longest_gtime", "string", "div3_longest_gtime", "string"), ("div3_wheels_off", "string", "div3_wheels_off", "string"), ("div3_tail_num", "string", "div3_tail_num", "string"), ("div4_airport", "string", "div4_airport", "string"), ("div4_airport_id", "string", "div4_airport_id", "string"), ("div4_airport_seq_id", "string", "div4_airport_seq_id", "string"), ("div4_wheels_on", "string", "div4_wheels_on", "string"), ("div4_total_gtime", "string", "div4_total_gtime", "string"), ("div4_longest_gtime", "string", "div4_longest_gtime", "string"), ("div4_wheels_off", "string", "div4_wheels_off", "string"), ("div4_tail_num", "string", "div4_tail_num", "string"), ("div5_airport", "string", "div5_airport", "string"), ("div5_airport_id", "string", "div5_airport_id", "string"), ("div5_airport_seq_id", "string", "div5_airport_seq_id", "string"), ("div5_wheels_on", "string", "div5_wheels_on", "string"), ("div5_total_gtime", "string", "div5_total_gtime", "string"), ("div5_longest_gtime", "string", "div5_longest_gtime", "string"), ("div5_wheels_off", "string", "div5_wheels_off", "string"), ("div5_tail_num", "string", "div5_tail_num", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = transformjoin2]
applymapping1 = ApplyMapping.apply(frame = transformjoin2, mappings = [("year", "long", "year", "string"), ("quarter", "long", "quarter", "string"), ("month", "long", "month", "string"), ("day_of_month", "long", "day_of_month", "string"), ("day_of_week", "long", "day_of_week", "string"), ("fl_date", "string", "fl_date", "string"), ("op_unique_carrier", "string", "op_unique_carrier", "string"), ("op_carrier_airline_id", "long", "op_carrier_airline_id", "string"), ("op_carrier", "string", "op_carrier", "string"), ("tail_num", "string", "tail_num", "string"), ("op_carrier_fl_num", "long", "op_carrier_fl_num", "string"), ("origin_airport_id", "long", "origin_airport_id", "string"), ("origin_airport_seq_id", "long", "origin_airport_seq_id", "string"), ("origin_city_market_id", "long", "origin_city_market_id", "string"), ("origin", "string", "origin", "string"), ("origin_city_name", "string", "origin_city_name", "string"), ("origin_state_abr", "string", "origin_state_abr", "string"), ("origin_state_fips", "long", "origin_state_fips", "string"), ("origin_state_nm", "string", "origin_state_nm", "string"), ("origin_wac", "long", "origin_wac", "string"), ("dest_airport_id", "long", "dest_airport_id", "string"), ("dest_airport_seq_id", "long", "dest_airport_seq_id", "string"), ("dest_city_market_id", "long", "dest_city_market_id", "string"), ("dest", "string", "dest", "string"), ("dest_city_name", "string", "dest_city_name", "string"), ("dest_state_abr", "string", "dest_state_abr", "string"), ("dest_state_fips", "long", "dest_state_fips", "string"), ("dest_state_nm", "string", "dest_state_nm", "string"), ("dest_wac", "long", "dest_wac", "string"), ("crs_dep_time", "long", "crs_dep_time", "string"), ("dep_time", "long", "dep_time", "string"), ("dep_delay", "double", "dep_delay", "double"), ("dep_delay_new", "double", "dep_delay_new", "double"), ("dep_del15", "double", "dep_del15", "double"), ("dep_delay_group", "long", "dep_delay_group", "string"), ("dep_time_blk", "string", "dep_time_blk", "string"), ("taxi_out", "double", "taxi_out", "double"), ("wheels_off", "long", "wheels_off", "string"), ("wheels_on", "long", "wheels_on", "string"), ("taxi_in", "double", "taxi_in", "double"), ("crs_arr_time", "long", "crs_arr_time", "string"), ("arr_time", "long", "arr_time", "string"), ("arr_delay", "double", "arr_delay", "double"), ("arr_delay_new", "double", "arr_delay_new", "double"), ("arr_del15", "double", "arr_del15", "double"), ("arr_delay_group", "long", "arr_delay_group", "string"), ("arr_time_blk", "string", "arr_time_blk", "string"), ("cancelled", "double", "cancelled", "double"), ("cancellation_code", "string", "cancellation_code", "string"), ("diverted", "double", "diverted", "double"), ("crs_elapsed_time", "double", "crs_elapsed_time", "double"), ("actual_elapsed_time", "double", "actual_elapsed_time", "double"), ("air_time", "double", "air_time", "double"), ("flights", "double", "flights", "double"), ("distance", "double", "distance", "double"), ("distance_group", "long", "distance_group", "string"), ("carrier_delay", "double", "carrier_delay", "double"), ("weather_delay", "double", "weather_delay", "double"), ("nas_delay", "double", "nas_delay", "double"), ("security_delay", "double", "security_delay", "double"), ("late_aircraft_delay", "double", "late_aircraft_delay", "double"), ("first_dep_time", "long", "first_dep_time", "string"), ("total_add_gtime", "double", "total_add_gtime", "double"), ("longest_add_gtime", "double", "longest_add_gtime", "double"), ("div_airport_landings", "long", "div_airport_landings", "string"), ("div_reached_dest", "double", "div_reached_dest", "double"), ("div_actual_elapsed_time", "double", "div_actual_elapsed_time", "double"), ("div_arr_delay", "double", "div_arr_delay", "double"), ("div_distance", "double", "div_distance", "double"), ("div1_airport", "string", "div1_airport", "string"), ("div1_airport_id", "long", "div1_airport_id", "string"), ("div1_airport_seq_id", "long", "div1_airport_seq_id", "string"), ("div1_wheels_on", "long", "div1_wheels_on", "string"), ("div1_total_gtime", "double", "div1_total_gtime", "double"), ("div1_longest_gtime", "double", "div1_longest_gtime", "double"), ("div1_wheels_off", "long", "div1_wheels_off", "string"), ("div1_tail_num", "string", "div1_tail_num", "string"), ("div2_airport", "string", "div2_airport", "string"), ("div2_airport_id", "string", "div2_airport_id", "string"), ("div2_airport_seq_id", "string", "div2_airport_seq_id", "string"), ("div2_wheels_on", "string", "div2_wheels_on", "string"), ("div2_total_gtime", "string", "div2_total_gtime", "string"), ("div2_longest_gtime", "string", "div2_longest_gtime", "string"), ("div2_wheels_off", "string", "div2_wheels_off", "string"), ("div2_tail_num", "string", "div2_tail_num", "string"), ("div3_airport", "string", "div3_airport", "string"), ("div3_airport_id", "string", "div3_airport_id", "string"), ("div3_airport_seq_id", "string", "div3_airport_seq_id", "string"), ("div3_wheels_on", "string", "div3_wheels_on", "string"), ("div3_total_gtime", "string", "div3_total_gtime", "string"), ("div3_longest_gtime", "string", "div3_longest_gtime", "string"), ("div3_wheels_off", "string", "div3_wheels_off", "string"), ("div3_tail_num", "string", "div3_tail_num", "string"), ("div4_airport", "string", "div4_airport", "string"), ("div4_airport_id", "string", "div4_airport_id", "string"), ("div4_airport_seq_id", "string", "div4_airport_seq_id", "string"), ("div4_wheels_on", "string", "div4_wheels_on", "string"), ("div4_total_gtime", "string", "div4_total_gtime", "string"), ("div4_longest_gtime", "string", "div4_longest_gtime", "string"), ("div4_wheels_off", "string", "div4_wheels_off", "string"), ("div4_tail_num", "string", "div4_tail_num", "string"), ("div5_airport", "string", "div5_airport", "string"), ("div5_airport_id", "string", "div5_airport_id", "string"), ("div5_airport_seq_id", "string", "div5_airport_seq_id", "string"), ("div5_wheels_on", "string", "div5_wheels_on", "string"), ("div5_total_gtime", "string", "div5_total_gtime", "string"), ("div5_longest_gtime", "string", "div5_longest_gtime", "string"), ("div5_wheels_off", "string", "div5_wheels_off", "string"), ("div5_tail_num", "string", "div5_tail_num", "string")], transformation_ctx = "applymapping1")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": p_tgt_path }, format = "parquet", transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = applymapping1]
datasink3 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": p_tgt_path , "partitionKeys": ["year", "month"] }, format = "parquet", transformation_ctx = "datasink3")
job.commit()