import sys
from awsglue.utils import getResolvedOptions
import boto3

args = getResolvedOptions(sys.argv,['bucket_name', 'folder_names'])
bucket_name = args['bucket_name']
folder_names = args['folder_names']
folder_names = [x.strip() for x in folder_names.split(',')]

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)

bucket_list = [file.key[:-1] for file in bucket.objects.all()]


for key_name in list(set(folder_names) - set(bucket_list)):
    bucket.put_object(Bucket=bucket_name, Body='', Key=(key_name+'/'))