import os
import subprocess
import uuid
import boto3
import json
from farm_uploader import proccess_farm_complete
import config_file

def handler(event, context):
  key = config_file.KEY
  secret_key = config_file.SECRET_KEY
  updated_farms_loc = config_file.UPDATED_FARMS_LOC
  s3 = boto3.resource('s3',aws_access_key_id =key,aws_secret_access_key=secret_key)
  for record in event['Records']:
      farm_loc = record['s3']['object']['key']
      fname = record['s3']['object']['key'].split('/')[-1]
      out_loc  = os.path.join(updated_farms_loc,fname)
      bucket_name =  event['Records'][0]['s3']['bucket']['name']
      s3.Bucket(bucket_name).download_file(farm_loc, r'/tmp/farm.json')
      with open(r'/tmp/farm.json','r') as f:
        farm = json.load(f)

      try:
        all_data_sets_proccesed = proccess_farm_complete(farm,max_results=1)
        is_succeeded = True
      except:
        is_succeeded = False

      tmp_farm_loc = r'/tmp/farm.json'
      with open(tmp_farm_loc, 'w+') as f:
        json.dump(farm, f)
      s3.Object(bucket_name, farm_loc).delete()
      s3.Bucket(bucket_name).upload_file(tmp_farm_loc, out_loc)

      if not is_succeeded:
          error_farms_loc=config_file.ERROR_FARMS_LOC
          error_loc  = os.path.join(error_farms_loc,fname)
          s3.Object(bucket_name, farm_loc).delete()
          s3.Bucket(bucket_name).upload_file(tmp_farm_loc, error_loc)
          return "ERROR farm {} was not processed".format(farm['id'])

      elif not all_data_sets_proccesed:
          error_farms_loc = config_file.ERROR_TYPE_SOME_DATA_SET_FARMS_LOC
          error_loc = os.path.join(error_farms_loc, fname)
          s3.Bucket(bucket_name).upload_file(tmp_farm_loc, error_loc)
          return "farm {} was processed on some datasets".format(farm['id'])

      else:
          return "farm {} processed successfully".format(farm['id'])


