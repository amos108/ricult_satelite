import os
import subprocess
import uuid
import boto3
import json
from farm_uploader import create_status_json,get_raw_dirs_groups
import config_file,earth_explorer_api

def handler(event, context):
  key = config_file.KEY
  secret_key = config_file.SECRET_KEY
  updated_farms_loc = config_file.UPDATED_FARMS_LOC
  s3 = boto3.resource('s3',aws_access_key_id =key,aws_secret_access_key=secret_key)
  data_bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
  upload_bucket = s3.Bucket(config_file.UPLOAD_BUCKET_NAME)

  for record in event['Records']:
      farm_loc = record['s3']['object']['key']
      fname = record['s3']['object']['key'].split('/')[-1]
      out_loc  = os.path.join(updated_farms_loc,fname)
      bucket_name =  event['Records'][0]['s3']['bucket']['name']
      tmp_farm_loc = r'/tmp/farm.json'
      s3.Bucket(bucket_name).download_file(farm_loc, tmp_farm_loc)
      with open(tmp_farm_loc,'r') as f:
        farm = json.load(f)

      create_status_json(farm, upload_bucket)

      if 'directory_list' not in farm:
        farm['directory_list'] = []

      for dataset in earth_explorer_api.ALL_DATA_SETS:
          if dataset not in farm:
            farm[dataset] = {}
            farm[dataset]['last_updated'] = '0001-1-1'

      with open(tmp_farm_loc, 'w+') as f:
        json.dump(farm, f)
      s3.Object(bucket_name, farm_loc).delete()
      s3.Bucket(bucket_name).upload_file(tmp_farm_loc, out_loc)



      if farm['status'] != 'valid':
          return

      groups = get_raw_dirs_groups(farm, data_bucket,max_results=200)
      counter = 0
      for data_set,group in groups.items():
          for date,raw_dir_group in group.items():
              date_str = date.strftime('%Y-%m-%d')
              file_name = str(farm['id']) +'_' + str(counter) + '.json'
              counter+=1
              temp_out_loc = config_file.DATE_FARM_LOC + '/' + file_name
              tmp_farm_loc = r'/tmp/' + file_name
              tmp_farm = {}
              tmp_farm['farm']     = farm
              tmp_farm['data_set'] = data_set
              tmp_farm['date']     = date_str
              tmp_farm['raw_dir_group'] = raw_dir_group
              tmp_farm['farm_json_loc'] = out_loc
              with open(tmp_farm_loc, 'w+') as f:
                json.dump(tmp_farm, f)
              s3.Bucket(bucket_name).upload_file(tmp_farm_loc, temp_out_loc)

      tmp_farm_loc = r'/tmp/farm.json'

