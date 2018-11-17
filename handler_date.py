import os
import subprocess
import uuid
import boto3
import json
from farm_uploader import proccess_farm_date_complete
import config_file,earth_explorer_api
from datetime import  datetime

def handler(event, context):
  key = config_file.KEY
  secret_key = config_file.SECRET_KEY
  updated_farms_loc = config_file.UPDATED_FARMS_LOC
  s3 = boto3.resource('s3',aws_access_key_id =key,aws_secret_access_key=secret_key)
  for record in event['Records']:
      tmp_file_loc = r'/tmp/temp_file.json'
      farm_loc = record['s3']['object']['key']
      fname = record['s3']['object']['key'].split('/')[-1]
      out_loc  = os.path.join(updated_farms_loc,fname)
      bucket_name =  event['Records'][0]['s3']['bucket']['name']
      s3.Bucket(bucket_name).download_file(farm_loc, tmp_file_loc)
      with open(tmp_file_loc,'r') as f:
          tmp_file = json.load(f)

      farm = tmp_file['farm']
      data_set = tmp_file['data_set']
      date_str = tmp_file['date']
      raw_dir_group = tmp_file['raw_dir_group']
      date = datetime.strptime(date_str,'%Y-%m-%d')
      is_succeeded = proccess_farm_date_complete(farm = farm, dataset=data_set, date =date, raw_dir_group=raw_dir_group)



      #merge !!!!!
      farm_json_loc = tmp_file['farm_json_loc']
      tmp_farm_loc = r'/tmp/farm.json'
      s3.Bucket(bucket_name).download_file(farm_json_loc,tmp_farm_loc)
      with open(tmp_farm_loc,'r') as f:
          tmp_farm = json.load(f)

      if 'directory_list' not in tmp_farm:
          tmp_farm['directory_list'] = []

      for dataset in earth_explorer_api.ALL_DATA_SETS:
          if dataset not in tmp_farm:
              tmp_farm[dataset] = {}
              tmp_farm[dataset]['last_updated'] = '0001-1-1'


      farm['directory_list'] = list(set(tmp_farm['directory_list']).union(farm['directory_list']))
      farm_date = datetime.strptime(tmp_farm[data_set]['last_updated'], '%Y-%m-%d')
      farm[data_set]['last_updated'] = max([farm_date, date]).strftime('%Y-%m-%d')

      with open(tmp_farm_loc,'w+') as f:
          json.dump(farm,f)

      s3.Bucket(bucket_name).upload_file(tmp_farm_loc, farm_json_loc)

      with open(r'/tmp/directory_list.json', 'w+') as outfile:
          json.dump(farm['directory_list'], outfile)

      with open(r'/tmp/directory_list.json' ,'rb') as data:
          s3.Bucket(config_file.UPLOAD_BUCKET_NAME).put_object(Key='satellite/' + str(farm['id']) + '/directory_list.json', Body=data)


      s3.Object(bucket_name, farm_loc).delete()
      if not is_succeeded:
          error_farms_loc=config_file.ERROR_FARMS_LOC
          error_loc  = os.path.join(error_farms_loc,farm_loc.split('/')[-1])
          s3.Bucket(bucket_name).upload_file(tmp_file_loc, error_loc)
          return "ERROR farm {} was not processed".format(farm['id'])


