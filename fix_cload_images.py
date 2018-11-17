import boto3,config_file
import json,general_algorithms,rasterio
import os
s3 = boto3.resource('s3', aws_access_key_id=config_file.KEY, aws_secret_access_key=config_file.SECRET_KEY)
dirctory_path = '/tmp/cloud_fix'
if not os.path.exists(dirctory_path):
    os.makedirs(dirctory_path)
    
bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
prefix = 'satellite/raw_satellite_data/sentinel/'
objs = list(bucket.objects.filter(Prefix= prefix).all())

with open('cload_bugs.json') as fid:
    bugs = json.load(fid)

all_b02_files = [obj.key for obj in objs if obj.key.split('/')[-1]=='B02.jp2']
all_direct_keys = ['/'.join(f.split('/')[0:-1])for f in all_b02_files]
from collections import  Counter
all_lat_lon = [f.split('/')[3]for f in all_b02_files]
c_all_lat_lon = Counter(all_lat_lon)

for ii,key in enumerate(all_direct_keys):
    print(ii/len(all_direct_keys))
    #try:
    if os.path.exists(dirctory_path+'/B02.jp2'):
        os.unlink(dirctory_path+'/B02.jp2')

    print(key,'key')
    bucket.download_file(key+'/B02.jp2', dirctory_path+'/B02.jp2')

    if os.path.exists(dirctory_path+'/B09.jp2'):
        os.unlink(dirctory_path+'/B09.jp2')

    bucket.download_file(key+'/B09.jp2',dirctory_path+'/B09.jp2')
    general_algorithms.save_cload_image_sentinel(dirctory_path)
#except:
    bugs.append(key)



with open('cload_bugs.json','w+') as fid:
    json.dump(bugs, fid)