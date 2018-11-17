import requests, os
import sys
import json
from shapely.geometry import Polygon
import boto3, time
import config_file
from datetime import datetime,timedelta

start_time = time.time()
## aws params
KEY = config_file.KEY
SECRET_KEY = config_file.SECRET_KEY
REGION = config_file.REGION

def remove_keys(the_dict,keys):
    for key in keys:
        if key in the_dict:
            del the_dict[key]
    return the_dict

ec2 = boto3.resource('ec2',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)
s3  = boto3.resource('s3',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)

history_path = config_file.HISTORY_PATH
if not os.path.exists(history_path):
    history = {}
    history['scenes'] = []
else:
    with open(history_path, 'r') as outfile:
        history = json.load(outfile)

with open('thailand.json') as fid:
    thailand = json.load(fid)


import sentnel_hub_api
tran_api = sentnel_hub_api
all_data_sets = {'sentinel': 'SENTINEL_2A'}

all_polygon = []
for ii, tai_feat in enumerate(thailand['features']):
    for polygon in tai_feat['geometry']['coordinates']:
        if len(polygon) == 1:
            polygon = polygon[0]
        all_polygon.append([[p[1], p[0]] for p in polygon])


end_datetime = datetime.now()
start_datetime = end_datetime - timedelta(days=30)

if False:
    all_scenes = {}
    for ee,poly in enumerate(all_polygon):
        print(ee/len(all_polygon))
        poly = Polygon(poly)
        xpoly = [[x, y] for x, y in zip(poly.exterior.xy[0], poly.exterior.xy[1])]
        poly = xpoly[0:-1]
        for dataset in all_data_sets:
            all_scenes.update(tran_api.get_all_scenes(poly, dataset,start_datetime = start_datetime,end_datetime=end_datetime))

    all_scenes = remove_keys(all_scenes,history)

    tmp_json = 'active_scenes.json'
    with open(tmp_json, 'w+') as fd:
        json.dump(all_scenes,fd)

    bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
    with open(tmp_json, 'rb') as data:
        bucket.put_object(Key=config_file.ACTIVE_SCENE_KEY, Body=data)

else:
    bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
    tmp_json = 'active_scenes.json'
    bucket.download_file(Key=config_file.ACTIVE_SCENE_KEY,Filename=tmp_json)
    with open(tmp_json, 'r') as fd:
        all_scenes = json.load(fd)

all_scenes = remove_keys(all_scenes, history)

import download_partial_sentinel_hub,time
counter = 0
failed = 0
tt = time.time()
for val in all_scenes.values():
    counter+=1
    print(counter,failed,len(all_scenes.values()),time.time()-tt)
    try:
        download_partial_sentinel_hub.main(val,curr_path = '/home/amos/temp')
    except:
        failed+=1
