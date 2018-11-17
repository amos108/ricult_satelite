import requests,os
import sys
import json
from multiprocessing import Pool
from shapely.geometry import Polygon
from shapely.ops import unary_union
import boto3,time

from multiprocessing import Process
import config_file

## aws params
KEY        = config_file.KEY
SECRET_KEY = config_file.SECRET_KEY
REGION     = config_file.REGION
aws_mode = False

if aws_mode:
    import aws_api
    tran_api = aws_api.TransparentApi()
    all_data_sets = aws_api.ALL_DATA_SETS
else:
    import earth_explorer_api
    tran_api = earth_explorer_api.TransparentApi()
    all_data_sets = earth_explorer_api.ALL_DATA_SETS

        
if __name__ == '__main__':
    partial_scenes = sys.argv[1::]
    start_time = time.time()
    print(partial_scenes)

    all_data_sets  = { 'sentinel' :'SENTINEL_2A'}
    s3  = boto3.resource('s3',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)
    bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
    tmp_json = 'active_scenes.json'
    bucket.download_file(Key=config_file.ACTIVE_SCENE_KEY,Filename=tmp_json)
    with open(tmp_json, 'r') as fd:
        all_scenes = json.load(fd)

    for ii,scene in enumerate(partial_scenes):
        temp_dir  = './temp'
        bucket_name = config_file.DATA_BUCKET_NAME
        if scene not in all_scenes:
            continue
        rel_scene = all_scenes[scene]
        yy = time.time()
        print('downloading %f percent'%(ii/len(partial_scenes)))
        tran_api.download_scene(rel_scene,sc_id = scene,temp_dir = temp_dir)

        tran_api.proccess_raw_downloads(bucket_name,rel_scene,temp_dir)
        curr_time = time.time() - yy
        tot_time  = time.time() - start_time
        print('current run time : {} ,total time : {}'.format(curr_time,tot_time))


