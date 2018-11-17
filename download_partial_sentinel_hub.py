import sys
sys.path+=[
    '/home/ec2-user',
    '/usr/lib64/python37.zip',
    '/usr/lib64/python3.7',
    '/usr/lib64/python3.7/lib-dynload',
    '/home/ec2-user/.local/lib/python3.7/site-packages',
    '/usr/local/lib64/python3.7/site-packages',
    '/usr/local/lib/python3.7/site-packages',
    '/usr/lib64/python3.7/site-packages',
    '/usr/lib/python3.7/site-packages',
]

import requests,os
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
import sentnel_hub_api
tran_api = sentnel_hub_api
all_data_sets = {'sentinel': 'SENTINEL_2A'}



def main(val,curr_path = '/home/ec2-user'):
    temp_dir = '/tmp/sat_downloads'  # curr_path + '/temp'
    yy = time.time()
    tran_api.download_scene(val, temp_dir,'sentinel')

    curr_time = time.time() - yy
    print('current run time : {}'.format(curr_time))


if __name__ == '__main__':
    partial_scenes = sys.argv[1::]
    partial_scenes[-1] = int(partial_scenes[-1])
    print(partial_scenes)
    main(partial_scenes)

