
import requests,os
import json
from multiprocessing import Pool
from shapely.geometry import Polygon
from shapely.ops import unary_union
import boto3,time

from multiprocessing import Process
from datetime import datetime,timedelta
import config_file

root_dir = 'temp'
## aws params
KEY        = config_file.KEY
SECRET_KEY = config_file.SECRET_KEY
REGION     = config_file.REGION

import earth_explorer_api
tran_api = earth_explorer_api.TransparentApi()
all_data_sets = earth_explorer_api.ALL_DATA_SETS

all_scenes = {}
polygon = [[14.56,100.453],[14.6,100.453],[14.6,100.457]]

end_datetime = datetime.now()
start_datetime = end_datetime - timedelta(days = 365*2)

all_scenes = tran_api.get_all_scenes(polygon=polygon, dataset='sentinel', start_datetime=start_datetime, end_datetime=end_datetime)

for scene in all_scenes:
    temp_dir = root_dir + '/' + scene
    tran_api.download_scene(all_scenes[scene], sc_id=scene, temp_dir=temp_dir)
sentinelhub.constants
