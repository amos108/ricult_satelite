
import requests
import time
import boto3 
import json
from shapely.geometry.polygon import Polygon
import os
import config_file
key        = config_file.KEY
secret_key = config_file.SECRET_KEY
region     = config_file.REGION
DATA_BUCKET_NAME   = config_file.DATA_BUCKET_NAME
UPLOAD_BUCKET_NAME = config_file.UPLOAD_BUCKET_NAME

def create_status_json(farm,bucket,ignore_area = 1e-4,tmp_directory = '/tmp'):
    if 'status' in farm:
        return 
    if len(farm['coordinates'])<3:
        status = 'invalid polygon'
    else:
        polygon = Polygon(farm['coordinates'])
        if not polygon.is_valid:
            status = 'invalid polygon'
        elif polygon.area>=ignore_area:
            status = 'too large'
        else:
            status = 'valid'

    with open(os.path.join(tmp_directory,'status.json'), 'w') as outfile:
        json.dump({'status':status}, outfile)

    with open(os.path.join(tmp_directory,'status.json'), 'rb') as data:
        bucket.put_object(Key = 'satellite/' + str(farm['id']) + '/status.json', Body=data)
    farm['status'] = status

def proccess_farm(farm,data_set,api,data_bucket_name   = DATA_BUCKET_NAME,upload_bucket_name = UPLOAD_BUCKET_NAME,tmp_directory = '/tmp',max_results=100):
    s3 = boto3.resource('s3',aws_access_key_id =key,aws_secret_access_key=secret_key)
    data_bucket   = s3.Bucket(data_bucket_name)
    upload_bucket = s3.Bucket(upload_bucket_name)
    client = boto3.client('s3',aws_access_key_id =key,aws_secret_access_key=secret_key)
    create_status_json(farm,upload_bucket,tmp_directory=tmp_directory)
    if farm['status']!='valid':
        return
    if 'directory_list' not in farm:
        farm['directory_list'] = []
    api.process_farm(farm,data_set,data_bucket,upload_bucket,max_results=max_results)
    with open(os.path.join(tmp_directory,'directory_list.json'), 'w') as outfile:
        json.dump(farm['directory_list'], outfile)

    with open(os.path.join(tmp_directory,'directory_list.json'), 'rb') as data:
        upload_bucket.put_object(Key = 'satellite/' + str(farm['id']) + '/directory_list.json', Body=data)


def proccess_farm_complete(farm,aws_mode = False,data_bucket_name   = DATA_BUCKET_NAME,upload_bucket_name = UPLOAD_BUCKET_NAME,max_results=100):
    if aws_mode:
        import aws_api
        tran_api = aws_api.TransparentApi()
        all_data_sets = aws_api.ALL_DATA_SETS
    else:
        import earth_explorer_api
        tran_api = earth_explorer_api.TransparentApi()
        all_data_sets = earth_explorer_api.ALL_DATA_SETS
        all_data_sets_proccesed=True
    for data_set in all_data_sets:
        try:
            proccess_farm(farm,data_set,tran_api,data_bucket_name,upload_bucket_name,max_results=max_results)
        except:
            all_data_sets_proccesed=False
    return all_data_sets_proccesed


def proccess_farm_date_complete(farm,dataset,date,raw_dir_group,aws_mode = False,data_bucket_name   = DATA_BUCKET_NAME,upload_bucket_name = UPLOAD_BUCKET_NAME):
    if aws_mode:
        import aws_api
        tran_api = aws_api.TransparentApi()
        all_data_sets = aws_api.ALL_DATA_SETS
    else:
        import earth_explorer_api
        tran_api = earth_explorer_api.TransparentApi()
        all_data_sets = earth_explorer_api.ALL_DATA_SETS

    s3 = boto3.resource('s3', aws_access_key_id=key, aws_secret_access_key=secret_key)
    data_bucket = s3.Bucket(data_bucket_name)
    upload_bucket = s3.Bucket(upload_bucket_name)
    try:
        tran_api.process_raw_dir_groups(farm, dataset, data_bucket, upload_bucket,date,raw_dir_group)
        return True
    except:
        return False

def get_raw_dirs_groups(farm,data_bucket,max_results = 100,aws_mode = False):

    if aws_mode:
        import aws_api
        tran_api = aws_api.TransparentApi()
        all_data_sets = aws_api.ALL_DATA_SETS
    else:
        import earth_explorer_api
        tran_api = earth_explorer_api.TransparentApi()
        all_data_sets = earth_explorer_api.ALL_DATA_SETS

    raw_dirs_groups = {}
    for data_set in all_data_sets:
        raw_dirs_groups[data_set] = tran_api.get_raw_dirs_groups(farm['coordinates'], data_set, data_bucket, farm, max_results=max_results)

    return raw_dirs_groups



def  main(aws_mode = False):


    if aws_mode:
        import aws_api
        tran_api = aws_api.TransparentApi()
        all_data_sets = aws_api.ALL_DATA_SETS
    else:
        import earth_explorer_api
        tran_api = earth_explorer_api.TransparentApi()
        all_data_sets = earth_explorer_api.ALL_DATA_SETS


    farms = requests.get('http://www.ricult.com/api/active_farms').json()

    if not os.path.exists('uploaded_farms.json'):
        uploaded_farms = []
    else:
        with open('uploaded_farms.json', 'r') as outfile:
            uploaded_farms = json.load(outfile)

    farms          = {f['id']:f for f in farms}

    ## update upload_farms dict
    uploaded_farms = {f['id']:f for f in uploaded_farms}

    for id in farms.keys():
        if id not in uploaded_farms:
            uploaded_farms[id] = farms[id]

        elif farms[id]['coordinates']!=uploaded_farms[id]['coordinates']:
            uploaded_farms[id] = farms[id]



    for ii,farm in enumerate(uploaded_farms.values()):
        for data_set in all_data_sets:
            print("data_set:%s farm:%d %d out of %d"%(data_set,farm['id'],ii+1,len(uploaded_farms)))
            proccess_farm(farm,data_set,tran_api)

    with open('uploaded_farms.json', 'w') as outfile:
        json.dump(list(uploaded_farms.values()), outfile)


def debug_main(aws_mode = False):
    if aws_mode:
        import aws_api
        tran_api = aws_api.TransparentApi()
        all_data_sets = aws_api.ALL_DATA_SETS
    else:
        import earth_explorer_api
        tran_api = earth_explorer_api.TransparentApi()
        all_data_sets = earth_explorer_api.ALL_DATA_SETS

    uploaded_farms = {}
    uploaded_farms[0] = {}
    uploaded_farms[0]['coordinates'] = [[19.005,105.005],[19.005,105.009],[19.009,105.009]]
    uploaded_farms[0]['id'] = 0

    for ii,farm in enumerate(uploaded_farms.values()):
        for data_set in all_data_sets:
            print("data_set:%s farm:%d %d out of %d"%(data_set,farm['id'],ii+1,len(uploaded_farms)))
            proccess_farm(farm,data_set,tran_api)

    with open('uploaded_farms.json', 'w') as outfile:
        json.dump(list(uploaded_farms.values()), outfile)

if __name__ == "__main__":
    debug_main()