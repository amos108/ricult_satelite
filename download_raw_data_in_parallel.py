import requests, os
import sys
import json
from shapely.geometry import Polygon
import boto3, time
import config_file
from datetime import datetime
from datetime import timedelta
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

aws_mode = False

if aws_mode:
    import aws_api

    tran_api = aws_api.TransparentApi()
    all_data_sets = aws_api.ALL_DATA_SETS
else:
    import earth_explorer_api

    tran_api = earth_explorer_api.TransparentApi()
    all_data_sets = earth_explorer_api.ALL_DATA_SETS
all_data_sets = {'sentinel': 'SENTINEL_2A'}

all_polygon = []
for ii, tai_feat in enumerate(thailand['features']):
    for polygon in tai_feat['geometry']['coordinates']:
        if len(polygon) == 1:
            polygon = polygon[0]
        all_polygon.append([[p[1], p[0]] for p in polygon])


end_datetime = datetime.now()
start_datetime = end_datetime - timedelta(days=365*2)

if True:
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

ec2_client = boto3.client('ec2',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)
ins_desc = ec2_client.describe_instances(Filters=[{'Name':'tag:Name','Values':['downloader_instance']},{'Name':'instance-state-name','Values':['running','stopped']}])
instances = []
for ires in ins_desc['Reservations']:
    instances+=ires['Instances']
instance_ids = [instance['InstanceId'] for instance in instances]
instance_ids_to_start = [instance['InstanceId'] for instance in instances if instance['State']['Name']=='stopped']
if instance_ids_to_start!=[]:
    ec2_client.start_instances(InstanceIds=instance_ids_to_start)
waiter = ec2_client.get_waiter('instance_status_ok')
stop_waiter = ec2_client.get_waiter('instance_stopped')
waiter.wait(InstanceIds=instance_ids)
ssm_client = boto3.client('ssm', config_file.REGION, aws_access_key_id=config_file.KEY,
                          aws_secret_access_key=config_file.SECRET_KEY)

command_format = '''
cd /home/ec2-user
python3 download_partial2.py {}
'''
# instance must have ssm installed !!!!!

all_scenes_keys  = set(list(all_scenes.keys()))

n_success = 0
success_list = set([])
pending_list = set([])
failed_list  = set([])

while n_success<len(all_scenes_keys):
    waiter.wait(InstanceIds=instance_ids)
    tot_cmnds = []
    valid_instance_ids = []
    for id in instance_ids:
        try:
            curr_commands = ssm_client.list_commands(InstanceId=id)['Commands']
            running_commands = [cmd for cmd in curr_commands if cmd['Status'] in ['InProgress', 'Pending']]

            tot_cmnds += curr_commands
            if running_commands == []:
                valid_instance_ids.append(id)
        except:
            pass

    try:
        ec2_client.stop_instances(InstanceIds=valid_instance_ids)
        stop_waiter.wait(InstanceIds=valid_instance_ids)
        ec2_client.start_instances(InstanceIds=valid_instance_ids)
        waiter.wait(InstanceIds=valid_instance_ids)
    except:
        pass

    time.sleep(60)
    print(len(valid_instance_ids))
    success_list = success_list.union(set([cmd['Comment'] for cmd in tot_cmnds if cmd['Status']=='Success']))
    pending_list = set([cmd['Comment'] for cmd in tot_cmnds if cmd['Status'] in ['InProgress','Pending']])
    failed_list = failed_list.union(set([cmd['Comment'] for cmd in tot_cmnds if cmd['Status']=='Failed']))

    n_success = len(success_list)
    print('success_list({}):'.format(len(success_list)),success_list)
    print('pending_list({}):'.format(len(pending_list)),pending_list)
    print('failed_list({}):'.format(len(failed_list)), failed_list)

    all_avail_scenes_keys = list(all_scenes_keys - pending_list-success_list)
    print('left to downloaded {} scenes out of {} scenes'.format(len(all_avail_scenes_keys),
                                                                 len(all_scenes_keys)))
    print('time from start {}'.format(time.time() - start_time))


    for id in valid_instance_ids:
        if all_avail_scenes_keys ==[]:
            time.sleep(60)
            break
        key = all_avail_scenes_keys[0]

        try:
            command = command_format.format(key)
            response = ssm_client.send_command(
                    InstanceIds=[id],
                    DocumentName="AWS-RunShellScript",
                    Parameters={'commands': [command]},
                    Comment = key
                    )
            if key in all_avail_scenes_keys:
                all_avail_scenes_keys.remove(key)
            time.sleep(60)

        except:
            print('command wasn''t sent continue next instance')
            if key not in all_avail_scenes_keys:
                all_avail_scenes_keys.append(key)
            time.sleep(60)


with open(history_path, 'w+') as outfile:
    json.dump(list(success_list),outfile)

ec2_client.stop_instances(InstanceIds=instance_ids)