import requests, os
import sys
import json
from shapely.geometry import Polygon
import boto3, time
import config_file
from datetime import datetime
from datetime import timedelta
import copy
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


all_command_ids = []
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


ec2_client = boto3.client('ec2',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)
ec2 = boto3.resource('ec2',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)
ssm_client = boto3.client('ssm', config_file.REGION, aws_access_key_id=config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)


waiter = ec2_client.get_waiter('instance_status_ok')

command_format = '''   
cd /home/ec2-user
python3 download_partial2.py {}
'''


def download_scene(scene):
    command = command_format.format(scene)
    new_inst = ec2.create_instances(ImageId='ami-0f3eecbf232a82546', InstanceType='t2.micro',MinCount=1, MaxCount=1, IamInstanceProfile={ 'Name': 's3fullaccess'},
                                    TagSpecifications=[{'ResourceType': 'instance','Tags': [{'Key':'Name','Value':'downloader'}]}],InstanceInitiatedShutdownBehavior='terminate',
                                    BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {"VolumeSize": 25}}])[0]

    new_inst.wait_until_running()
    waiter.wait(InstanceIds = [new_inst.id])
    response = ssm_client.send_command(
        InstanceIds=[new_inst.id],
        DocumentName="AWS-RunShellScript",
        Parameters={'commands': [command]},
        Comment=scene
    )
    all_command_ids.append(response['Command']['CommandId'])


def turn_off_unused_instances():
    all_commands = [ssm_client.list_commands(CommandId=cid)['Commands'][0] for cid in all_command_ids]
    not_running_commands = [cmd for cmd in all_commands if cmd['Status'] not in ['InProgress', 'Pending']]

    all_ids = []
    for command in not_running_commands:
        all_ids+=command['InstanceIds']

    if all_ids == []:
        return

    ins_desc = ec2_client.describe_instances(InstanceIds=all_ids,Filters = [{'Name': 'instance-state-name', 'Values': ['running']}])
    instances = []
    for ires in ins_desc['Reservations']:
        instances += ires['Instances']

    instance_ids = [instance['InstanceId'] for instance in instances]
    if instance_ids!=[]:
        ec2_client.terminate_instances(InstanceIds=instance_ids)

def print_progress():
    all_commands = [ssm_client.list_commands(CommandId=cid)['Commands'][0] for cid in all_command_ids]
    success_list = set([cmd['Comment'] for cmd in all_commands if cmd['Status']=='Success'])
    pending_list = set([cmd['Comment'] for cmd in all_commands if cmd['Status'] in ['InProgress','Pending']])
    failed_list  = set([cmd['Comment'] for cmd in all_commands if cmd['Status']=='Failed'])

    print('success_list({}):'.format(len(success_list)),success_list)
    print('pending_list({}):'.format(len(pending_list)),pending_list)
    print('failed_list({}):'.format(len(failed_list)), failed_list)
    print('time from start {}'.format(time.time() - start_time))


all_scenes_keys  = set(list(all_scenes.keys()))
all_avail_scenes_keys = copy.deepcopy(all_scenes_keys)

while True:

    for scene in list(all_avail_scenes_keys):
        turn_off_unused_instances()
        download_scene(scene)
        print_progress()

    all_avail_scenes_keys = all_scenes_keys - pending_list-success_list
    print('left to downloaded {} scenes out of {} scenes'.format(len(all_avail_scenes_keys),len(all_scenes_keys)))


    if (len(all_avail_scenes_keys) == 0):
        if (len(pending_list)==0):
            break
        else:
            time.sleep(60)


with open(history_path, 'w+') as outfile:
    json.dump(list(success_list),outfile)

