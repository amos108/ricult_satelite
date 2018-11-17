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

aws_mode = False

import sentnel_hub_api
tran_api = sentnel_hub_api
all_data_sets = {'sentinel': 'SENTINEL_2A'}

if False:
    with open('thailand.json') as fid:
        thailand = json.load(fid)

    all_polygon = []
    for ii, tai_feat in enumerate(thailand['features']):
        for polygon in tai_feat['geometry']['coordinates']:
            if len(polygon) == 1:
                polygon = polygon[0]
            all_polygon.append([[p[1], p[0]] for p in polygon])
else:
    from shapely.geometry import box
    import shapely

    def fishnet(geometry, threshold):
        bounds = geometry.bounds
        xmin = int(bounds[0] // threshold)
        xmax = int(bounds[2] // threshold)
        ymin = int(bounds[1] // threshold)
        ymax = int(bounds[3] // threshold)
        ncols = int(xmax - xmin + 1)
        nrows = int(ymax - ymin + 1)
        result = []
        for i in range(xmin, xmax+1):
            for j in range(ymin, ymax+1):
                b = box(i*threshold, j*threshold, (i+1)*threshold, (j+1)*threshold)
                g = geometry.intersection(b)
                if g.is_empty:
                    continue
                result.append(g)
        return result

    with open('pakistan.json') as fid:
        pakistan = json.load(fid)

    polygon = pakistan['geometry']['coordinates'][0]
    polygon = Polygon(polygon)
    a_polygons = fishnet(polygon,1)
    all_polygon = []
    for polygons in a_polygons:
        if type(polygons) == shapely.geometry.polygon.Polygon:
            polygons = [polygons]
        for polygon in list(polygons):
            xx,yy = polygon.exterior.coords.xy
            pol = [[y,x] for x,y in zip(xx,yy)]
            all_polygon.append(pol)

# lt = 13.8
# ln = 100.3
# delta = 1e-5
#
# all_polygon = [[[lt-delta,ln-delta],[lt-delta,ln+delta],[lt+delta,ln+delta],[lt+delta,ln-delta]]]

end_datetime = datetime.now()
start_datetime = end_datetime - timedelta(days=7)

if True:
    all_scenes = {}
    for ee,poly in enumerate(all_polygon):
        print(ee/len(all_polygon))
        poly = Polygon(poly)
        xpoly = [[x, y] for x, y in zip(poly.exterior.xy[0], poly.exterior.xy[1])]
        poly = xpoly[0:-1]
        for dataset in all_data_sets:
            all_scenes.update(tran_api.get_all_scenes(poly, dataset,start_datetime = start_datetime,end_datetime=end_datetime))
        time.sleep(1)
    all_scenes = remove_keys(all_scenes,history)


ec2_client = boto3.client('ec2',config_file.REGION,aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY)
def get_instance_ids(states =  ['running', 'stopped']):
    ins_desc = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': ['downloader_instance']},
                                                      {'Name': 'instance-state-name',
                                                       'Values':states}])
    instances = []
    for ires in ins_desc['Reservations']:
        instances+=ires['Instances']
    return [instance['InstanceId'] for instance in instances]

waiter = ec2_client.get_waiter('instance_status_ok')
stop_waiter = ec2_client.get_waiter('instance_stopped')

instance_ids_to_start = get_instance_ids(['stopped'])
if instance_ids_to_start!=[]:
    ec2_client.start_instances(InstanceIds=instance_ids_to_start)
    waiter.wait(InstanceIds=instance_ids_to_start)

ssm_client = boto3.client('ssm', config_file.REGION, aws_access_key_id=config_file.KEY,
                          aws_secret_access_key=config_file.SECRET_KEY)

command_format = '''
cd /home/ec2-user
python3 download_partial_sentinel_hub.py {}
'''
# instance must have ssm installed !!!!!



all_scenes_keys  = set([' '.join([str(k) for k in key]) for key in all_scenes.values()])

success_list = set([])
pending_list = set([])
failed_list  = set([])
all_commands  = []

def update_all_commands():
    for cmd in all_commands:
        if cmd['Status'] not in ['Success','Failed']:
            try:
                cmd_list = ssm_client.list_commands(CommandId=cmd['CommandId'])['Commands']
                cmd['Status'] = cmd_list[0]['Status']
            except:
                time.sleep(1)

    all_commands_new = []
    for cmd in all_commands:
        cmd_list = ssm_client.list_commands(CommandId=cmd['CommandId'])['Commands']
        all_commands_new.append(cmd_list[0])


def get_unavail_instances():
    update_all_commands()
    running_commands = [cmd for cmd in all_commands if cmd['Status'] in ['InProgress', 'Pending']]

    unavail_ids = []
    for command in running_commands:
        unavail_ids+=command['InstanceIds']
    return unavail_ids

def send_commands(missions):
    running_ids             = get_instance_ids(['running'])
    avail_running_instances = list(set(running_ids) - set(get_unavail_instances()))

    if avail_running_instances!=[]:
        #ec2_client.reboot_instances(InstanceIds=avail_running_instances)
        waiter.wait(InstanceIds=avail_running_instances)

    for a_ins, key in zip(avail_running_instances, missions):
        command =  command_format.format(key)
        response = ssm_client.send_command(
            InstanceIds=[a_ins],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': [command]},
            Comment= key)
        all_commands.append(response['Command'])

def monitor_progress():
    update_all_commands()
    success_list = set([cmd['Comment'] for cmd in all_commands if cmd['Status']=='Success'])
    pending_list = set([cmd['Comment'] for cmd in all_commands if cmd['Status'] in ['InProgress','Pending']])
    failed_list  = set([cmd['Comment'] for cmd in all_commands if cmd['Status']=='Failed'])

    print('success_list({}):'.format(len(success_list)),success_list)
    print('pending_list({}):'.format(len(pending_list)),pending_list)
    print('failed_list({}):'.format(len(failed_list)), failed_list)
    print('time from start {}'.format(time.time() - start_time))
    return success_list,pending_list,failed_list

while len(success_list)<len(all_scenes_keys):
    #waiter.wait(InstanceIds=instance_ids)

    success_list, pending_list, failed_list = monitor_progress()

    missions = list(all_scenes_keys - pending_list - success_list)
    print(len(missions))
    try:
        send_commands(missions)
    except:
        pass
    time.sleep(30)

with open(history_path, 'w+') as outfile:
    json.dump(list(success_list),outfile)

instance_ids = get_instance_ids(['running'])
if instance_ids!=[]:
    ec2_client.stop_instances(InstanceIds=instance_ids)