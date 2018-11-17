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

# lt = 12.1#13.8
# ln = 100.1#100.3
# delta = 1e-5

#all_polygon = [[[lt-delta,ln-delta],[lt-delta,ln+delta],[lt+delta,ln+delta],[lt+delta,ln-delta]]]

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
        time.sleep(1)
    # all_scenes = remove_keys(all_scenes,history)


# instance must have ssm installed !!!!!
command_preamble = ["sh","-c"]
command_format = "cd /home/ec2-user; python3 download_partial_sentinel_hub.py {}"

all_scenes_keys  = set([' '.join([str(k) for k in key]) for key in all_scenes.values()])
batch_client = boto3.client('batch',aws_access_key_id =config_file.KEY,aws_secret_access_key=config_file.SECRET_KEY,region_name = config_file.REGION)
for key in list(all_scenes_keys)[0:1000]:
    command = command_format.format(key)

    response = batch_client.submit_job(
        jobName=key.replace(' ','_'),
        jobQueue='dwn',
        jobDefinition='downloader_job:2',
        parameters={
            'string': 'string'
        },
        containerOverrides={
            'vcpus': 1,
            'memory': config_file.MEMORY,
            'command': command_preamble + [command]
        },
        retryStrategy={
            'attempts': 10
        })