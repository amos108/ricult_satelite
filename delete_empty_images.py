import boto3,config_file
import json,rasterio

s3 = boto3.resource('s3', aws_access_key_id=config_file.KEY, aws_secret_access_key=config_file.SECRET_KEY)

bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
prefix = 'satellite/raw_satellite_data/sentinel/'
objs = list(bucket.objects.filter(Prefix= prefix).all())



def delete_directory(direct):
    for obj in objs:
        if '/'.join(obj.key.split('/')[0:-1]) == direct:
            obj.delete()

with open('error_folders.json') as fid:
    error_folders = json.load(fid)

with open('bugs.json') as fid:
    bugs = json.load(fid)


rel_objs = [obj for obj in objs if obj.key.split('/')[-1] == 'B04.jp2']
for obj in rel_objs:
    try:
        bucket.download_file(obj.key,'B04.jp2')

        with rasterio.open('B04.jp2') as fop:
            b4 = fop.read()

        if b4.max()==0:
            direct = '/'.join(obj.key.split('/')[0:-1])
            delete_directory(direct)
            error_folders.append(direct)
    except:
        bugs.append(direct)

with open('error_folders.json','w+') as fid:
    json.dump(error_folders,fid)

with open('bugs.json','w+') as fid:
    json.dump(bugs, fid)