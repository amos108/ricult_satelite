from datetime import  datetime
import sentinelhub,numpy
import earth_explorer_api,time,os,shutil
import general_utills
import rasterio
from PIL import  Image
import skimage,boto3,general_algorithms,config_file
from pyproj import Proj, transform
## aws params
KEY        = config_file.KEY
SECRET_KEY = config_file.SECRET_KEY
REGION     = config_file.REGION
INSTANCE_ID = config_file.INSTANCE_ID


def get_all_scenes(polygon, dataset,start_datetime,end_datetime):
    mn = numpy.min(polygon, 0)
    mx = numpy.max(polygon, 0)
    search_bbox = sentinelhub.BBox(bbox=[mn[1], mn[0], mx[1], mx[0]], crs=sentinelhub.CRS.WGS84)
    start_str = start_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    end_str   = end_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    search_time_interval = (start_str, end_str)

    wfs_iterator = sentinelhub.WebFeatureService(search_bbox, search_time_interval,
                                     data_source=sentinelhub.DataSource.SENTINEL2_L1C,
                                     maxcc=1.0, instance_id=INSTANCE_ID)

    return {(tn+date +str(n)):(tn,date,n) for tn,date,n in wfs_iterator.get_tiles()}


def download_scene(val,temp_dir,dataset):
    curr_dir = temp_dir
    if os.path.exists(curr_dir):
        for the_file in os.listdir(curr_dir):
            file_path = os.path.join(curr_dir, the_file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
    else:
        os.makedirs(curr_dir)

    tile_name, t_time,n = val
    tile_req = sentinelhub.AwsTileRequest(tile=tile_name, time=t_time, aws_index=n, data_folder=temp_dir)

    tile_req.save_data()
    sdir = os.listdir(temp_dir)[0]
    os.rename(temp_dir+'/' + sdir, temp_dir + '/raw')

    tile_info = tile_req.get_aws_service().get_tile_info()

    cs = tile_info['tileGeometry']['crs']['properties']['name'].split(':')[-1]
    inProj = Proj(init='epsg:' + cs)
    outProj = Proj(init='epsg:4326')# same as wgs84

    min_lon_lat = numpy.min(tile_info['tileGeometry']['coordinates'], 0).min(0)
    max_lon_lat = numpy.min(tile_info['tileGeometry']['coordinates'], 0).max(0)
    min_lon_lat = transform(inProj, outProj,min_lon_lat[0],min_lon_lat[1])
    max_lon_lat = transform(inProj, outProj, max_lon_lat[0], max_lon_lat[1])

    date = datetime.strptime(t_time,'%Y-%m-%d')
    upload_downloaded_scene(min_lon_lat =min_lon_lat ,max_lon_lat = max_lon_lat,acquisitionDate = date.strftime('%Y-%m-%d'),dataset = dataset,raw_directory = temp_dir+'/raw')


def upload_downloaded_scene(min_lon_lat,max_lon_lat,acquisitionDate,bucket_name='ricult-development', raw_directory='temp',
                               dataset=None):
    s3 = boto3.resource('s3',aws_access_key_id =KEY,aws_secret_access_key=SECRET_KEY)
    bucket = s3.Bucket(bucket_name)

    lon_step = config_file.LON_STEP_SAVED
    lat_step = config_file.LAT_STEP_SAVED

    s3_key = 'satellite/raw_satellite_data/' +  dataset

    min_lat_r = int(min_lon_lat[1]/lat_step)*lat_step
    max_lat_r = int(max_lon_lat[1]/lat_step + 1)*lat_step

    min_lon_r = int(min_lon_lat[0]/lon_step)*lon_step
    max_lon_r = int(max_lon_lat[0]/lon_step + 1)*lon_step

    min_lat_r = round(min_lat_r,3)
    max_lat_r = round(max_lat_r,3)
    min_lon_r = round(min_lon_r,3)
    max_lon_r = round(max_lon_r,3)
    max_lon_r = round(max_lon_r,3)

    def crop_directory(source_dir, out_dir,polygon):
        for file in os.listdir(source_dir):
            if len(file.split('.')) < 2:
                pass
            elif file.split('.')[-1].lower() == 'tif':
                general_utills.crop_image(os.path.join(source_dir, file), os.path.join(out_dir, file), polygon)

            elif file.split('.')[-1].lower() == 'tiff':
                general_utills.crop_image(os.path.join(source_dir, file), os.path.join(out_dir, file), polygon)

            elif file.split('.')[-1].lower() == 'jp2':
                general_utills.crop_image(os.path.join(source_dir, file), os.path.join(out_dir, file), polygon)

            elif file.split('.')[-1].lower() == 'jpeg':
                pass
            else:
                shutil.copyfile(os.path.join(source_dir, file), os.path.join(out_dir, file))

    def proc_curr_latlon(inp):
        bucket = s3.Bucket(bucket_name)
        curr_lat, curr_lon,ii = inp
        curr_lat = round(curr_lat, 3)
        curr_lon = round(curr_lon, 3)
        tmp_dir = raw_directory + '/tmp' + str(ii)
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        polygon = [[curr_lat, curr_lon], [curr_lat + lat_step, curr_lon],
                   [curr_lat + lat_step, curr_lon + lon_step], [curr_lat, curr_lon + lon_step]]

        prefix = s3_key + '/' + str(curr_lat) + '_' + str(curr_lon) + '/' + acquisitionDate
        in_tmp_dir1 = tmp_dir + '/temp_1'
        in_tmp_dir2 = tmp_dir + '/temp_2'

        if os.path.exists(in_tmp_dir1):
            shutil.rmtree(in_tmp_dir1)

        if os.path.exists(in_tmp_dir2):
            shutil.rmtree(in_tmp_dir2)

        if general_utills.bucket_has_key(bucket, prefix):
            os.makedirs(in_tmp_dir1)
            os.makedirs(in_tmp_dir2)
            general_utills.download_dir_from_bucket(bucket, prefix, in_tmp_dir1)

        crop_directory(source_dir=raw_directory,out_dir=tmp_dir,polygon = polygon)

        try:
            if os.path.exists(in_tmp_dir1):
                earth_explorer_api.TransparentApi.merge_dirs(tmp_dir,[in_tmp_dir1])
                crop_directory(source_dir=tmp_dir, out_dir=in_tmp_dir2,polygon = polygon)
                general_algorithms.save_cload_image(in_tmp_dir2, dataset)
                general_algorithms.save_rgb_image(in_tmp_dir2, dataset)
            else:
                general_algorithms.save_cload_image(tmp_dir, dataset)
                general_algorithms.save_rgb_image(tmp_dir, dataset)
                in_tmp_dir2 = tmp_dir
        except:
            pass

        general_utills.upload_dir_to_bucket(in_tmp_dir2, bucket, prefix)

    args = []
    pp = 0
    for curr_lat in numpy.arange(min_lat_r,max_lat_r,lat_step):
        for curr_lon in numpy.arange(min_lon_r,max_lon_r,lon_step):
            pp+=1
            args.append((curr_lat,curr_lon,pp))

    if False:
        pool = ThreadPool(processes=10)
        pool.map(proc_curr_latlon, args)
        pool.close()
        pool.join()
    else:
        for arg in args:
            proc_curr_latlon(arg)

