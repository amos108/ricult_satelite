import requests,json
import datetime
from datetime import timedelta
import os,numpy
import zipfile,gzip
import time
from requests import get  
from urllib.request import urlretrieve
import tarfile,re
from PIL import Image
from urllib import parse
from urllib.request import urlopen
import shutil
import zipfile
import tarfile
import general_utills
import boto3
from datetime import datetime
from shapely.geometry.polygon import Polygon
import config_file
import general_algorithms

USER     = config_file.EE_USER
PASSWORD = config_file.EE_PASSWORD


AUTH_TYPE = "EROS"
CATALOG_ID = "EE"
OP_STR = 'https://earthexplorer.usgs.gov/inventory/json/v/1.4.0/%s?jsonRequest=%s'

LANDSAT_8_DATASET = 'LANDSAT_8_C1'
LANDSAT_7_DATASET = 'LANDSAT_ETM_C1'
SENTINAL_DATASET  = 'SENTINEL_2A'

ALL_DATA_SETS = {'landsat-8':LANDSAT_8_DATASET,'landsat-7':LANDSAT_7_DATASET,'sentinel':SENTINAL_DATASET}#

## aws params
KEY        = config_file.KEY
SECRET_KEY = config_file.SECRET_KEY
REGION     = config_file.REGION

LON_STEP = config_file.LON_STEP_SAVED
LAT_STEP = config_file.LAT_STEP_SAVED

def download(url, filename):
    try:
        urlretrieve(url,filename)
    except:
        pass ## #TODO: logger

class MainApi():
    def __init__(self,user = USER,password=PASSWORD,auth_type=AUTH_TYPE,catalog_id = CATALOG_ID,):
        self.user = user 
        self.password = password 
        self.auth_type = auth_type 
        self.catalog_id = catalog_id 
        self.api_key = None

    @ staticmethod
    def get_url(action_name,param_dict):
        def dict2str(dd):
            dd_str = '{'
            for k,v in dd.items():
                dd_str += '"'+ str(k)+'": "'
                if type(v)==dict:
                    dd_str = dd_str[0:-1]
                    dd_str +=dict2str(v)+ ',' 
                elif type(v)==list:
                    dd_str = dd_str[0:-1]
                    dd_str +="[" + ''.join(['"' + str(cv) +'",' for cv in v])[0:-1] + "]"+ ','
                elif type(v)==str:
                    dd_str += str(v) + '",' 
                else:
                    dd_str = dd_str[0:-1]
                    dd_str += str(v) + ',' 

            dd_str =dd_str[0:-1]
            dd_str += '}'
            return dd_str
        
        url = 'https://earthexplorer.usgs.gov/inventory/json/v/1.4.0/%s?jsonRequest=%s'%(action_name,dict2str(param_dict))
        return url

    def login(self):
        login_dict = dict(username=self.user,password = self.password,authType = self.auth_type,catalogId = self.catalog_id)
        x = requests.post(MainApi.get_url('login',login_dict))
        json1_data = json.loads(x.content.decode())
        self.api_key = json1_data['data']

    def logout(self):
        if self.api_key ==None:
            return
        logout_dict = dict(apiKey = self.api_key)
        requests.post(MainApi.get_url('logout',logout_dict))

    def get_all_datasets(self):
        self.login()
        dataset_dict = dict(apiKey = self.api_key)
        x = requests.post(MainApi.get_url('datasets',dataset_dict))
        json1_data = json.loads(x.content.decode())
        self.logout()
        return json1_data['data']

    def get_polygon(self,dataset_name,polygon,start_datetime=None,end_datetime=None,max_results = 10,sort_order = 'DESC'):
        min_p = numpy.inf
        max_p = -numpy.inf
        for p in polygon:
            min_p = numpy.minimum(p,min_p)
            max_p = numpy.maximum(p,max_p)

        all_search_res = self.search(dataset_name,list(min_p),list(max_p),start_datetime,end_datetime,sort_order=sort_order)

        if all_search_res==[]:
            return {}
        else:
            return {sr['entityId']:sr for sr in all_search_res}


    def search(self,dataset_name,lower_left=None,upper_right=None,start_datetime=None,end_datetime=None,max_results = 10,sort_order = 'DESC'):
        self.login()

        search_dict = {}
        search_dict["datasetName"]   = dataset_name
        if lower_left is not None and upper_right is not None:
            search_dict["spatialFilter"] = {'filterType':'mbr','lowerLeft':dict(latitude=lower_left[0],longitude=lower_left[1])
                                        ,'upperRight':dict(latitude=upper_right[0],longitude=upper_right[1])}
        if start_datetime is not None and end_datetime is not None:
            search_dict["temporalFilter"] = {"startDate": start_datetime.strftime('%Y-%m-%d'),"endDate": end_datetime.strftime('%Y-%m-%d')}
            search_dict["maxResults"]     = 50000
        else:
            search_dict["maxResults"]     = max_results
        search_dict["startingNumber"] = 1
        search_dict["sortOrder"]     = sort_order
        search_dict["apiKey"]        = self.api_key

        x = requests.post(MainApi.get_url('search',search_dict))
        json1_data = json.loads(x.content.decode())
        self.logout()
        try:
            results = json1_data['data']['results']
        except:
            results = [] # todo: add logger
        return results

    def get_products(self,dataset_name,scene_id,products=None):
        self.login()
        product_dict = {}
        product_dict["datasetName"]   = dataset_name
        product_dict["apiKey"]        = self.api_key
        product_dict["entityIds"]      = scene_id


        x = requests.post(MainApi.get_url('downloadoptions',product_dict))
        json1_data = json.loads(x.content.decode())
        all_products = []
        all_ent_ids  = []

        for cproducts in json1_data['data']:
            for coption in cproducts['downloadOptions']:
                if (products is None) or (coption['downloadCode'] in products):
                    all_ent_ids.append(cproducts['entityId'])
                    all_products.append(coption['downloadCode'])

        download_dict = {}
        download_dict["apiKey"]        = self.api_key
        download_dict["datasetName"]  = dataset_name
        download_dict["products"]     = all_products
        download_dict["entityIds"]    = all_ent_ids

        requests.post(MainApi.get_url('clearorder',{'apiKey':self.api_key}))
        while True:
            x = requests.post(MainApi.get_url('download',download_dict))
            json1_data = json.loads(x.content.decode())
            if (json1_data['errorCode'] == 'DOWNLOAD_RATE_LIMIT'):
                print('cant download yet due to lame access pausing for minute')
                time.sleep(60)
            else:
                break

        requests.post(MainApi.get_url('clearorder',{'apiKey':self.api_key}))
        self.logout()
        return json1_data['data']

    @staticmethod
    def download_all(products,dir):
        for prod in products:
            split  = parse.urlsplit(prod['url'])
            file_name = dir + '/' + prod['product']
            download(prod['url'],file_name)

class TransparentApi():
    def __init__(self,lon_step = LON_STEP,lat_step = LAT_STEP):
        self.api = api = MainApi()
        lon_step = self.lon_step = LON_STEP
        lat_step = self.lat_step = LAT_STEP

    def get_results(self,polygon,dataset,start_datetime,end_datetime,max_results = 1):
        scenes = self.api.get_polygon(ALL_DATA_SETS[dataset],polygon,start_datetime,end_datetime,max_results)
        return scenes

    def get_all_scenes(self,polygon,dataset,start_datetime=None,end_datetime=None,max_results=1):
        scenes = self.get_results(polygon,dataset,start_datetime,end_datetime,max_results)
        for scene in scenes.values():
            scene['dataset'] = dataset
        return scenes

    def download_scene(self,scene,sc_id,temp_dir = 'temp'):
        api = self.api
        dataset = scene['dataset']
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

        pr = api.get_products(ALL_DATA_SETS[dataset],sc_id)
        MainApi.download_all(pr,curr_dir)
        download(scene['fgdcMetadataUrl'],curr_dir + '/metadata.xml')

    def proccess_raw_downloads(self,bucket_name = 'ricult-development',scene = {},raw_directory='temp',dataset=None):


        if dataset == None:
            dataset = scene['dataset']

        if dataset =='landsat-8' or dataset =='landsat-7':
            zip_ref = zipfile.ZipFile(raw_directory+'/FR_BUND', 'r')
            zip_ref.extractall(raw_directory)
            zip_ref.close()

            tar = tarfile.open(raw_directory+'/STANDARD')
            tar.extractall(path=raw_directory)
            tar.close()


            os.remove(raw_directory+'/FR_BUND')
            os.remove(raw_directory+'/STANDARD')

            for file in os.listdir(raw_directory):
                if (file.split('.')[-1].lower() == 'tif') or (file.split('.')[-1].lower() == 'txt'):
                    nfile = file.split('_')[-1]
                    os.rename(raw_directory + '/' + file, raw_directory + '/' + nfile)

            b1_path = raw_directory + '/B1.TIF'
            with Image.open(b1_path) as b1_image:
                b1_size = b1_image.size


        if dataset =='sentinel':
            os.rename(raw_directory + '/FRB', raw_directory + '/FRB.tiff')
            zip_ref = zipfile.ZipFile(raw_directory+'/STANDARD', 'r')
            zip_ref.extractall(raw_directory)
            zip_ref.close()
            
            zip_name = [file for file in os.listdir(raw_directory) if (file.split('.')[-1] == 'SAFE')][0]

            zip_dir_name = raw_directory +'/' + zip_name
            for fname in os.listdir(zip_dir_name) :
                if fname.split('.')[-1] == 'xml':
                    shutil.copyfile(zip_dir_name + '/' + fname, raw_directory+ '/' + fname)

            zip_name =  zip_dir_name +'/GRANULE'

            zip_name = zip_name + '/' + os.listdir(zip_name)[0]
            images_dir = zip_name + '/IMG_DATA'
            for image_fname in os.listdir(images_dir) :
                shutil.copyfile(images_dir + '/' + image_fname, raw_directory+ '/' + image_fname.split('_')[-1])

            os.remove(raw_directory+'/STANDARD')
            general_utills.rmtree(zip_dir_name)



        min_lon_lat = numpy.array(scene['spatialFootprint']['coordinates']).min(1)[0]
        max_lon_lat = numpy.array(scene['spatialFootprint']['coordinates']).max(1)[0]

        self.upload_downloaded_scene(min_lon_lat=min_lon_lat,max_lon_lat=max_lon_lat,acquisitionDate=scene['acquisitionDate'],bucket_name=bucket_name ,
                                     raw_directory=raw_directory,dataset=dataset)


    def upload_downloaded_scene(self,min_lon_lat,max_lon_lat,acquisitionDate,bucket_name='ricult-development', raw_directory='temp',
                                   dataset=None):
        s3 = boto3.resource('s3',aws_access_key_id =KEY,aws_secret_access_key=SECRET_KEY)
        bucket = s3.Bucket(bucket_name)

        lon_step = self.lon_step
        lat_step = self.lat_step
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

            if os.path.exists(in_tmp_dir1):
                TransparentApi.merge_dirs(tmp_dir,[in_tmp_dir1])
                crop_directory(source_dir=tmp_dir, out_dir=in_tmp_dir2,polygon = polygon)
                general_algorithms.save_cload_image(tmp_dir, dataset)
                general_utills.upload_dir_to_bucket(in_tmp_dir2, bucket, prefix)
            else:
                general_algorithms.save_cload_image(tmp_dir, dataset)
                general_utills.upload_dir_to_bucket(tmp_dir, bucket, prefix)

        args = []
        pp = 0
        for curr_lat in numpy.arange(min_lat_r,max_lat_r,lat_step):
            for curr_lon in numpy.arange(min_lon_r,max_lon_r,lon_step):
                pp+=1
                args.append((curr_lat,curr_lon,pp))

        for arg in args:
            proc_curr_latlon(arg)


    def get_raw_dirs_groups(self,polygon,dataset,data_bucket,farm,max_results = 1):
        lon_step = self.lon_step
        lat_step = self.lat_step

        min_lat_lon = numpy.array(polygon).min(0)
        max_lat_lon = numpy.array(polygon).max(0)
        farm_polygon = Polygon(polygon)

        raw_dirs = {}
        min_lat_r = int(min_lat_lon[0]/lat_step)*lat_step
        max_lat_r = int(max_lat_lon[0]/lat_step + 1)*lat_step

        min_lon_r = int(min_lat_lon[1]/lon_step)*lon_step
        max_lon_r = int(min_lat_lon[1]/lon_step + 1)*lon_step

        min_lat_r = round(min_lat_r,3)
        max_lat_r = round(max_lat_r,3)
        min_lon_r = round(min_lon_r,3)
        max_lon_r = round(max_lon_r,3)

        last_updated =  farm[dataset]['last_updated']
        last_updated = datetime.strptime(farm[dataset]['last_updated'], '%Y-%m-%d')


        for curr_lat in numpy.arange(min_lat_r,max_lat_r,lat_step):
            for curr_lon in numpy.arange(min_lon_r,max_lon_r,lon_step):
                curr_lat = round(curr_lat,3)
                curr_lon = round(curr_lon,3)
                grid_pol = Polygon([[curr_lat,curr_lon],[curr_lat+lat_step,curr_lon],[curr_lat+lat_step,curr_lon+lon_step],[curr_lat,curr_lon+lon_step]])

                if grid_pol.intersects(farm_polygon):
                    prefix = 'satellite/raw_satellite_data/'+ dataset + '/'+str(curr_lat) + '_' +str(curr_lon)+ '/'
                    all_files = list(data_bucket.objects.filter(Prefix= prefix).all())
                    dates = set()
                    for file in all_files:
                        splited = file.key[len(prefix)::].split('/')
                        if len(splited)>0:
                            cdate = datetime.strptime(splited[0], '%Y-%m-%d')
                            if last_updated<cdate:
                                dates.add(cdate)

                    
                    for date in dates:
                        if date not in raw_dirs:
                            raw_dirs[date] = []
                        raw_dirs[date].append((curr_lat,curr_lon))

        rel_keys = sorted(raw_dirs.keys())[-max_results::]
        oraw_dirs = {rk:raw_dirs[rk] for rk in rel_keys}
        return oraw_dirs

    def process_farm(self,farm,dataset,data_bucket,upload_bucket,max_results=1,root_dir = '/tmp'):
        if dataset not in farm:
            farm[dataset] = {}
            farm[dataset]['last_updated'] = '0001-1-1'

        raw_dir_groups = self.get_raw_dirs_groups(farm['coordinates'],dataset,data_bucket,farm,max_results)
        if len(raw_dir_groups)==0:
            return

        processed_directory =  os.path.join(root_dir,'proc_dir')
        for date,raw_dir_group in raw_dir_groups.items():
            self.process_raw_dir_groups(farm, dataset, data_bucket, upload_bucket,date,raw_dir_group,root_dir)

    def process_raw_dir_groups(self,farm, dataset, data_bucket, upload_bucket,date,raw_dir_group,root_dir='/tmp'):
        if dataset not in farm:
            farm[dataset] = {}
            farm[dataset]['last_updated'] = '0001-1-1'

        date_str = date.strftime('%Y-%m-%d')
        upload_key = 'satellite/' + str(farm['id']) + '/' + dataset + '/' + date_str

        processed_directory = os.path.join(root_dir, 'proc_dir')
        if os.path.exists(processed_directory):
            shutil.rmtree(processed_directory)
        os.makedirs(processed_directory)

        lat, lon = raw_dir_group[0]
        download_key = 'satellite/raw_satellite_data/' + dataset + '/' + str(lat) + '_' + str(lon) + '/' + date_str
        general_utills.download_dir_from_bucket(data_bucket, download_key, processed_directory)
        tmp_proc_dir = os.path.join(root_dir, 'tmp_proc_dir')
        for lat, lon in raw_dir_group[1::]:
            if os.path.exists(tmp_proc_dir):
                shutil.rmtree(tmp_proc_dir)
            os.makedirs(tmp_proc_dir)
            download_key = 'satellite/raw_satellite_data/' + dataset + '/' + str(lat) + '_' + str(
                lon) + '/' + date_str
            general_utills.download_dir_from_bucket(data_bucket, download_key, tmp_proc_dir)
            TransparentApi.merge_dirs(processed_directory, [tmp_proc_dir])

        TransparentApi.proccess_farm_from_dir(processed_directory, farm, dataset)
        if os.path.exists(tmp_proc_dir):
            shutil.rmtree(tmp_proc_dir)

        general_utills.upload_dir_to_bucket(processed_directory, upload_bucket, upload_key)
        image_files = [fname for fname in os.listdir(processed_directory) if
                       fname.split('.')[-1].lower() in ['jp2', 'tiff', 'tif']]
        if image_files != []:
            farm['directory_list'].append(dataset + '/' + date_str)

        farm_date = datetime.strptime(farm[dataset]['last_updated'],'%Y-%m-%d')
        farm[dataset]['last_updated'] = max([farm_date,date]).strftime('%Y-%m-%d')

    @staticmethod
    def proccess_farm_from_dir(processed_directory,farm,dataset):
        farm_json_data = {}
        for file in os.listdir(processed_directory):

            if file.split('.')[-1].lower()=='tif':
                print(file)
                general_utills.crop_image(processed_directory+'/'+file,processed_directory+'/'+file,farm['coordinates'])
            elif file.split('.')[-1].lower()=='tiff':
                print(file)
                general_utills.crop_image(processed_directory+'/'+file,processed_directory+'/'+file,farm['coordinates'])
            elif file.split('.')[-1].lower()=='jp2':
                print(file)
                general_utills.crop_image(processed_directory+'/'+file,processed_directory+'/'+file,farm['coordinates'])



        if dataset=='landsat-8' or dataset=='landsat-7' :
            try:
                b3 = Image.open(processed_directory + '/B3.TIF')
                b4 = Image.open(processed_directory + '/B4.TIF')
                b5 = Image.open(processed_directory + '/B5.TIF')

                ndvi_array = general_utills.get_ndvi_image(b4,b5,b3)
                b3.close()
                b4.close()
                b5.close()
                ndvi = Image.fromarray(ndvi_array,'L')
                ndvi.save(processed_directory +'/ndvi.jpg')
                ndvi.close()
                general_utills.save_tiff_of_image(ndvi_array,processed_directory +'/ndvi.tiff',processed_directory + '/B4.TIF')
                general_utills.create_tiff_image_json(processed_directory +'/RT.tif',processed_directory +'/rgb.json',farm['coordinates'])
            except:
                pass

        if dataset=='sentinel':
            try:
                b3 = Image.open(processed_directory + '/B03.jp2')
                b4 = Image.open(processed_directory + '/B04.jp2')
                b8 = Image.open(processed_directory + '/B08.jp2')

                ndvi_array = general_utills.get_ndvi_image(b4,b8,b3)
                b3.close()
                b4.close()
                b8.close()
                ndvi = Image.fromarray(ndvi_array,'L')
                ndvi.save(processed_directory +'/ndvi.jpg')
                ndvi.close()
                general_utills.save_tiff_of_image(ndvi_array,processed_directory +'/ndvi.tiff',processed_directory + '/B04.jp2')
                general_utills.create_tiff_image_json(processed_directory +'/FRB.tiff',processed_directory +'/rgb.json',farm['coordinates'])

            except:
                pass


        if os.path.exists(processed_directory +'/cloud_image.tiff'):
            general_utills.create_tiff_image_json(processed_directory +'/cloud_image.tiff',processed_directory +'/cloud_image.json',farm['coordinates'])
            farm_json_data['cload_percent'] = general_algorithms.get_cload_percent(processed_directory + '/cloud_image.tiff')
        else:
            farm_json_data['cload_percent'] = 1

        if os.path.exists(processed_directory +'/ndvi.tiff'):
            general_utills.create_tiff_image_json(processed_directory +'/ndvi.tiff',processed_directory +'/ndvi.json',farm['coordinates'])

        with open(processed_directory +'/farm_data.json', 'w') as outfile:
            json.dump(farm_json_data, outfile)

    @staticmethod
    def merge_dirs(orig_dir,dirs):
        for dir in dirs:
            for file in os.listdir(dir):
                if file not in os.listdir(orig_dir):
                    shutil.copyfile(os.path.join(dir,file),os.path.join(orig_dir,file))
                    continue

                if file.split('.')[-1].lower()=='tif':
                    general_utills.merge_image(os.path.join(orig_dir,file),[os.path.join(dir,file),os.path.join(orig_dir,file)])

                elif file.split('.')[-1].lower()=='tiff':
                    general_utills.merge_image(os.path.join(orig_dir,file),[os.path.join(dir,file),os.path.join(orig_dir,file)])

                elif file.split('.')[-1].lower()=='jp2':
                    general_utills.merge_image(os.path.join(orig_dir,file),[os.path.join(dir,file),os.path.join(orig_dir,file)])

                # elif file.split('.')[-1].lower()=='jpeg':
                #     pass
                # else:
                #     count = 0
                #     dst_file = os.path.join(orig_dir,file)
                #     while os.path.exists(dst_file):
                #         dst_file = os.path.join(orig_dir,str(count) + file)
                #         count += 1
                #     shutil.copyfile(os.path.join(dir,file),dst_file)
        

if __name__ =='__main__':
    import general_utills
    import os
    polygon = general_utills.read_polygons('coordinates.txt')

    t_api = TransparentApi()

    t_api.proccess_raw_downloads(r'D:\raw_ee\sentinel\T31UDQ\2018-07-23','sentinel')