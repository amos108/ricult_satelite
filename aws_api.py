from boto3.session import Session as boto3_session
import boto3
from boto import s3
from boto.s3.connection import S3Connection
import os,numpy
from requests import get  
import gzip,copy
from osgeo import ogr
import re,datetime
from lxml import html
from urllib.request import urlretrieve
import os
import general_utills
from PIL import Image

import shutil

KEY = 'AKIAJLCP2NJX5QKOPK7Q'
SECRET_KEY = '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'

ALL_DATA_SETS = {'sentinel':'sentinel','landsat-8':'landsat-8'}


def download(url, file_name):
    #urlretrieve(url,file_name), verify=False
    with open(file_name, 'wb') as f:
        resp = get(url)
        f.write(resp.content)

class LandSat():
    def __init__(self,key=KEY,secret_key=SECRET_KEY,download_scene_list = True):
        self.scene_list = scene_list = 'https://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz'
        self.region = region = os.environ.get('AWS_REGION', 'us-east-1')
        self.landsat_bucket  = landsat_bucket = 'landsat-pds'
        conn = S3Connection(key,secret_key)
        self.session = session = boto3_session(region_name=region)
       #self.s3 = s3 = session.client('s3')
        self.bucket = conn.get_bucket(landsat_bucket )
        scene_list_path = 'scene_list.gz'
        if download_scene_list:
            download(scene_list,scene_list_path)
        with gzip.open(scene_list_path, 'rb') as f:
            str_file_content = f.read()
        rows = str_file_content.decode().split('\n')
        title = rows[0].split(',')
        self.file_content = {r:[] for r in title}
        for row in rows[1::]:
            row_s = row.split(',')
            if len(row_s)!=len(title):
                continue
            for i,r in enumerate(row_s):
                self.file_content[title[i]].append(r)
        
        new_dates = []
        for date in self.file_content['acquisitionDate']:
            splited = date.split('.')
            nofrag = splited[0]
            if len(splited)>1:
                frag = splited[1]
            else:
                frag = '0'

            new_dates.append(datetime.datetime.strptime(nofrag,'%Y-%m-%d %H:%M:%S').replace(microsecond=int(frag)))
        self.file_content['acquisitionDate'] = new_dates

        for key in self.file_content:
            if key in ['row', 'min_lat', 'min_lon', 'max_lat', 'max_lon']:
                self.file_content[key] = numpy.array(self.file_content[key],float)
            else:
                self.file_content[key] = numpy.array(self.file_content[key])

    def get_polygon(self,polygon,min_date,max_date = datetime.datetime.now(),max_results = 1000):
        polygon = [numpy.array(p) for p in polygon]
        min_p = numpy.inf
        max_p = -numpy.inf
        for p in polygon:
            min_p = numpy.minimum(p,min_p)
            max_p = numpy.maximum(p,max_p)

        min_lat, max_lat = min_p[0],max_p[0]
        min_lon, max_lon = min_p[1],max_p[1]

        out_of_polygon_1 = (min_lat>self.file_content['max_lat'])|(max_lat<self.file_content['min_lat'])
        out_of_polygon_2 = (min_lon>self.file_content['max_lon'])|(max_lon<self.file_content['min_lon'])
        out_of_polygon_3 = (self.file_content['acquisitionDate']<min_date)|(self.file_content['acquisitionDate']>max_date)
        out_of_polygon = (out_of_polygon_1|out_of_polygon_2|out_of_polygon_3)
        out_content = {}
        for key in self.file_content:
            out_content[key] = self.file_content[key][out_of_polygon == False]

        ii = numpy.argsort(out_content['acquisitionDate'])[::-1]
        ii = ii[0:max_results]
        for key in self.file_content:
            out_content[key] = out_content[key][ii]
        return out_content

    @staticmethod
    def download_all(products,dir):
        for iii,(pid,url) in enumerate(zip(products['productId'],products['download_url'])):
            print('downloading %d out of %d '%(iii+1,len(products['productId'])))
            curr_dir = dir + '/' + '_'.join([str(products[k][iii]) for k in ['min_lat','max_lat','min_lon','max_lon']])
            if not os.path.exists(curr_dir):
                os.makedirs(curr_dir)

            curr_dir = curr_dir + '/' +products['acquisitionDate'][iii].strftime('%y_%m_%d')
            if not os.path.exists(curr_dir):
                os.makedirs(curr_dir)
            else:
                continue

            response = get(url)
            par_url = url.split('/')[0:-1]
            tree = html.fromstring(response.content)
            file_name = tree.find('body').find('a').attrib['href']
            download('/'.join(par_url+[file_name]),curr_dir+'/rgb.jpg')

            for li in tree.find('body').find('ul').findall('li'):
                file_name = li.find('a').attrib['href']
                if (file_name.split('.')[-1]=='TIF') or (file_name.split('.')[-1]=='txt') or (file_name.split('.')[-1]=='IMD'):
                    download('/'.join(par_url+[file_name]),curr_dir+'/'+file_name.split('_')[-1].lower())

class Sentinal():
    def __init__(self,level = 'l1c'):
        self.mgs_kml_http = mgs_kml_http ="https://sentinels.copernicus.eu/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml/ec05e22c-a2bc-4a13-9e84-02d5257b09a8"

        self.level = level
        mgs_kml  = 'mgs.kml'
        download(mgs_kml_http,mgs_kml)

        driver = ogr.GetDriverByName('KML')
        self.driver = driver
        self.datasource = driver.Open(mgs_kml)
        self.layer = self.datasource.GetLayer()

    def get_relevent_tiles_str(self,polygon):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for x,y in polygon:
            ring.AddPoint(x,y)
        (x,y) = polygon[0]
        ring.AddPoint(x,y)
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        layer = self.layer
        layer.SetAttributeFilter(None)
        layer.SetSpatialFilter(None)
        layer.SetSpatialFilter(poly)
        
        tiles = []
        for feature in layer:
            utm_code  = feature['name'][0:2]
            lat_band  = feature['name'][2]
            square    = feature['name'][3::]
            tiles.append({'utm_code':utm_code,'lat_band':lat_band,'square':square})
        
        layer.SetSpatialFilter(None)

        return tiles

    def get_sat_data(self,tile,date):
        s3 = boto3.resource('s3')
        level = self.level
        s2  = 'sentinel-s2' + '-'+level
        utm_code  = tile['utm_code']
        lat_band  = tile['lat_band']
        square    = tile['square']
        year       = str(date.year)
        month      = str(date.month)
        day        = str(date.day)
        pref = 'tiles/%s/%s/%s/%s/%s/%s/'%(utm_code,lat_band,square,year,month,str(day))

        results = list(s3.Bucket(s2).objects.filter(Prefix =pref).all())
        return results

    def get_polygon(self,polygon,min_date,max_date = datetime.datetime.now(),max_results = 10):
        tiles = self.get_relevent_tiles_str(polygon)
        results  = []
        nresluts = 0 
        ct = copy.deepcopy(min_date)
        while (max_date-ct).days>=0:
            ct_results = []
            for tile in tiles:
                 ct_results+=self.get_sat_data(tile,ct)
            results +=ct_results
            if len(ct_results)>0:
                nresluts+=1
                if nresluts==max_results:
                    break
            ct += datetime.timedelta(days=1)
        return results
        

    @staticmethod
    def filter_image_results(results):
        return [k for k in results if re.match(".*/\d+/B\d+.jp2", k.key)]

    @staticmethod
    def download_all(products,dir):
        for prod in products:
            pkey = prod.key.split('/')
            curr_dir = dir +'/' +  '_'.join(pkey[0:4])
            if not os.path.exists(curr_dir):
                os.makedirs(curr_dir)            

            curr_dir += '/' +  '_'.join(pkey[4:7])
            if not os.path.exists(curr_dir):
                os.makedirs(curr_dir)            

            file_name = curr_dir +'/' + pkey[8]
            if not os.path.exists(file_name):
                prod.Bucket().download_file(prod.key, file_name)

class TransparentApi():

    def __init__(self,download_scene_list=True):
        self.api = {'landsat-8':LandSat(download_scene_list = download_scene_list),'sentinel':Sentinal()}


    def download_raw_images(self,raw_directory,polygon,dataset,start_datetime=datetime.datetime.now()-datetime.timedelta(days=30),end_datetime=datetime.datetime.now(),max_results=1):

        out_content =  self.get_results(polygon,dataset,start_datetime,end_datetime,max_results)
        if dataset=='landsat-8':
            LandSat.download_all(out_content,raw_directory)

        if dataset=='sentinel':
            Sentinal.download_all(out_content,raw_directory)
    
    def get_results(self,polygon,dataset,start_datetime=datetime.datetime.now()-datetime.timedelta(days=30),end_datetime=datetime.datetime.now(),max_results=1):
        if dataset=='landsat-8':
            api = self.api[dataset]
            out_content = api.get_polygon(polygon,start_datetime,end_datetime,max_results)

        if dataset=='sentinel':
            api = self.api[dataset]
            out_content = api.get_polygon(polygon,start_datetime,end_datetime,max_results)
            out_content = Sentinal.filter_image_results(out_content)
        return out_content
        

    def process_farm(self,raw_directory,farm,bucket,dataset,processed_directory = '/tmp/processed_directory'):
        res = self.get_results(farm['coordinates'],dataset)
        if len(res)==0:
            return 
        if dataset=='landsat-8':
            curr_dir = raw_directory + '/' + '_'.join([str(res[k][0]) for k in ['min_lat','max_lat','min_lon','max_lon']])
        elif dataset=='sentinel':
            curr_dir = raw_directory + '/' + '_'.join(res[0].key.split('/')[0:4])

        if not os.path.exists(curr_dir):
            return 

        for date_dir in os.listdir(curr_dir):
            generated_name = 'satellite/' + str(farm['id']) + '/' +dataset + '/'+ date_dir

            if os.path.exists(processed_directory):
                shutil.rmtree(processed_directory)
            os.makedirs(processed_directory)

            curr_date_dir = curr_dir + '/' + date_dir
            for file in os.listdir(curr_date_dir):
                if file.split('.')[-1].lower()=='tif':
                    general_utills.crop_image(curr_date_dir+'/'+file,processed_directory+'/'+file,farm['coordinates'])
                elif file.split('.')[-1].lower()=='jp2':
                    general_utills.crop_image(curr_date_dir+'/'+file,processed_directory+'/'+file,farm['coordinates'])
                else:
                    shutil.copy(curr_date_dir+'/'+file,processed_directory+'/'+file)


            if dataset=='landsat-8':
                try:
                    rgb_im = Image.open(processed_directory+'/'+'rgb.jpg')
                    rgb_im = rgb_im.resize((10000,10000))
                    min_c = numpy.array(farm['coordinates']).min(0)
                    max_c = numpy.array(farm['coordinates']).max(0)
                    min_w = int(rgb_im.size[0]*(min_c[1]-res['min_lon'])/(res['max_lon']-res['min_lon']))
                    max_h = int(rgb_im.size[1]*(max_c[0]-res['min_lat'])/(res['max_lat']-res['min_lat']))
                    max_w = int(rgb_im.size[0]*(max_c[1]-res['min_lon'])/(res['max_lon']-res['min_lon']))
                    min_h = int(rgb_im.size[1]*(min_c[0]-res['min_lat'])/(res['max_lat']-res['min_lat']))
                
                    rgb_im.crop((min_w,min_h,max_w+1,max_h+1)).save(processed_directory+'/'+'rgb.jpg')
                except:
                    pass
                try:
                    b4 = Image.open(processed_directory + '/b4.tif')
                    b5 = Image.open(processed_directory + '/b5.tif')

                    nvdi = general_utills.get_nvdi_image(b4,b5)
                    nvdi = Image.fromarray(nvdi,'L')
                    nvdi.save(processed_directory +'/nvdi.jpg')
                except:
                    pass

            if dataset=='sentinel':
                try:
                    b4 = Image.open(processed_directory + '/b04.jp2')
                    b5 = Image.open(processed_directory + '/b05.jp2')

                    nvdi = general_utills.get_nvdi_image(b4,b5)
                    nvdi = Image.fromarray(nvdi,'L')
                    nvdi.save(processed_directory +'/nvdi.jpg')
                except:
                    pass

            for file in os.listdir(processed_directory):
                k = bucket.new_key(generated_name + '/' + file)
                k.set_contents_from_filename(processed_directory+'/'+file)

if __name__ =='__main__':
    
    polygon = [[48.85769298962216, 2.2920077833455252], [48.85678945005647, 2.2933274301808524], [48.85928120323469, 2.2975224050801444], [48.860368218169455, 2.295805791310613]]

    t_api = TransparentApi()
    for dataset in ALL_DATA_SETS.keys():
        t_api.download_images('aws_downloads/'+dataset,polygon,dataset)