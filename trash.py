from sentinelhub import WebFeatureService, BBox, CRS, DataSource
from sentinelhub import AwsTileRequest,WmsRequest,AwsProductRequest
import  config_file,time

INSTANCE_ID = '97b3c965-e57f-4a06-9ddc-e1ec478d930a'  # In case you put instance ID into cofniguration file you can leave this unchanged

search_bbox = BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84)
search_time_interval = ('2017-12-01T00:00:00', '2017-12-15T23:59:59')


wfs_iterator = WebFeatureService(search_bbox, search_time_interval,
                                 data_source=DataSource.LANDSAT8,
                                 instance_id='f438079a-b072-4285-b154-037b0627d223')
request = AwsProductRequest(product_id=x[0]['properties']['id'], data_folder='')
tt = time.time()
metafiles = []
for (tile_name,t_time,aws_index) in wfs_iterator.get_tiles():
    tile_request = AwsTileRequest(tile=tile_name, time=t_time, aws_index=aws_index, data_folder=data_folder)#metafiles=metafiles
    tile_request.save_data()


print(tt - time.time())
request = AwsProductRequest(product_id=product_id, data_folder=data_folder)


tile_id = 'S2A_OPER_MSI_L1C_TL_MTI__20151219T100121_A002563_T38TML_N02.01'
tile_name, time, aws_index = sentinelhub.AwsTile.tile_id_to_tile(tile_id)






from sentinelhub import WebFeatureService, BBox, CRS, DataSource
from sentinelhub import AwsTile
tile_id = 'S2A_OPER_MSI_L1C_TL_MTI__20151219T100121_A002563_T38TML_N02.01'
tile_name, time, aws_index = AwsTile.tile_id_to_tile(tile_id)
tile_name, time, aws_index
from sentinelhub import AwsTileRequest
import sentinelhub
sentinelhub.data
bands = ['B8A', 'B10']
metafiles = ['tileInfo', 'preview', 'qi/MSK_CLOUDS_B00']
data_folder = './AwsData'

request = AwsTileRequest(tile=tile_name, time=time, aws_index=aws_index, metafiles=metafiles, data_folder=data_folder,
                         data_source=DataSource.SENTINEL2_L1C)

request.save_data()  # This is where the download is triggered
data_list = request.get_data()  # This will not redownload anything because data is already stored on disk

b8a, b10, tile_info, preview, cloud_mask = data_list


from sentinelhub import AwsTile





tile_id = x[0]['properties']['id']
tile_name, time, aws_index = AwsTile.tile_id_to_tile(tile_id)
tile_name, time, aws_index






import rasterio,numpy,skimage
temp_dir = '/home/shilo/Downloads'
with rasterio.open(temp_dir + '/B02.jp2') as fop:
    b2 = fop.read()
with rasterio.open(temp_dir + '/B03.jp2') as fop:
    b3 = fop.read()
    meta = fop.meta
with rasterio.open(temp_dir + '/B04.jp2') as fop:
    b4 = fop.read()


norm = max([b3.max(),b2.max(),b4.max()])

b4 = 255*numpy.array(b4[0].astype(float)) / norm
b3 = 255*skimage.transform.resize(b3[0].astype(float), b4.shape) / norm
b2 = 255*skimage.transform.resize(b2[0].astype(float), b4.shape) / norm
# Update meta to reflect the number of layers
meta.update(count=3)
meta.update(dtype='uint8')
with rasterio.open(temp_dir + '/FRB.tiff', 'w', **meta) as dst:
    for id, layer in enumerate([b4, b3, b2], start=1):
        layer = numpy.minimum(layer, 255)
        dst.write_band(id, layer.astype(numpy.uint8))

import cv2

#-----Reading the image-----------------------------------------------------
img = cv2.imread(temp_dir + '/FRB.tiff', 1)

#-----Converting image to LAB Color model-----------------------------------
lab= cv2.cvtColor(img, cv2.COLOR_BGR2LAB)

#-----Splitting the LAB image to different channels-------------------------
l, a, b = cv2.split(lab)


#-----Applying CLAHE to L-channel-------------------------------------------
clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8,8))
cl = clahe.apply(l)

# tmp_max=cl.max()
# cl=cl/tmp_max
# cl=numpy.sqrt(cl)
# cl=cl*tmp_max
# cl=cl.astype('uint8')
#-----Merge the CLAHE enhanced L-channel with the a and b channel-----------
limg = cv2.merge((cl,a,b))

#-----Converting image from LAB Color model to RGB model--------------------
final = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR)

cv2.imwrite(temp_dir+'/final.jpeg',final)
#_____END_____#

'''
empty image delete
'''

'''
cloud check
'''
import boto3,config_file
import json
import general_utills

s3 = boto3.resource('s3', aws_access_key_id=config_file.KEY, aws_secret_access_key=config_file.SECRET_KEY)

bucket = s3.Bucket(config_file.DATA_BUCKET_NAME)
# prefix = 'satellite/raw_satellite_data/sentinel/'
# objs = list(bucket.objects.filter(Prefix= prefix).all())

dirctory_path='satellite/raw_satellite_data/sentinel/11.2_102.0/2018-09-11'
dirctory_path='satellite/raw_satellite_data/sentinel/12.0_102.2/2018-10-01'

bucket.download_file(dirctory_path + '/B02.jp2','B02.jp2')
bucket.download_file(dirctory_path + '/B09.jp2','B09.jp2')

image_path ='B02.jp2'


with rasterio.open(image_path) as tiff_image:
    data = tiff_image.read()

data = data[0].astype(float)
mt = 2050
st = 50 / 3
nx = (data - mt) / st
iscload_array_b02 = numpy.zeros(nx.shape)
for k1 in range(nx.shape[0]):
    for k2 in range(nx.shape[1]):
        if data[k1][k2] == 0:
            iscload_array_b02[k1][k2] = -1
        else:
            iscload_array_b02[k1][k2] = general_utills.norm_cdf(nx[k1][k2])

image_path = 'B09.jp2'

with rasterio.open(image_path) as tiff_image:
    data = tiff_image.read()

data = data[0].astype(float)
mt = 600
st = 100 / 3
nx = (data - mt) / st
iscload_array_b09 = numpy.zeros(nx.shape)
for k1 in range(nx.shape[0]):
    for k2 in range(nx.shape[1]):
        if data[k1][k2] == 0:
            iscload_array_b09[k1][k2] = -1
        else:
            iscload_array_b09[k1][k2] = general_utills.norm_cdf(nx[k1][k2])

iscload_array_b09=skimage.transform.resize(iscload_array_b09.astype(float), iscload_array_b02.shape)
iscload_array = numpy.maximum(iscload_array_b02, iscload_array_b09)
general_utills.save_tiff_of_image(iscload_array, 'cloud_image.tiff', image_path)


import docker