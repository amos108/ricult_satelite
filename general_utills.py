import numpy
from PIL import Image
import json
import os
import stat
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from math import *
import sys 
import rasterio
import rasterio
from rasterio.plot import show
from rasterio.plot import show_hist
from rasterio.mask import mask
from shapely.geometry import box
from fiona.crs import from_epsg
import json
from rasterio.warp import transform
from rasterio.merge import merge
import shutil


def norm_cdf(x):
    #'Cumulative distribution function for the standard normal distribution'
    return (1.0 + erf(x / sqrt(2.0))) / 2.0

def getFeatures(gdf):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    return [json.loads(gdf.to_json())['features'][0]['geometry']]

def rmtree(top):
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            filename = os.path.join(root, name)
            os.chmod(filename, stat.S_IWRITE)
            os.remove(filename)
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(top)   

def crop_image(input_image,out_image,polygon):
    polygon = [numpy.array(p) for p in polygon]
    if (polygon[0]!=polygon[-1]).all():
        polygon.append(polygon[0])
    min_p = numpy.inf
    max_p = -numpy.inf
    for p in polygon:
        min_p = numpy.minimum(p,min_p)
        max_p = numpy.maximum(p,max_p)

    minx, maxx = min_p[1],max_p[1]
    miny, maxy = min_p[0],max_p[0]
    
    box_x = [minx,minx,maxx,maxx]
    box_y = [miny,maxy,maxy,miny]

    try:
        with rasterio.open(input_image) as data:
            coords = rasterio.warp.transform(src_crs='wgs84', dst_crs=data.crs, xs=box_x, ys=box_y)
            out_meta = data.meta.copy()
            coords = Polygon([[x, y] for x, y in zip(coords[0], coords[1])])
            out_img, out_transform = mask(data,all_touched=True, shapes=[coords], crop=True)
        out_meta.update({"driver": "GTiff","transform": out_transform,"crs": data.crs,'width': out_img.shape[2], 'height': out_img.shape[1]})
        with rasterio.open(out_image, "w", **out_meta) as dest:
            dest.write(out_img)
    except:
        with rasterio.open(input_image) as data:
            coords = rasterio.warp.transform(src_crs='wgs84', dst_crs=data.crs, xs=box_x, ys=box_y)
            out_meta = data.meta.copy()

        out_img = numpy.zeros([data.count] + [1]*(len(data.shape)),data.dtypes[0])
        out_meta.update({"driver": "GTiff","crs": data.crs,'width': 1, 'height': 1})
        with rasterio.open(out_image, "w", **out_meta) as dest:
            dest.write(out_img)



def get_ndvi_image(b4,b8,b3):
    (w1,h1) = b4.size
    (w2,h2) = b8.size
    (w3,h3) = b3.size

    w = max([w1,w2,w3])
    h = max([h1,h2,h3])
    b3 = numpy.array(b3.resize((w,h))).astype(float)
    b4 = numpy.array(b4.resize((w,h))).astype(float)
    b8 = numpy.array(b8.resize((w,h))).astype(float)

    if len(b4.shape)>2:
        b4 = b4[:,:,0]
    if len(b8.shape)>2:
        b8 = b8[:,:,0]
    if len(b3.shape)>2:
        b3 = b3[:,:,0]

    nvdi_1 = (b8-b4)/(b8+b4)
    nvdi_2 = (b4-b3)/(b4+b3)
    nvdi = nvdi_1
    nvdi[numpy.isnan(nvdi_1)] = nvdi_2[numpy.isnan(nvdi_1)]
    nvdi[numpy.isnan(nvdi)] = -1
    return nvdi


def read_polygons(filename):
    with open(filename) as fid:
        polygons = eval(fid.read())
    return polygons

def save_tiff_of_image(array_image,output_path,tiff_path):
    tiff_ref = rasterio.open(tiff_path)
    profile = tiff_ref.profile
    profile.update(
        dtype=rasterio.float64,
        count=1)

    with rasterio.open(output_path, 'w+', **profile) as dst:
        dst.write(array_image.astype(rasterio.float64), 1)

def create_tiff_image_json(image_path,json_path,polygon,ignore_area = 1e-4):

    tiffimage = rasterio.open(image_path)

    c, a, b, f, d, e = tiffimage.get_transform()

    def pixel2coord(col, row):
        """Returns global coordinates to pixel center using base-0 raster index"""
        xp = a * col + b * row +  c
        yp = d * col + e * row +  f
        return(xp, yp)

    arr = tiffimage.read()
    xx_1,yy_1 = numpy.meshgrid(numpy.arange(tiffimage.width),numpy.arange(tiffimage.height))
    xx_2,yy_2 = numpy.meshgrid(1+numpy.arange(tiffimage.width),numpy.arange(tiffimage.height))
    xx_3,yy_3 = numpy.meshgrid(1+numpy.arange(tiffimage.width),1+numpy.arange(tiffimage.height))
    xx_4,yy_4 = numpy.meshgrid(numpy.arange(tiffimage.width),1+numpy.arange(tiffimage.height))

    xx_1 = xx_1.reshape(-1)
    xx_2 = xx_2.reshape(-1)
    xx_3 = xx_3.reshape(-1)
    xx_4 = xx_4.reshape(-1)
    yy_1 = yy_1.reshape(-1)
    yy_2 = yy_2.reshape(-1)
    yy_3 = yy_3.reshape(-1)
    yy_4 = yy_4.reshape(-1)

    polygon = Polygon(polygon)

    if not polygon.is_valid:
        with open(json_path, 'w') as outfile:
            json.dump([],outfile)
        return 

    
    
    if polygon.area>=ignore_area:
        with open(json_path, 'w') as outfile:
            json.dump([],outfile)
        return 
    pix = []
    if len(arr.shape) == 3:
        arr = arr[:,yy_1,xx_1]
        arr = arr.T
    else:
        arr = arr[yy_1,xx_1]


    def get_lat_long(yy,xx):
        xx,yy = tiffimage.xy(xx, yy)
        lt, ln = rasterio.warp.transform(dst_crs='wgs84', src_crs = tiffimage.crs, xs=xx,ys = yy)
        return lt, ln

    ln1, lt1 = get_lat_long(xx_1,yy_1)
    ln2, lt2 = get_lat_long(xx_2,yy_2)
    ln3, lt3 = get_lat_long(xx_3,yy_3)
    ln4, lt4 = get_lat_long(xx_4,yy_4)
    for (x1,x2,x3,x4,y1,y2,y3,y4,val) in zip(lt1,lt2,lt3,lt4,ln1,ln2,ln3,ln4,arr):

        coords = []
        coords.append([x1,y1])

        coords.append([x2,y2])

        coords.append([x3,y3])

        coords.append([x4,y4])

        inter_polygon = polygon.intersection(Polygon(coords))
        if inter_polygon.area>0:
            try:
                val = float(val)
            except:
                val = [float(v) for v in val]

            if inter_polygon.type =='Polygon':
                inter_polygon = [inter_polygon]
            for cpol in list(inter_polygon):
                coords = [[x,y] for x,y in zip(cpol.exterior.coords.xy[0],cpol.exterior.coords.xy[1])]
                pix.append({'coords':coords,'value':val})

    with open(json_path, 'w') as outfile:
        json.dump(pix,outfile)

    tiffimage.close()


def upload_dir_to_bucket(dir_path,bucket,pre_fix = ''):
    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                bucket.put_object(Key=pre_fix + '/' + full_path[len(dir_path) + 1:], Body=data)


def download_dir_from_bucket(bucket,prefix,processed_directory):
    for ckey in bucket.objects.filter(Prefix=prefix).all():
        rel_key = ckey.key[len(prefix)+1:]
        full_path = os.path.join(processed_directory, rel_key)
        if ckey.key[-1] == '/':
            if not os.path.exists(full_path):
                os.makedirs(full_path)
        else:
            d_name = os.path.dirname(full_path)
            if not os.path.exists(d_name):
                os.makedirs(d_name)
            bucket.download_file(ckey.key,full_path)

def bucket_has_key(bucket,prefix):
    return (len(list(bucket.objects.filter(Prefix=prefix).all()))>0)

def merge_image(out_path,images_to_merge_path):
    src_files_to_mosaic = []
    for fp in images_to_merge_path:
         src = rasterio.open(fp)
         src_files_to_mosaic.append(src)

    mosaic, out_trans = rasterio.merge.merge(src_files_to_mosaic)
    out_meta = src.meta.copy()

    # Update the metadata
    out_meta.update({"driver": "GTiff", "height": mosaic.shape[1],"width": mosaic.shape[2],"transform": out_trans,"crs": src_files_to_mosaic[0].crs})

    with rasterio.open(out_path, "w", **out_meta) as dest:
        dest.write(mosaic)
    for f in src_files_to_mosaic:
        f.close()


if __name__ == '__main__':
    polygon=[[9.9,100.9],[9.9,100.95],[9.95,100.95],[9.95,100.9]]
    input_image = 'B03.jp2'
    out_image = 'tmp.jp2'
    crop_image(input_image,out_image,polygon)