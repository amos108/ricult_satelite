import general_utills,numpy
from PIL import  Image
import rasterio
import os
import config_file
if config_file.MODE == 'DOWNLOADER':
    import skimage
    import cv2


def save_cload_image_landsat(dirctory_path):
    image_path = dirctory_path + '/RT.tif'
    if not os.path.exists(image_path):
        return
    with rasterio.open(image_path) as tiff_image:
        data = tiff_image.read()

    hsv_array = (numpy.array(Image.fromarray(data.transpose([1,2,0])).convert('HSV'))[:,:,0]/255).astype(float)

    mt = 0.3495
    st = 0.0543
    nx = (hsv_array-mt)/st
    iscload_array = numpy.zeros(nx.shape)
    for k1 in range(nx.shape[0]):
        for k2 in range(nx.shape[1]):
            if hsv_array[k1][k2] == 0:
                iscload_array[k1][k2] = -1
            else:
                iscload_array[k1][k2] = general_utills.norm_cdf(nx[k1][k2])

    general_utills.save_tiff_of_image(iscload_array, dirctory_path + '/cloud_image.tiff', image_path)


def save_cload_image_sentinel(dirctory_path):
    '''
    image_path = dirctory_path + '/B01.jp2'
    if not os.path.exists(image_path):
        return
    with rasterio.open(image_path) as tiff_image:
        data = tiff_image.read()

    data = data[0].astype(float)
    mt = 2050
    st = 50/3
    nx = (data-mt)/st
    iscload_array = numpy.zeros(nx.shape)
    for k1 in range(nx.shape[0]):
        for k2 in range(nx.shape[1]):
            if data[k1][k2] == 0:
                iscload_array[k1][k2] = -1
            else:
                iscload_array[k1][k2] = general_utills.norm_cdf(nx[k1][k2])

    general_utills.save_tiff_of_image(iscload_array, dirctory_path + '/cloud_image.tiff', image_path)

    '''

    image_path = dirctory_path + '/B02.jp2'
    if not os.path.exists(image_path):
        return
    with rasterio.open(image_path) as tiff_image:
        data = tiff_image.read()

    data = data[0].astype(float)
    mt = 2050
    st = 50/3
    nx = (data-mt)/st
    iscload_array_b02 = numpy.zeros(nx.shape)
    for k1 in range(nx.shape[0]):
        for k2 in range(nx.shape[1]):
            if data[k1][k2] == 0:
                iscload_array_b02[k1][k2] = -1
            else:
                iscload_array_b02[k1][k2] = general_utills.norm_cdf(nx[k1][k2])


    image_path = dirctory_path + '/B09.jp2'
    if not os.path.exists(image_path):
        return
    with rasterio.open(image_path) as tiff_image:
        data = tiff_image.read()

    data = data[0].astype(float)
    mt = 600
    st = 100/3
    nx = (data-mt)/st
    iscload_array_b09 = numpy.zeros(nx.shape)
    for k1 in range(nx.shape[0]):
        for k2 in range(nx.shape[1]):
            if data[k1][k2] == 0:
                iscload_array_b09[k1][k2] = -1
            else:
                iscload_array_b09[k1][k2] = general_utills.norm_cdf(nx[k1][k2])

    iscload_array_b09=skimage.transform.resize(iscload_array_b09.astype(float), iscload_array_b02.shape)
    iscload_array = numpy.maximum(iscload_array_b02,iscload_array_b09)
    general_utills.save_tiff_of_image(iscload_array, dirctory_path + '/cloud_image.tiff', image_path)

def save_cload_image(raw_directory,dataset):
    if dataset == 'landsat-8' or dataset == 'landsat-7':
        save_cload_image_landsat(raw_directory)

    elif dataset == 'sentinel':
        save_cload_image_sentinel(raw_directory)



def get_cload_percent(cloud_image):

    with rasterio.open(cloud_image) as tiff_image:
        iscload_array = tiff_image.read()

    cload_per = ((iscload_array*(iscload_array!=-1)).sum())/((iscload_array!=-1).sum())
    if numpy.isnan(cload_per):
        cload_per = -1
    return cload_per


def save_rgb_image(tmp_dir, dataset):

    if dataset == 'sentinel':
        save_rgb_image_sentinel(tmp_dir)

def save_rgb_image_sentinel(temp_dir):

    with rasterio.open(temp_dir + '/B02.jp2') as fop:
        b2 = fop.read()
    with rasterio.open(temp_dir + '/B03.jp2') as fop:
        b3 = fop.read()
        meta = fop.meta
    with rasterio.open(temp_dir + '/B04.jp2') as fop:
        b4 = fop.read()

    norm = max([b3.max(), b2.max(), b4.max()])

    b4 = 255 * numpy.array(b4[0].astype(float)) / norm
    b3 = 255 * skimage.transform.resize(b3[0].astype(float), b4.shape) / norm
    b2 = 255 * skimage.transform.resize(b2[0].astype(float), b4.shape) / norm

    img = numpy.array([b2,b3,b4]).transpose([1,2,0]).astype('uint8')
    lab= cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8,8))
    cl = clahe.apply(l)
    limg = cv2.merge((cl,a,b))
    final = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR)

    b2 = final[:,:,0]
    b3 = final[:,:,1]
    b4 = final[:,:,2]

    # Update meta to reflect the number of layers
    meta.update(count=3)
    meta.update(dtype='uint8')
    with rasterio.open(temp_dir + '/FRB.tiff', 'w', **meta) as dst:
        for id, layer in enumerate([b4, b3, b2], start=1):
            layer = numpy.minimum(layer, 255)
            dst.write_band(id, layer.astype(numpy.uint8))
