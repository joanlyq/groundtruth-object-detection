import os
import cv2
import boto3
import json
import io
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import skimage.io
import PIL.Image
import numpy as np

#conda install -c conda-forge scikit-image==0.18.2

PIL.Image.MAX_IMAGE_PIXELS = 933120000
def read_image_from_s3(bucket, key, region_name='ap-southeast-2'):
    """Load image file from s3.

    Parameters
    ----------
    bucket: string
        Bucket name
    key : string
        Path in s3

    Returns
    -------
    np array
        Image array
    """
    s3 = boto3.resource('s3', region_name='ap-southeast-2')
    bucket = s3.Bucket(bucket)
    object = bucket.Object(key)
    response = object.get()
    file_stream = response['Body']
    im = PIL.Image.open(file_stream)
    return np.array(im)

def crop_img(RAW_IMG_DIR, CROP_IMG_DIR, CROP_HEIGHT, CROP_WIDTH, OVERLAP):
    STRIDE_HEIGHT = CROP_HEIGHT * (1-OVERLAP)
    STRIDE_WIDTH = CROP_WIDTH * (1-OVERLAP)
    #read raw image
    s3_rec = boto3.resource("s3")
    s3_bucket = RAW_IMG_DIR.split("/")[0]
    bucket = s3_rec.Bucket(s3_bucket)
    print(s3_bucket)
    print(bucket)
    folder_dir = RAW_IMG_DIR.replace(s3_bucket, "")[1:]
    client = boto3.client('s3')
    result = client.list_objects_v2(Bucket=s3_bucket, Prefix=folder_dir, Delimiter='/')
    for file in result.get("Contents"):
        fn = file.get("Key")
        print ('fn : ', fn)
        if fn.endswith("5mm.tiff"):
            # Open image
            #bucket.download_file(fn, 'temp.tiff')
            img = bucket.Object(f"{fn}")

            #print(img)
            #response = img.get()
            #file_stream = response['Body']
            file_stream = io.BytesIO()
            img.download_fileobj(file_stream)
            #print(file_stream)
            #raw_img = PIL.Image.open(file_stream)
            #raw_img = cv2.imread(file_stream, -1)
            #raw_img=read_image_from_s3(s3_bucket,fn, region_name='ap-southeast-2')
            raw_img = skimage.io.imread(file_stream, plugin='pil')
            #raw_img = mpimg.imread(file_stream, format='jpeg')
            print(raw_img)
            height, width = raw_img.shape[:2]
            #width, height = raw_img.size
            print(height,width)

            raw_img_name = fn.replace(".tif", "")
            raw_img_name = raw_img_name.split("/")[-1]
            print(raw_img_name)
            #client.put_object(Bucket=s3_bucket, Key=CROP_IMG_DIR + "/")

            # Iterate no sliding
            count = 0
            for i in range(0, height, STRIDE_HEIGHT):
                for j in range(0, width, STRIDE_WIDTH):
                    crop_img = raw_img[i:(i + CROP_HEIGHT), j:(j + CROP_WIDTH)]

                    # Pad black rows
                    if (i + CROP_HEIGHT) > height:
                        crop_img = cv2.copyMakeBorder(crop_img, 0, (i + CROP_HEIGHT) - height, 0, 0, cv2.BORDER_CONSTANT, 0)

                    # Pad black columns
                    if (j + CROP_WIDTH) > width:
                        crop_img = cv2.copyMakeBorder(crop_img, 0, 0, 0, (j + CROP_WIDTH) - width, cv2.BORDER_CONSTANT, 0)

                    #set saving dir and save images to aws
                    dst_dir = CROP_IMG_DIR.replace(s3_bucket, "")[1:]
                    print(dst_dir)
                    img_string = cv2.imencode('.png', crop_img)[1].tobytes()
                    img_name = dst_dir +"{}/{}_{:05d}.png".format(raw_img_name,raw_img_name, count)
                    print(img_name)
                    client.put_object(
                        Bucket=s3_bucket,
                        Key= img_name,
                        Body=img_string,
                        ContentType='image/png',
                    )
                    print(f"uploaded image {img_name} to {dst_dir} \n")
                    count = count + 1
            print("done " + fn)

def main():
    """
    Performs the following tasks:
    1. Reads input from 'input.json'
    2. Collects image names from S3 and creates the crop for GT
    3. Uploads the manifest file to S3
    """

    with open("input.json") as fjson:
        input_dict = json.load(fjson)

    RAW_IMG_DIR = input_dict["raw_img_dir"]
    CROP_IMG_DIR = input_dict["crop_img_dir"]
    CROP_HEIGHT = int(input_dict["crop_height"])
    CROP_width = int(input_dict["crop_width"])
    OVERLAP = float(input_dict["overlap"])

    crop_img(RAW_IMG_DIR, CROP_IMG_DIR, CROP_HEIGHT, CROP_WIDTH, OVERLAP)
    print("done")
    
    
if __name__ == '__main__':
    main()