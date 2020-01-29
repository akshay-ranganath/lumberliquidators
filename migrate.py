'''
Scene 7 to Cloudinary Migration for Lumber Liquidators
'''

import cloudinary
import cloudinary.uploader
import csv
import logging
from concurrent.futures import ThreadPoolExecutor as PoolExecutor

logger = logging.getLogger('root')
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

videos = []
images = []
others = []

cloudinary.config()

def migrate_image(image_name):
    migrate_object(image_name, "image")

def migrate_video(video_name):
    migrate_object(video_name, "video")

def migrate_object(object_name, object_type="image"):
    object_url = ""    
    if object_type == "image" or object_type == "video":
        object_url = f"https://s7d2.scene7.com/is/{object_type}/LumberLiquidators/{object_name}?scl=1&qlt=100"
        if object_type == "image":
            object_url += "&format=alpha-png"    
    try:
        resp = cloudinary.uploader.upload(
            object_url,
            use_filename=True,
            resource_type = object_type,
            type = "upload",
            unique_filename=False,
            overwrite=False
        )
        print(f"{object_name},{object_type},Success")
    except Exception as e:
        print(f"{object_name},{object_type},Fail,{e}")
    

if __name__=="__main__":
    
    with open("test.csv") as csvfile:
        s3objects = csv.reader(csvfile)
        for (object_name, object_type) in s3objects:
            if object_type=="IMAGE" or object_type=="Animated GIF":
                images.append(object_name)
            elif object_type=="Video":
                videos.append(object_name)            

        logging.info(f"Migrating {len(images)} images.")
        logging.info(f"Migrating {len(videos)} videos.")        
        
        with PoolExecutor(max_workers=20) as executor:
            # migrate images  
            for _ in executor.map(migrate_image, images):
                pass          
            # migrate videos      
            for _ in executor.map(migrate_video, videos):
                pass