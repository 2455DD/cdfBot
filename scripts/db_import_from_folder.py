from PIL import Image,UnidentifiedImageError
import imagehash
from plugins.media_parser.media_sql_helper import MediaParserSqlHelper
from pathlib import Path
import videohash


db_path = Path(input("请输入目标数据库路径:"))

if not db_path.parent.exists():
    db_path.parent.mkdir(parents=True)
if not db_path.exists():
    db_path.open('w').close()

helper = MediaParserSqlHelper(db_path)

img_path = input("请输入目标图片文件夹:")
if img_path:
    img_path = Path(img_path)
    if not img_path.is_dir():
        print("输入错误")
    for img in img_path.glob("**/*"):
        try:
            orginal_hash = imagehash.whash(Image.open(img))
            hash = str(orginal_hash)

            name = img.name
            if helper.is_image_exists(hash):
                print("[INFO] {1}数据库中存在，跳过")
            helper.insert_image(name,hash,str(img))
        except UnidentifiedImageError as err:
            print(f"Error: {img} 打不开，删除该文件")
            img.unlink()
    
video_path = Path(input("请输入目标视频文件夹:"))

if not video_path.is_dir():
    print("输入错误")
for video in video_path.glob("**/*"):
    hash = str(videohash.VideoHash(path = str(video)))

    name = video.name
    if helper.is_video_exists(hash):
        print("[INFO] {1}数据库中存在，跳过")    
    helper.insert_video(name,hash,str(video))    
