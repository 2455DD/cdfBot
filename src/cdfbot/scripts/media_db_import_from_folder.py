"""将图片和视频文件夹中的文件导入到数据库中"""
from pathlib import Path

import imagehash
import videohash  # type:ignore[import]
from PIL import Image, UnidentifiedImageError

from plugins.media_parser.media_sql_helper import MediaParserSqlHelper

db_path = Path(input("请输入目标数据库路径:"))

if not db_path.parent.exists():
    db_path.parent.mkdir(parents=True)
if not db_path.exists():
    db_path.open('w', encoding="utf-8").close()

helper = MediaParserSqlHelper(db_path)

img_path_str = input("请输入目标图片文件夹:")
if img_path_str:
    img_path = Path(img_path_str)
    if not img_path.is_dir():
        print("输入错误")
    media_hash: str = ""
    for img in img_path.glob("**/*"):
        try:
            # pylint: disable=invalid-name
            media_hash = str(imagehash.whash(Image.open(img)))

            name = img.name
            if helper.is_image_exists(media_hash):
                print("[INFO] {1}数据库中存在，跳过")
            helper.insert_image(name, media_hash, str(img))
        except UnidentifiedImageError as err:
            print(f"Error: {img} 打不开")
            # img.unlink()

video_path = Path(input("请输入目标视频文件夹:"))

if not video_path.is_dir():
    print("输入错误")
for video in video_path.glob("**/*"):
    # pylint: disable=invalid-name
    media_hash = str(videohash.VideoHash(path=str(video)))

    name = video.name
    if helper.is_video_exists(media_hash):
        print("[INFO] {1}数据库中存在，跳过")
    helper.insert_video(name, media_hash, str(video))
