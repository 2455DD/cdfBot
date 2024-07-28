import sqlite3
from typing import *
from pathlib import Path

class MediaParserSqlHelper():
    def __init__(self,db_path:str) -> None:
        assert Path(db_path).parent.exists(),"媒体数据库父目录不存在"
        assert not Path(db_path).is_dir(),"媒体数据库路径是目录"
        
        self.db_path = Path(db_path)
        self.init_db()
        
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS "Image" (
            "id" INTEGER NOT NULL,
            "file_name" TEXT NOT NULL,
            "hash" TEXT NOT NULL,
            PRIMARY KEY ("id")
            );

            CREATE TABLE IF NOT EXISTS  "Image_Tag_Relation" (
            "id" INTEGER NOT NULL,
            "image_id" INTEGER,
            "tag_id" INTEGER,
            PRIMARY KEY ("id"),
            CONSTRAINT "Image2Tag_image" FOREIGN KEY ("image_id") REFERENCES "Image" ("id"),
            CONSTRAINT "Image2Tag_tag" FOREIGN KEY ("tag_id") REFERENCES "Tag" ("id")
            );

            CREATE TABLE  IF NOT EXISTS  "Tag"(
            "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "name" TEXT NOT NULL,
            "category" integer NOT NULL,
            "description" TEXT
            );

            CREATE TABLE  IF NOT EXISTS  "Video"(
            "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
            "file_name" TEXT,
            "hash" TEXT
            );

            CREATE TABLE  IF NOT EXISTS "Video_Tag_Relation"(
            "id" INTEGER NOT NULL,
            "video_id" integer NOT NULL,
            "tag_id" INTEGER NOT NULL,
            PRIMARY KEY ("id") ON CONFLICT FAIL,
            CONSTRAINT "视频id" FOREIGN KEY ("video_id") REFERENCES "Video" ("id"),
            CONSTRAINT "标签id" FOREIGN KEY ("tag_id") REFERENCES "Tag" ("id")
            );

            CREATE VIEW IF NOT EXISTS "ImagesList" AS SELECT
                Image.file_name AS name, 
                Image.hash AS hash, 
                GROUP_CONCAT(name,";") Tag
            FROM
                Image,
                Image_Tag_Relation,
                Tag
            WHERE
                Image.id = Image_Tag_Relation.image_id AND
                Tag.id = Image_Tag_Relation.tag_id;


            CREATE VIEW IF NOT EXISTS "VideosList" AS SELECT
                Video.file_name AS video, 
                Video.hash AS hash, 
                GROUP_CONCAT(name,";") Tag
            FROM
                Video
                INNER JOIN
                Video_Tag_Relation
                ON 
                    Video.id = Video_Tag_Relation.video_id
                INNER JOIN
                Tag
                ON 
                    Video_Tag_Relation.tag_id = Tag.id
            GROUP BY
                Video.id;
            ''')
        conn.autocommit = False
        self.conn = conn
    
    #---------------- Insert -------------------------
    def insert_image(self,image_name:str,image_hash:str) :
        try:
            with self.conn.cursor() as cur:
                cur.execute('''
                            INSERT INTO Image(file_name,hash)
                            VALUE (:name,:hash)
                            ''',{"name":image_name,"hash":image_hash})
                self.conn.commit() 
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
              
    def insert_video(self,video_name:str,video_hash:str):
        try:
            with self.conn.cursor() as cur:
                cur.execute('''
                            INSERT INTO Image(file_name,hash)
                            VALUE (:name,:hash)
                            ''',{"name":video_name,"hash":video_hash})
                self.conn.commit() 
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
            
    #---------------- Query ------------------------
  
    def is_image_exists(self,image_hash:str)->bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute('''
                            SELECT 1 FROM Image WHERE Image.hash = '?' LIMIT 1;
                            ''',(image_hash))
                self.conn.commit()
                if cur.fetchone() is not None:
                    return True
                else:
                    return False  
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err


    def is_video_exists(self,video_hash:str)->bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute('''
                            SELECT 1 FROM Video WHERE Video.hash = '?' LIMIT 1;
                            ''',(video_hash))
                self.conn.commit()
                if cur.fetchone() is not None:
                    return True
                else:
                    return False  
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
    
if __name__ == "__main__":
    MediaParserSqlHelper("./.develop_assets/media.db")