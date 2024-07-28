import sqlite3
from typing import *
from pathlib import Path
from structlog.stdlib import get_logger

logger = get_logger()

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
            "file_path" TEXT NOT NULL,
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
            "file_path" TEXT NOT NULL,
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
            cur = self.conn.cursor()
            cur.execute('''
                        INSERT INTO Image(file_name,hash)
                        VALUES (?,?)
                        ''',(image_name,image_hash))
            self.conn.commit()
            logger.debug("成功插入")
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()
              
    def insert_video(self,video_name:str,video_hash:str):
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        INSERT INTO Video(file_name,hash)
                        VALUES (:name,:hash)
                        ''',(video_name,video_hash))
            self.conn.commit() 
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()
            
    #---------------- Query ------------------------
  
    def is_image_exists(self,image_hash:str)->bool:
        logger.debug("成功查询")
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        SELECT 1 FROM Image WHERE Image.hash = ?
                        ''',(image_hash,))
            self.conn.commit()
            if cur.fetchone() is not None:
                return True
            else:
                return False  
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()


    def is_video_exists(self,video_hash:str)->bool:
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        SELECT 1 FROM Video WHERE Video.hash = ?
                        ''',(video_hash,))
            self.conn.commit()
            if cur.fetchone() is not None:
                return True
            else:
                return False  
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()
    
if __name__ == "__main__":
    MediaParserSqlHelper("./.develop_assets/media.db")