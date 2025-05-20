""" MediaParser中SQL通信相关代码子模块 """

import sqlite3
from pathlib import Path
from structlog.stdlib import get_logger

logger = get_logger()

class MediaParserSqlHelper():
    """媒体转换器数据库助手类"""

    def __init__(self, db_path: str | Path) -> None:
        if isinstance(db_path, str):
            db_path = Path(db_path)
        assert db_path.parent.exists(), "媒体数据库父目录不存在"
        assert not db_path.is_dir(), "媒体数据库路径是目录"
        self.db_path: Path = db_path
        self.init_db()

    def init_db(self):
        """初始化数据库，特别是创建表结构
        """
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

    # ---------------- Insert -------------------------
    def insert_image(self, image_name: str, image_hash: str, image_path: str):
        """插入图片记录

        Args:
            image_name (str): 图片文件名
            image_hash (str): 图片哈希值，目前存储imagehash的whash值
            image_path (str): 目标路径

        Raises:
            sqlite3.OperationalError: 数据库操作错误
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        INSERT INTO Image(file_name,file_path,hash)
                        VALUES (?,?,?)
                        ''', (image_name, image_path, image_hash))
            self.conn.commit()
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()

    def insert_video(self, video_name: str, video_hash: str, video_path: str):
        """插入视频记录

        Args:
            video_name (str): 视频文件名
            video_hash (str): 视频哈希值，目前用videohash库计算
            video_path (str): 目标路径

        Raises:
            sqlite3.OperationalError: 数据库操作错误
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        INSERT INTO Video(file_name,file_path,hash)
                        VALUES (?,?,?)
                        ''', (video_name, video_path, video_hash))
            self.conn.commit()
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()

    # ---------------- Query ------------------------

    def is_image_exists(self, image_hash: str) -> bool:
        """检查图片是否存在, 避免重复

        Args:
            image_hash (str): 查询hash值

        Raises:
            sqlite3.OperationalError: 数据库错误

        Returns:
            bool: `True`时存在
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        SELECT 1 FROM Image WHERE Image.hash = ?
                        ''', (image_hash,))
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

    def is_video_exists(self, video_hash: str) -> bool:
        """检查视频是否存在, 避免重复

        Args:
            video_hash (str): 查询hash值

        Raises:
            sqlite3.OperationalError: 数据库错误

        Returns:
            bool: `True`时存在
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        SELECT 1 FROM Video WHERE Video.hash = ?
                        ''', (video_hash,))
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

    def get_image_path_by_hash(self, image_hash: str) -> str:
        """获取数据库中图片路径

        Args:
            image_hash (str): 查询hash值

        Raises:
            sqlite3.OperationalError: 数据库错误

        Returns:
            str: 数据库中图片路径
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        SELECT Image.file_path FROM Image WHERE Image.hash = ?
                        ''', (image_hash,))
            self.conn.commit()
            result = cur.fetchone()
            return result[0]
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()

    def get_video_path_by_hash(self, video_hash: str) -> str:
        """获取数据库中视频路径

        Args:
            video_hash (str): 查询hash值

        Raises:
            sqlite3.OperationalError: 数据库错误

        Returns:
            str: 数据库中视频路径
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        SELECT Video.file_path FROM Video WHERE Video.hash = ?
                        ''', (video_hash,))
            self.conn.commit()
            result = cur.fetchone()
            return result[0]
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()

    # -------------- 删除------------------
    def delete_image_record(self, image_hash: str):
        """删除目标图片记录

        Args:
            image_hash (str): 目标图片的hash值

        Raises:
            sqlite3.OperationalError: 数据库错误
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        DELETE FROM Image WHERE Image.hash = ?
                        ''', (image_hash,))
            self.conn.commit()
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()

    def delete_video_record(self, video_hash: str):
        """删除目标视频记录

        Args:
            video_hash (str): 目标视频的hash值

        Raises:
            sqlite3.OperationalError: 数据库错误
        """
        try:
            cur = self.conn.cursor()
            cur.execute('''
                        DELETE FROM Video WHERE Video.hash = ?
                        ''', (video_hash,))
            self.conn.commit()
        except sqlite3.OperationalError as err:
            self.conn.rollback()
            raise err
        finally:
            cur.close()

if __name__ == "__main__":
    MediaParserSqlHelper("./.develop_assets/media.db")
