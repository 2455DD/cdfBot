import sqlite3
from typing import *
from pathlib import Path

class MediaParserSqlHelper():
    def __init__(self,db_path:str) -> None:
        assert Path(db_path).exists(),"媒体数据库路径不存在"
        assert Path(db_path).is_dir(),"媒体数据库不是一个文件"
        
        self.db_path = Path(db_path)
        self.init_db()
        
    def init_db(self):
        #TODO: 设计init_db，负责判断数据库是否符合预期，否则创建数据库
        pass
    
    def _connect_(self):
        #TODO: 设计_connect_db，抽象负责与数据库的连接
        pass