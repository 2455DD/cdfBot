"""提供MediaParser的Config设置"""

from pathlib import Path

from alicebot import ConfigModel


class MediaParserConfig(ConfigModel):
    """媒体转化器设置，包括数据库路径、下载地址等等
    """
    __config_name__ = "media_parser"
    db_path:Path = Path("db/media.db")
    download_root_path:Path = Path("assets/downloads")
