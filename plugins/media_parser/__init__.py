from alicebot import Plugin,ConfigModel
from alicebot.event import MessageEvent
from alicebot.adapter.cqhttp import CQHTTPMessage,CQHTTPMessageSegment
from typing import List
from structlog.stdlib import get_logger
import sqlite3
import aiohttp
import aiofiles

logger = get_logger()

class MediaParserConfig(ConfigModel):
    __config_name__ = "media_parser"
    db_path:str = "db/media.db"
    download_root_path:str = "assets/downloads"

class MediaParser(Plugin[MessageEvent,sqlite3.Connection,MediaParserConfig]):
    """MediaParser

    解析并存储视频和图片

    Args:
        Plugin (_type_): _description_
    """
    priority = 2
    def __init_state__(self) -> sqlite3.Connection | None:
        conn = sqlite3.connect(self.config.db_path,timeout=5)
        return conn
    
    
    async def _handle_multimedia_segment(self,seg:CQHTTPMessageSegment):
        
        # TODO: 数据库处理
        if seg.type == "image":
            url = seg["url"]
        elif seg.type == "video":
            url = seg["url"]
        elif seg.type == "file":
            raise NotImplementedError("文件形式的消息暂不允许")
        elif seg.type == "text":
            return
        else:
            raise ValueError(f"该方法不处理{seg.type}")
        
        file_name = self.config.download_root_path + f"/{seg.type}/{seg['file']}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    async with aiofiles.open(file_name,mode="wb") as f:
                        f.write(await resp.read())
                        
                
        
    
    async def handle(self) -> None:
        if self.event.message.type == "forward":
            forward_msg:CQHTTPMessage = self.event.message
            
            forward_msgs:List[CQHTTPMessage] = await self.event.adapter.call_api("get_forward_msg"
                                                                    ,id=forward_msg[0].get("id"))
            for node in forward_msgs: # 结点列表中不同节点
                for node_msg in node[0].get("content"):
                    for node_seg in node_msg:
                        try:
                            await self._handle_multimedia_segment(node_seg)
                        except NotImplementedError:
                            logger.info()

        else:
            for node_seg in self.event.message:
                    try:
                        await self._handle_multimedia_segment(node_seg)
                    except NotImplementedError:
                        logger.info()
        

    async def rule(self) -> bool:
        if self.event.adapter.name != 'cqhttp' or self.event.type != "message":
            return False
        
        for seg in self.event.message:
            if seg.type in ["image","video","file","forward"]:
                return True
            
        return False