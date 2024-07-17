import os
from alicebot import Plugin,ConfigModel
from alicebot.event import MessageEvent
from alicebot.adapter.cqhttp import CQHTTPMessage,CQHTTPMessageSegment
from typing import List,Union
from structlog.stdlib import get_logger
import sqlite3
import httpx
import aiofiles
from PIL import Image
import imagehash
import videohash

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
        if not os.path.exists(self.config.db_path):
            logger.error("数据库无法打开，跳过链接")
            return
        else:    
            conn = sqlite3.connect(self.config.db_path,timeout=5)
        return conn
    
    async def _save_metadata_to_db(self,path:str,type:str):
        assert os.path.exists(path)
        match type:
            case "image":
                image_hash = str(imagehash.crop_resistant_hash(Image.open(path), min_segment_size=500, segmentation_image_size=1000))
                pass
            case "video":
                pass
            case "_":
                raise ValueError(f"{type} 不被支持")
            
    async def _handle_multimedia_segment(self,seg:CQHTTPMessageSegment):
        # TODO: 数据库处理
        if seg.type == "image":
            url = seg["url"]
        elif seg.type == "video":
            url = seg["url"]
        elif seg.type == "file":
            raise NotImplementedError("文件形式的消息暂不允许") #TODO: 实现文件形式内容
        elif seg.type == "text":
            return
        else:
            raise ValueError(f"该方法不处理{seg.type}")
        
        file_name = f"{self.config.download_root_path}/{seg.type}/{seg['file']}"
        
        if not os.path.exists(self.config.download_root_path):
            os.makedirs(self.config.download_root_path)
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                async with aiofiles.open(file_name,mode="wb") as f:
                    await f.write(await resp.aread())
        
        match seg.type:
            case "image":
                orginal_hash = imagehash.whash(Image.open(file_name))
                hash = str(orginal_hash)
            case "video":
                hash = str(videohash.VideoHash(file_name)) #TODO
            case "file":
                hash = None #TODO: 实现文件形式内容
            case "_":
                raise ValueError(f"{type} 不被支持")

        logger.debug(hash)
        
    
    async def parse_forward_msgs(self,forward_msg:CQHTTPMessage) ->  List[CQHTTPMessageSegment]:
        raw_forward_msgs:List[CQHTTPMessage] = await self.event.adapter.call_api("get_forward_msg"
                                                        ,id=forward_msg[0].get("id"))
        
        node_segs:List[CQHTTPMessageSegment] = list()
        
        for node in raw_forward_msgs["messages"]: # 结点列表中不同节点
            for node_msg_txt in node["content"]:
                match node_msg_txt["type"]:
                    case "image":
                        node_segs += CQHTTPMessageSegment(type="image",
                            data={
                                "file": node_msg_txt["data"]["file"],
                                "url": node_msg_txt["data"]["url"],
                                "type": None,
                            },)
                    case "video":
                        node_segs += CQHTTPMessageSegment(type="video",
                            data={
                                "file": node_msg_txt["data"]["file"],
                                "url": node_msg_txt["data"]["url"],
                                "type": None,
                            },)
                    case "forward":
                        logger.debug(f"{forward_msg[0].get("id")}：递归解析合并{node_msg_txt["data"]["id"]}")
                        temp_seg_node = CQHTTPMessage(CQHTTPMessageSegment(type="forward",
                                                             data={
                                                                 "id":str(node_msg_txt["data"]["id"])
                                                             }
                                                    ))
                        
                        node_segs.extend(await self.parse_forward_msgs(temp_seg_node))
        
        return node_segs
    
    async def handle(self) -> None:
        if self.event.message[0].type == "forward":
            segs = await self.parse_forward_msgs(self.event.message)
            for seg in segs:
                try:
                    await self._handle_multimedia_segment(seg)
                except NotImplementedError:
                    logger.warn(f"处理{seg.type}类别方法仍未实现")
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