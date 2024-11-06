import os
from alicebot import Plugin,ConfigModel
from alicebot.event import MessageEvent
from alicebot.adapter.cqhttp import CQHTTPMessage,CQHTTPMessageSegment
from typing import Dict, List,Union
from structlog.stdlib import get_logger
import sqlite3
import httpx
import aiofiles
from PIL import Image
import imagehash
import videohash
from datetime import datetime
from pathlib import Path
from .media_sql_helper import MediaParserSqlHelper
from ._errors import MediaDuplicatedError
logger = get_logger()

class MediaParserConfig(ConfigModel):
    __config_name__ = "media_parser"
    db_path:str = "db/media.db"
    download_root_path:str = "assets/downloads"

media_type_chinese:Dict[str,str] = {
    "image":"图片",
    "video":"视频",
    "file":"文件",
    "text":"文本"
}
    

class MediaParser(Plugin[MessageEvent,MediaParserSqlHelper,MediaParserConfig]):
    """MediaParser

    解析并存储视频和图片

    Args:
        Plugin (_type_): _description_
    """
    priority = 2
    def __init_state__(self) -> sqlite3.Connection | None:
        db_path = Path(self.config.db_path)
        try:
            if not db_path.parent.exists():
                logger.warning(f"媒体数据库父目录{db_path.parent.absolute()}不存在，尝试创建")
                db_path.parent.mkdir(parents=True)
            if not db_path.exists():
                logger.warning(f"媒体数据库{db_path}不存在，尝试创建")
                db_path.open("w").close()
        except OSError as err:
            logger.error(err)
            return None
        else:    
            return MediaParserSqlHelper(self.config.db_path)
    
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
    
    async def _save_file(self,path:str,url:str):
        async with aiofiles.open(path,mode="wb") as f:        
            async with httpx.AsyncClient() as client:
                async with client.stream("get",url) as resp:
                    resp.raise_for_status()
                    async for chuck in resp.aiter_bytes():
                            await f.write(chuck)        
            
    async def _handle_multimedia_segment(self,seg:CQHTTPMessageSegment,under_forward:bool=False):
        if under_forward:
            root_path = self.config.download_root_path + "/" + "forward_" + datetime.now().strftime("%Y%m%d-%H%M%S")
            file_path = f"{root_path}/{seg.type}/{seg['file']}"
        else:
            file_path = f"{self.config.download_root_path}/{seg.type}/{seg['file']}"
        
        
        if not os.path.exists(self.config.download_root_path):
            os.makedirs(self.config.download_root_path)
        
        try:
            if seg.type == "image":
                url = seg["url"]
                await self._save_file(file_path,url)
                
            elif seg.type == "video":
                url:str = seg["url"]
                if url.startswith("http"):
                    await self._save_file(file_path,url)
                else:
                    if Path(url).is_file():
                        file_path = url
                    else:
                        logger.warning("下载视频失败: 非法的URL")
                        return
            elif seg.type == "file":
                raise NotImplementedError("文件形式的消息暂不允许") #TODO: 实现文件形式内容
            elif seg.type == "text":
                return
            else:
                raise ValueError(f"该方法不处理{seg.type}")
        except httpx.HTTPStatusError as e:
            logger.error(f"{seg['file']} 无法访问：{seg['url']} \n{e}")
            return
        except Exception as e:
            logger.error(f"{seg['file']} 出错： \n{e}")
            return
          
        match seg.type:
            case "image":
                orginal_hash = imagehash.whash(Image.open(file_path))
                hash = str(orginal_hash)
                if self.state.is_image_exists(hash):
                    if Path(self.state.get_image_path_by_hash(hash)).exists():
                        raise MediaDuplicatedError(media_type_chinese[seg.type],seg["file"])
                    else:
                        self.state.delete_image_record(hash)
                        self.state.insert_image(seg["file"],hash,file_path)
                else:
                    self.state.insert_image(seg["file"],hash,file_path)
            case "video":
                hash = str(videohash.VideoHash(path = file_path)) #TODO
                if self.state.is_video_exists(hash):
                    if Path(self.state.get_video_path_by_hash(hash)):
                        raise MediaDuplicatedError(media_type_chinese[seg.type],seg["file"])
                    else:
                        self.state.delete_image_record(hash)
                        self.state.insert_video(seg["file"],hash,file_path)
                else:
                    self.state.insert_video(seg["file"],hash,file_path)
            case "file":
                hash = None #TODO: 实现文件形式内容
            case "_":
                raise ValueError(f"{type} 不被支持")

        logger.debug(f"{seg.type}: \t{hash}")
        
    
    async def parse_forward_msgs(self,forward_msg:CQHTTPMessage) ->  List[CQHTTPMessageSegment]:
        # raw_forward_msgs:List[CQHTTPMessage] = await self.event.adapter.call_api("get_forward_msg"
        #                                                 ,id=forward_msg[0].get("id"))
        node_segs:List[CQHTTPMessageSegment] = list()
        logger.debug("成功解析合并转发消息")
        if len(forward_msg)>1:
            raise NotImplementedError()
        
        for node in forward_msg[0]["content"]: # 结点列表中不同节点
            nodes = node["message"]
            for node in nodes:
                match node["type"]:
                    case "image":
                        file_name:str = node["data"]["file_unique"]
                        if not file_name.endswith((".jpg",".png",".tiff",".webp")):
                            file_name = file_name +  ".jpg"
                        node_segs += CQHTTPMessageSegment(type="image",
                            data={
                                "file": file_name,
                                "url": node["data"]["url"],
                                "type": None,
                            },)
                    case "video":
                        node_segs += CQHTTPMessageSegment(type="video",
                            data={
                                "file": node["data"]["file_unique"],
                                "url": node["data"]["url"],
                                "file_id":node["data"]["file_id"],
                                "type": None,}
                            )
                    case "forward":
                        logger.debug(f"{forward_msg[0].get("id")}：递归解析合并{node["data"]["id"]}")
                        logger.warning(f"根据协议端相关Issue#216，暂时先使用缓解手段")
                        
                        message = node["data"]
                        node_segs.extend(await self.parse_forward_msgs([message]))
        
        return node_segs
    
    async def handle(self) -> None:
        if self.event.message[0].type == "forward":
            segs = await self.parse_forward_msgs(self.event.message)
            err_count = 0
            root_path = self.config.download_root_path + "/" + "forward_" + datetime.now().strftime("%Y%m%d-%H%M%S")
            logger.info("从{}接收到待存储多媒体消息{}条，开始处理".format(
                self.event.get_sender_id(),
                len(segs)
            ))
            for i,seg in enumerate(segs):
                logger.info("当前处理类型{} ({}/{})".format(media_type_chinese[seg.type],i+1,len(segs)))
                try:
                    await self._handle_multimedia_segment(seg)
                except NotImplementedError:
                    logger.warn(f"处理{seg.type}类别方法仍未实现")
                    err_count += 1
                except httpx.HTTPStatusError as err:
                    logger.error("下载中出现网络问题: {}".format(err))
                    await self.event.reply("出现网络错误： {}".format(err))
                    err_count += 1
                except MediaDuplicatedError as err:
                    logger.info(str(err)+"，跳过该条")
                    err_count += 1
                except Exception as err:
                    logger.error("未捕获错误,{}".format(err))
                    await self.event.reply("出现严重错误： {}".format(err))
                    err_count +=1        
            await self.event.reply(f"下载已完成，有消息{len(segs)}条，出现错误{err_count}条")
                    
        else:
            logger.info("从{}接收到待存储多媒体消息1条，开始处理".format(
                self.event.get_sender_id(),
            ))
            err_count=0
            for i,node_seg in enumerate(self.event.message):
                    try:
                        await self._handle_multimedia_segment(node_seg)
                    except NotImplementedError:
                        logger.warn("方法未实现")
                    except httpx.HTTPStatusError as err:
                        logger.error("下载中出现网络问题: {}".format(err))
                        await self.event.reply("出现网络错误： {}".format(err))
                        err_count += 1
                    except MediaDuplicatedError as err:
                        logger.info(str(err)+"，跳过该条")
                        err_count += 1
                    except Exception as e:
                        logger.error(f"{node_seg["file"]}出现错误，跳过\n{e}")
                        err_count+=1
                        await self.event.reply(f"下载文件中({i}/{len(self.event.message)})出现错误")
            await self.event.reply(f"下载已完成，有消息{len(self.event.message)}条，出现错误{err_count}条")

    async def rule(self) -> bool:
        if self.event.adapter.name != 'cqhttp' or self.event.type != "message":
            return False
        
        for seg in self.event.message:
            if seg.type in ["image","video","file","forward"]:
                return True
            
        return False