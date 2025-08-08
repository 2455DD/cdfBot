"""该模块实现了MediaParser，解析QQ消息的模块类"""

from collections import defaultdict
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from botocore.client import BaseClient
import httpx
import imagehash
import videohash  # type: ignore
from alicebot import Plugin,Depends
from alicebot.adapter.cqhttp import CQHTTPMessage, CQHTTPMessageSegment
from alicebot.adapter.cqhttp.event import MessageEvent
# from alicebot.event import MessageEvent
from PIL import Image
from structlog.stdlib import get_logger

from ._errors import MediaDuplicatedError
from .media_sql_helper import MediaParserSqlHelper
from .config import MediaParserConfig
from ._s3_updater import S3Uploader


logger = get_logger()

MEDIA_TYPE_TRANSLATION: Dict[str, str] = {
    "image": "图片",
    "video": "视频",
    "file": "文件",
    "text": "文本"
}


class MediaParser(Plugin[MessageEvent, MediaParserSqlHelper, MediaParserConfig]):
    """MediaParser

    解析并存储视频和图片

    Args:
        Plugin (_type_): _description_
    """
    priority = 2
    database:MediaParserSqlHelper = Depends(MediaParserSqlHelper,
                                            )

    def __init_state__(self) -> MediaParserSqlHelper | None:
        db_path = Path(self.config.db_path)
        try:
            if not db_path.parent.exists():
                logger.warning(f"媒体数据库父目录{db_path.parent.absolute()}不存在，尝试创建")
                db_path.parent.mkdir(parents=True)
            if not db_path.is_file():
                logger.warning(f"媒体数据库{db_path}不存在，尝试创建")
                db_path.open("w", encoding="utf-8").close()
        except OSError as err:
            logger.error("创建数据库时发生错误", err_msg=err)
            return None
        else:
            helper = MediaParserSqlHelper(self.config.db_path)
            return helper

    async def _save_file(self, path: str, url: str, type: str):
        async with aiofiles.open(path, mode="wb") as f:
            async with httpx.AsyncClient() as client:
                async with client.stream("get", url) as resp:
                    resp.raise_for_status()
                    async for chuck in resp.aiter_bytes():
                        await f.write(chuck)

        name = Path(path).name
        if self.uploader is not None:
            self.uploader.upload_file(Path(path), f"qq-{type}/{name}")
            # await self.uploader.async_upload_file(path, f"qq-{type}/{name}")

    async def _handle_multimedia_segment(self,
                                         seg: CQHTTPMessageSegment,
                                         under_forward: bool = False):
        file_path: str = ""
        if under_forward:
            root_path: Path = self.config.download_root_path / \
                f"forward_{datetime.now().strftime("%Y%m%d-%H%M%S")}"
            file_path = str((root_path/seg.type/seg['file']).absolute())
        else:
            file_path = str((self.config.download_root_path /
                            seg.type/seg['file']).absolute())

        if not os.path.exists(self.config.download_root_path):
            os.makedirs(self.config.download_root_path)

        url: str = seg["url"] if "url" in seg else ""
        try:
            if seg.type == "image":
                await self._save_file(file_path, url, seg.type)

            elif seg.type == "video":
                if url.startswith("http"):
                    await self._save_file(file_path, url, seg.type)
                else:
                    if Path(url).is_file():
                        file_path = url
                    else:
                        logger.warning("下载视频失败: 非法的URL")
                        return
            elif seg.type == "file":
                raise NotImplementedError("文件形式的消息暂不允许")  # TODO: 实现文件形式内容
            elif seg.type == "text":
                return
            else:
                raise ValueError(f"该方法不处理{seg.type}")
        except httpx.HTTPStatusError as e:
            logger.error(f"{seg['file']} 无法访问：{seg['url']} \n{e}")
            return

        media_hash: str | None = None
        match seg.type:
            case "image":
                orginal_hash = imagehash.whash(Image.open(file_path))
                media_hash = str(orginal_hash)
                if self.state.is_image_exists(media_hash):
                    if Path(self.state.get_image_path_by_hash(media_hash)).exists():
                        raise MediaDuplicatedError(
                            MEDIA_TYPE_TRANSLATION[seg.type], seg["file"])
                    else:
                        self.state.delete_image_record(media_hash)
                        self.state.insert_image(
                            seg["file"], media_hash, file_path)
                else:
                    self.state.insert_image(seg["file"], media_hash, file_path)
            case "video":
                media_hash = str(videohash.VideoHash(path=file_path))
                if self.state.is_video_exists(media_hash):
                    if Path(self.state.get_video_path_by_hash(media_hash)):
                        raise MediaDuplicatedError(
                            MEDIA_TYPE_TRANSLATION[seg.type], seg["file"])
                    else:
                        self.state.delete_image_record(media_hash)
                        self.state.insert_video(
                            seg["file"], media_hash, file_path)
                else:
                    self.state.insert_video(seg["file"], media_hash, file_path)
            case "file":
                media_hash = None  # TODO: 实现文件形式内容
            case "_":
                raise ValueError(f"{type} 不被支持")

        logger.debug(f"{seg.type}: \t{media_hash}")

    async def _check_reply_message(self, seg: CQHTTPMessageSegment):
        pass

    async def flatten_forward_msgs(self, forward_msg: CQHTTPMessage) -> CQHTTPMessage:
        """递归将Forward消息解析为展平消息

        Args:
            forward_msg (CQHTTPMessage): 原始转发消息

        Raises:
            NotImplementedError: 未理解Forward消息的长度大于1

        Returns:
            CQHTTPMessage: 已展平消息
        """
        node_segs: CQHTTPMessage = CQHTTPMessage()
        if len(forward_msg) > 1:
            raise NotImplementedError("Forward消息的长度大于1")
        file_name: str = ""
        for node in forward_msg[0]["content"]:  # 结点列表中不同节点
            nodes = node["message"]
            for node in nodes:
                match node["type"]:
                    case "image":
                        if "file_unique" in node["data"]:
                            file_name = node["data"]["file_unique"]
                        else:
                            file_name = node["data"]["file"]
                        if not file_name.endswith((".jpg", ".png", ".tiff", ".webp")):
                            file_name = file_name + ".jpg"
                        node_segs.append(CQHTTPMessageSegment(type="image",
                                                              data={
                                                                  "file": file_name,
                                                                  "url": node["data"]["url"],
                                                                  "type": None,
                                                              },))
                    case "video":
                        if "file_unique" in node["data"]:
                            file_name = node["data"]["file_unique"]
                        else:
                            file_name = node["data"]["file"]
                        node_segs += [CQHTTPMessageSegment(type="video",
                                                           data={
                                                               "file": file_name,
                                                               "url": node["data"]["url"],
                                                               "type": None, }
                                                           )]
                    case "forward":
                        logger.debug(
                            f"解析{forward_msg[0].get("id")}中：递归解析合并{node["data"]["id"]}")
                        node_segs.extend(await self.flatten_forward_msgs(CQHTTPMessage(node)))
                    case "reply":
                        logger.error(
                            f"解析{forward_msg[0].get("id")}中：转发消息中包含回复消息,不支持")
                        # logger.debug(f"解析{forward_msg[0].get("id")}：回复信息")
                        # reply_msg_id = node["data"]["id"]
                        # await self.event.adapter.call_api("get_msg",message_id=reply_msg_id)
        return node_segs

    async def handle(self) -> None:
        self.event: MessageEvent
        if self.event.message[0].type == "forward":
            segs = await self.flatten_forward_msgs(self.event.message)
            logger.info(
                f"从{self.event.get_sender_id()}接收到待存储多媒体消息{len(segs)}条，开始处理")
        else:
            segs = self.event.message
            logger.info(f"从{self.event.get_sender_id()}接收到待存储多媒体消息1条，开始处理")
        err_counts_dict: Dict[str, List[str]] = defaultdict(list)
        err_count = 0

        self.uploader: Optional[S3Uploader] = None
        if self.config.enable_s3:
            self.uploader = S3Uploader(
                self.config.s3_endpoint,
                self.config.s3_access_key,
                self.config.s3_secret_key,
                self.config.s3_bucket
            )

        for i, seg in enumerate(segs):
            logger.info(
                f"当前处理类型{MEDIA_TYPE_TRANSLATION[seg.type]} ({i+1}/{len(segs)})")
            try:
                await self._handle_multimedia_segment(seg)
            except NotImplementedError:
                logger.warn(f"处理{seg.type}类别方法仍未实现")
                err_counts_dict["类别未实现"].append(f"第{i}条: 类型{seg['file']}")
                err_count += 1
            except httpx.HTTPStatusError as err:
                logger.error(f"下载中出现网络问题: {err}")
                await self.event.reply(f"出现网络错误：\n {err}")
                err_counts_dict["网络问题"].append(f"第{i}条: {err}")
                err_count += 1
            except MediaDuplicatedError as err:
                logger.info(str(err)+"，跳过该条")
                err_counts_dict["文件重复"].append(f"第{i}条: {err}")
                err_count += 1
            except Exception as err:
                logger.exception(f"未捕获错误,{err}")
                await self.event.reply(f"出现严重错误： {err}")
                err_counts_dict["未捕获错误"].append(
                    f"第{i}条: 错误类型{type(err)} {err}")
                err_count += 1
        if err_count > 0:
            reply_msg = f"下载已完成，有消息{len(segs)}，出现错误：\n"
            err_idx: int = 0
            for err_type, err_list in err_counts_dict.items():
                reply_msg += f"{chr(ord('A')+err_idx)}. {err_type}:\n"
                err_idx += 1
                for err_msg in err_list:
                    reply_msg += f"\t{err_msg}\n"
            await self.event.reply(reply_msg)
        else:
            await self.event.reply(f"下载已完成，有消息{len(segs)}条，全部成功")

    async def rule(self) -> bool:
        if self.event.adapter.name != 'cqhttp' or self.event.type != "message":
            return False

        for seg in self.event.message:
            if seg.type in ["image", "video", "file", "forward"]:
                return True

        return False
