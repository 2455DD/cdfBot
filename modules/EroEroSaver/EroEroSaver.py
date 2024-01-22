from graia.ariadne.app import Ariadne
from graia.ariadne.event.message import GroupMessage
from graia.ariadne.message.chain import MessageChain
from graia.ariadne.message.element import Image
from graia.ariadne.model import Group
from graia.saya import Channel
from graia.saya.builtins.broadcast.schema import ListenerSchema
from pathlib import Path

from modules.access_control.access_control import check_group,check_member

channel = Channel.current()
channel.name("EroEroSaver")
channel.description("存储指定群组的瑟图和视频")
channel.author("Cannedfish")

@channel.use(
    ListenerSchema(
        listening_events=[GroupMessage]),
        Decorator=[check_group(),check_member()]
    )
async def EroeroSave(app: Ariadne, group: Group, message: MessageChain):
    if Image in message:
        for element in message[Image]:
            pass