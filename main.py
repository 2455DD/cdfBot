import pkgutil
from creart import create
from graia.ariadne.app import Ariadne
from graia.ariadne.connection.config import (
    HttpClientConfig,
    WebsocketClientConfig,
    config,
)
from graia.ariadne.event.message import GroupMessage
from graia.ariadne.message.chain import MessageChain
from graia.ariadne.model import Group
from graia.broadcast import Broadcast
from graia.saya import Saya

bcc = create(Broadcast)

saya = create(Saya)
app = Ariadne(
    connection=config(
        1
    )
)

with saya.module_context():
    for module_info in pkgutil.iter_modules(["modules"]):
        if module_info.name.startswith("_"): # 不导入`_`开头模组
            continue
        saya.require(f"modules.{module_info.name}")
        
app.launch_blocking()