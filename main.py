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
from launart import Launart,Launchable
import kayaku

class ConfigService(Launchable):
    id = "bot.config"

    @property
    def required(self):
        return set()

    @property
    def stages(self):
        return {"preparing", "cleanup"}

    async def launch(self, _mgr: Launart):
        async with self.stage("preparing"):
            # 在 preparing 阶段预加载模型并写入 JSON Schema
            kayaku.bootstrap()

        async with self.stage("cleanup"):
            # 在 cleanup 阶段写入所有模型
            kayaku.save_all()

# TODO: kayaku initialization
def main():
    bcc = create(Broadcast)
    saya = create(Saya)
    mgr = Launart()
    
    kayaku.initialize({
        "{**}":"./config/{**}",
        "{**}.credential":"./secrets/credential.jsonc:{**}"
    })
    
    mgr.add_service(ConfigService())
    
    app = Ariadne(
        connection=config(
            1,
            launch_manager=mgr
        )
    )

    with saya.module_context():
        for module_info in pkgutil.iter_modules(["modules"]):
            if module_info.name.startswith("_"): # 不导入`_`开头模组
                continue
            saya.require(f"modules.{module_info.name}")
            
    app.launch_blocking()

if __name__ == "__main__":
    main()