from kayaku import config
from pathlib import Path

@config("module.eroero_saver")
class EroeroSaverConfig():
    img_name_format: str
    db_path: str