"""所有MediaParser相关的异常类"""
class MediaDuplicatedError(RuntimeError):
    """媒体文件重复异常类"""
    def __init__(self,seg_type:str,file_name:str) -> None:
        super().__init__()
        self.type = seg_type
        self.name = file_name

    def __str__(self) -> str:
        err = f"{self.type}{self.name}与已保存数据重复"
        return err
