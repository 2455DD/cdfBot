# s3_uploader.py
import asyncio
from pathlib import Path
import boto3
from botocore.client import Config
from threading import Lock
from httpx import URL


class S3Uploader:
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self,
                 endpoint_url: str = 'https://s4.eu.mega.io',
                 access_key='your_access_key',
                 secret_key='your_secret_key',
                 bucket='qq-bot-bucket',
                 region='us-east-1'):
        if hasattr(self, '_initialized'):
            return
        self._client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name=region
        )
        self.bucket = bucket
        self._initialized = True  # 防止 __init__ 多次执行

    def upload_file(self, local_path: Path, s3_key: str):
        if not local_path.is_file():
            raise FileNotFoundError(local_path)
        with local_path.open("rb") as f:
            self._client.upload_fileobj(f, self.bucket, s3_key)

    # async def async_upload_file(self, local_path: str, s3_key: str):
    #     """Upload File in s3_key"""
    #     loop = asyncio.get_running_loop()
    #     await loop.run_in_executor(None, self.upload_file, Path(local_path), s3_key)
