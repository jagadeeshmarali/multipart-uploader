# from types_aiobotocore_s3.client import S3Client
from loguru import logger
from .hash_file import HashFileProtocol


class DirectUpload:
    def __init__(self, client: any, bucket: str, file_path: str, file: bytes):
        self.client = client
        self.bucket = bucket
        self.file_path = file_path
        self.file = file

    async def upload(self, hash_file: HashFileProtocol) -> str:
        """
        Uploads the file directly to s3 with the specified bucket and  path
        """

        await self.client.put_object(
            Bucket=self.bucket,
            Key=self.file_path,
            Body=self.file,
        )
        logger.info("Completed uploading file.")
        await hash_file.update_hash(chunk=self.file)

        return hash_file.digest()
