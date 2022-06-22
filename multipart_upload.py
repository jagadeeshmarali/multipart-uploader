import asyncio

from fastapi import UploadFile

# from types_aiobotocore_s3.client import S3Client
from loguru import logger
from .hash_file import HashFileProtocol


MIN_MULTIPART_THRESHOLD = 1024 * 1024 * 5


class MultipartUploadImplementation:
    def __init__(self, client: any, bucket: str, file_path: str):
        self._part_info = []
        self.bucket = bucket
        self.file_path = file_path
        self.upload_id = ""
        self.client = client

    async def __aenter__(self):
        create_multipart_upload_resp = await self._create_multipart_upload()
        self.upload_id = create_multipart_upload_resp["UploadId"]
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._complete_multipart_upload()
        except Exception as e:
            await self._abort_multipart_upload()
            raise Exception(e)

    async def _create_multipart_upload(self) -> dict:
        """
        creates the multipart upload
        """
        create_multipart_upload_resp = await self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.file_path,
            # Expires=datetime.now() + timedelta(days=EXPIRATION_DAYS),
        )
        return create_multipart_upload_resp

    async def upload_chunk(
        self,
        chunk: bytes,
        part_number: int,
    ) -> None:
        """
        Uploads the chunk and updates the part info with the Etag's
        """
        resp = await self.client.upload_part(
            Body=chunk,
            UploadId=self.upload_id,
            PartNumber=part_number,
            Bucket=self.bucket,
            Key=self.file_path,
        )

        self._part_info.append({"PartNumber": part_number, "ETag": resp["ETag"]})

    async def _complete_multipart_upload(self) -> None:
        """
        Completes the multipart upload
        """
        await self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.file_path,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": self._part_info},
        )
        logger.info("Completed uploading file.")

    async def _abort_multipart_upload(self) -> None:
        """
        Aborts the multipart upload
        """
        await self.client.abort_multipart_upload(
            Bucket=self.bucket, Key=self.file_path, UploadId=self.upload_id
        )
        logger.info("Aborted uploading file.")


class MultipartUpload:
    def __init__(
        self,
        client: any,
        bucket: str,
        file_path: str,
        initial_chunk: bytes,
        file: UploadFile,
        chunk_size: int = 1024 * 1024 * 10,
    ):
        self.client = client
        self.bucket = bucket
        self.file_path = file_path
        self.initial_chunk = initial_chunk
        self.file = file
        self.chunk_size = chunk_size

    async def upload(self, hash_file: HashFileProtocol) -> str:
        """
        This function the file to s3 in chunks by first creating the multipart upload
        then upload the initial chunk and later upload the remaining chunks.
        After uploading the chunks to s3. complete multipart upload is called.
        """
        async with MultipartUploadImplementation(
            client=self.client,
            bucket=self.bucket,
            file_path=self.file_path,
        ) as mp:
            chunk_number = 0
            current_chunk = self.initial_chunk
            while True:
                if current_chunk == b"":
                    break
                chunk_number += 1
                upload_task = mp.upload_chunk(
                    chunk=current_chunk,
                    part_number=chunk_number,
                )
                hash_task = hash_file.update_hash(chunk=current_chunk)
                read_task = self.file.read(self.chunk_size)
                _, current_chunk, _ = await asyncio.gather(
                    *[upload_task, read_task, hash_task]
                )
        return hash_file.digest()
