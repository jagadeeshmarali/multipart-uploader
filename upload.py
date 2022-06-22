from .multipart_upload import MultipartUpload
from .direct_upload import DirectUpload
from aiobotocore.session import get_session
from fastapi import UploadFile
from typing import Protocol
from .hash_file import HashFileProtocol

MIN_MULTIPART_THRESHOLD = 1024 * 1024 * 5


class UploadProtocol(Protocol):
    async def upload(self):
        ...


class Upload:
    def __init__(
        self,
        file: UploadFile,
        bucket: str,
        file_path: str,
        chunk_size: int = 1024 * 1024 * 10,
    ):
        self.file = file
        self.bucket = bucket
        self.file_path = file_path
        self.chunk_size = chunk_size

    def _should_upload_directly(
        self, chunk: bytes, min_multipart_threshold: float
    ) -> bool:
        """
        _should_upload_directly function decides whether it has to upload file directly or begin multipart upload
        """
        return len(chunk) < min_multipart_threshold

    async def upload(self, hash_file: HashFileProtocol) -> None:
        """
        Upload first takes the initial chunk and decides whether it has to upload directly or initiate multipart upload
        """
        # scalene_profiler.start()
        # Reads the initial chunk to decide whether it has to do the direct upload or initiate multipart upload
        session = get_session()
        async with session.create_client("s3") as client:
            initial_chunk = await self.file.read(self.chunk_size)

            if self._should_upload_directly(
                chunk=initial_chunk,
                min_multipart_threshold=MIN_MULTIPART_THRESHOLD,
            ):
                direct_upload = DirectUpload(
                    client=client,
                    bucket=self.bucket,
                    file_path=self.file_path,
                    file=initial_chunk,
                )
                await direct_upload.upload(hash_file=hash_file)
            else:
                multipart_upload = MultipartUpload(
                    client=client,
                    bucket=self.bucket,
                    file_path=self.file_path,
                    initial_chunk=initial_chunk,
                    file=self.file,
                    chunk_size=self.chunk_size,
                )
                await multipart_upload.upload(hash_file=hash_file)

        # scalene_profiler.stop()