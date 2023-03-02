**Example**

```
from upload import Upload
from fastapi import UploadFile
import hashlib
import asyncio

class MD5Hash:
    def __init__(self):
        self._hash_md5 = hashlib.md5()

    async def update_hash(self, chunk: bytes) -> None:
        """
        updates the md5 hash
        """
        self._hash_md5.update(chunk)

    def digest(self) -> str:
        """
        returns the hex digest of the md5 hash
        """
        return self._hash_md5.hexdigest()





if __name__ == "__main__":
  file_stream:UploadFile=""
  bucket:str = "" #AWS s3 bucket
  file_path :str = "" #AWS s3 file path
  upload = Upload(
    file=file_stream,
    bucket=bucket,
    file_path=file_path)
  md5 = MD5Hash()
  asyncio.run(upload.upload(hash_file=md5))
  md5_hash = md5.digest()
```
