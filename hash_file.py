import hashlib
from typing import Protocol


class HashFileProtocol(Protocol):
    async def update_hash(self, chunk: bytes) -> None:
        ...

    def digest(self) -> str:
        ...