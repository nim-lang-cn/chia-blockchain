import asyncio

import aiosqlite
import copy
from typing import Dict

from aiosqlite.core import Connection

class DBWrapper:
    """
    This object handles HeaderBlocks and Blocks stored in DB used by wallet.
    """

    db: Dict[str,aiosqlite.Connection]
    lock: asyncio.Lock

    def __init__(self, connection: Dict[str,aiosqlite.Connection]):
        self.db = dict()
        self.db = connection
        self.lock = asyncio.Lock()

    async def begin_transaction(self, str="chia"):
        cursor = await self.db[str].execute("BEGIN TRANSACTION")
        await cursor.close()

    async def rollback_transaction(self,str="chia"):
        # Also rolls back the coin store, since both stores must be updated at once
        if self.db[str].in_transaction:
            cursor = await self.db[str].execute("ROLLBACK")
            await cursor.close()

    async def commit_transaction(self,str="chia"):
        if isinstance(self.db[str], aiosqlite.Connection):
            await self.db[str].commit()
