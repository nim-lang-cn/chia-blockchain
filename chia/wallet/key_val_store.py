from typing import Any, Dict

import aiosqlite

from chia.util.byte_types import hexstr_to_bytes
from chia.util.db_wrapper import DBWrapper
from chia.util.streamable import Streamable


class KeyValStore:
    """
    Multipurpose persistent key-value store
    """

    db_connection: Dict[str,aiosqlite.Connection]
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper, db:str="chia"):
        self = cls()
        self.db_wrapper = db_wrapper
        self.db_connection = db_wrapper.db
        await self.db_connection[db].execute("pragma journal_mode=wal")
        await self.db_connection[db].execute("pragma synchronous=2")

        await self.db_connection[db].execute(
            ("CREATE TABLE IF NOT EXISTS key_val_store(" " key text PRIMARY KEY," " value text)")
        )

        await self.db_connection[db].execute("CREATE INDEX IF NOT EXISTS name on key_val_store(key)")

        await self.db_connection[db].commit()
        return self

    async def _clear_database(self, db="chia"):
        cursor = await self.db_connection[db].execute("DELETE FROM key_val_store")
        await cursor.close()
        await self.db_connection[db].commit()

    async def get_object(self, key: str, type: Any, db="chia") -> Any:
        """
        Return bytes representation of stored object
        """

        cursor = await self.db_connection[db].execute("SELECT * from key_val_store WHERE key=?", (key,))
        row = await cursor.fetchone()
        await cursor.close()

        if row is None:
            return None

        return type.from_bytes(hexstr_to_bytes(row[1]))

    async def set_object(self, key: str, obj: Streamable, db="chia"):
        """
        Adds object to key val store
        """
        async with self.db_wrapper.lock:
            cursor = await self.db_connection[db].execute(
                "INSERT OR REPLACE INTO key_val_store VALUES(?, ?)",
                (key, bytes(obj).hex()),
            )
            await cursor.close()
            await self.db_connection[db].commit()
