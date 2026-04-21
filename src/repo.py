"""asyncpg-based user repository."""

from uuid import UUID

import asyncpg

from contracts import DuplicateNicknameError, User


class UserRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert(self, user: User, workflow_id: str) -> None:
        try:
            await self._pool.execute(
                "INSERT INTO users (id, nickname, created_at, last_workflow_id) "
                "VALUES ($1, $2, $3, $4)",
                user.id,
                user.nickname,
                user.created_at,
                workflow_id or None,
            )
        except asyncpg.UniqueViolationError as exc:
            raise DuplicateNicknameError(user.nickname) from exc

    async def list_all(self) -> list[User]:
        rows = await self._pool.fetch(
            "SELECT id, nickname, created_at, last_workflow_id FROM users ORDER BY created_at"
        )
        return [User(**dict(r)) for r in rows]

    async def get(self, user_id: UUID) -> User | None:
        row = await self._pool.fetchrow(
            "SELECT id, nickname, created_at, last_workflow_id FROM users WHERE id = $1",
            user_id,
        )
        return User(**dict(row)) if row else None

    async def delete(self, user_id: UUID) -> None:
        await self._pool.execute("DELETE FROM users WHERE id = $1", user_id)
