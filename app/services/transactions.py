"""Transaction helpers shared by service modules."""

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.errors import translate_integrity_error


async def commit_session(session: AsyncSession) -> None:
    """Commit the current unit of work, translating integrity failures.

    The session is always rolled back when the commit fails so callers never
    observe a partially applied transaction.
    """
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise translate_integrity_error(exc) from exc
    except SQLAlchemyError:
        await session.rollback()
        raise
