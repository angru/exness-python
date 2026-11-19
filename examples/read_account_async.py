"""Run with uv run python examples/read_account_async.py and local EXN_* settings."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

from exness import AsyncExnessClient, Credentials, ExnessError


def make_client() -> AsyncExnessClient:
    return AsyncExnessClient(
        credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
        account_id=os.environ["EXN_ACCOUNT_ID"],
        discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
    )


async def main() -> None:
    async with make_client() as client:
        # TaskGroup cancels and awaits sibling tasks before the client closes on error.
        async with asyncio.timeout(30), asyncio.TaskGroup() as group:
            account = group.create_task(client.get_account())
            snapshot = group.create_task(client.get_snapshot())
        print(account.result().model_dump_json(indent=2))
        print(snapshot.result().model_dump_json(indent=2))
        async with asyncio.timeout(30):
            async for item in client.iter_orders_history(
                from_time=datetime.now(UTC) - timedelta(days=1), limit=100
            ):
                print(item.model_dump_json())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except* (ExnessError, KeyError, TimeoutError):
        # TaskGroup may return multiple failures; avoid exposing arbitrary causes.
        raise SystemExit(
            "Account reads failed; check local EXN_* settings and API availability"
        ) from None
