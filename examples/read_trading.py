"""Read transactions with explicit snapshot boundaries. Requires EXN_* credentials."""

import asyncio
import os
from contextlib import aclosing

from exness import (
    AsyncExnessClient,
    Credentials,
    ExnessError,
    TradingStreamGap,
    TradingStreamOptions,
)
from exness.trading import TradingSnapshotEvent


async def main() -> None:
    async with (
        AsyncExnessClient(
            credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
            account_id=os.environ["EXN_ACCOUNT_ID"],
            discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
        ) as client,
        aclosing(client.stream_trading(options=TradingStreamOptions(max_reconnects=3))) as stream,
    ):
        async for event in stream:
            if isinstance(event, TradingStreamGap):
                print("Connection lost: invalidate cached state and await a fresh snapshot.")
            elif isinstance(event, TradingSnapshotEvent):
                print(
                    f"New baseline: {len(event.payload.orders)} orders, "
                    f"{len(event.payload.positions)} positions. Replace any previous cache."
                )
            else:
                print(
                    f"Transaction status={event.status!r}, operation={event.operation_id!r}; "
                    f"changed orders={len(event.payload.orders)}, "
                    f"positions={len(event.payload.positions)}, deals={len(event.payload.deals)}"
                )
    print("Stream ended: cached state is no longer current.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except (ExnessError, KeyError) as error:
        raise SystemExit(str(error)) from None
