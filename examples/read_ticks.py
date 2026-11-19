"""Live ticks with explicit gap handling. Requires local EXN_* credentials."""

import asyncio
import os
from contextlib import aclosing

from exness import AsyncExnessClient, Credentials, ExnessError, TickStreamGap, TickStreamOptions


async def main() -> None:
    async with (
        AsyncExnessClient(
            credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
            account_id=os.environ["EXN_ACCOUNT_ID"],
            discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
        ) as client,
        aclosing(
            client.stream_ticks(
                [os.environ.get("EXN_INSTRUMENT", "EURUSD")],
                options=TickStreamOptions(max_reconnects=3),
            )
        ) as stream,
    ):
        async for event in stream:
            if isinstance(event, TickStreamGap):
                print(
                    f"Stream interrupted; reconnect {event.reconnect_attempt}. "
                    "Missed ticks cannot be replayed."
                )
            else:
                print(event.model_dump_json())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except (ExnessError, KeyError) as error:
        raise SystemExit(str(error)) from None
