"""Read one server subscription per connection; requires the four EXN_* variables."""

import argparse
import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import aclosing

from exness import AsyncExnessClient, Credentials, ExnessError, ServerStreamGap, ServerStreamOptions
from exness.events import AccountStateEvent, HmrSnapshot, HmrUpdate, InstrumentEvent


async def main(kind: str, names: list[str]) -> None:
    async with AsyncExnessClient(
        credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
        account_id=os.environ["EXN_ACCOUNT_ID"],
        discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
    ) as client:
        options = ServerStreamOptions(max_reconnects=3)
        stream: AsyncGenerator[
            AccountStateEvent | InstrumentEvent | HmrSnapshot | HmrUpdate | ServerStreamGap, None
        ]
        if kind == "account_state":
            stream = client.stream_account_state(options=options)
        elif kind == "instruments":
            stream = client.stream_instruments(names, options=options)
        else:
            stream = client.stream_hmr(names, options=options)
        async with aclosing(stream):
            async for event in stream:
                if isinstance(event, ServerStreamGap):
                    print(
                        "Gap: invalidate cached state. HMR waits for a new snapshot; "
                        "account/instrument recovery is described in README.md."
                    )
                elif isinstance(event, HmrSnapshot):
                    print(f"Replace the HMR baseline with {len(event.periods)} periods.")
                elif isinstance(event, HmrUpdate):
                    print(
                        f"HMR: upsert {len(event.upserted_periods)} periods, "
                        f"remove IDs {event.removed_period_ids} from the latest baseline."
                    )
                elif isinstance(event, InstrumentEvent):
                    print(
                        f"Instrument {event.payload.instrument}: {event.action} "
                        "(full conditions; del removes the cached instrument)."
                    )
                else:
                    print(
                        f"Account observation {event.event_time}: equity="
                        f"{event.payload.account_state.equity}"
                    )
    print("Stream ended: cached state is no longer live.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stream", choices=["account_state", "instruments", "hmr"])
    parser.add_argument("instruments", nargs="*", default=["EURUSD"])
    args = parser.parse_args()
    try:
        asyncio.run(main(args.stream, args.instruments))
    except KeyboardInterrupt:
        pass
    except (ExnessError, KeyError, ValueError) as error:
        raise SystemExit(str(error)) from None
