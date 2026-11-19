"""Read market data; optionally set EXN_OPERATION_ID to inspect an existing operation."""

import os
from datetime import UTC, datetime, timedelta

from exness import Credentials, ExnessClient, ExnessError


def main() -> None:
    instrument = os.environ.get("EXN_INSTRUMENT", "EURUSD")
    with ExnessClient(
        credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
        account_id=os.environ["EXN_ACCOUNT_ID"],
        discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
    ) as client:
        print(client.get_instrument_conditions(instrument).model_dump_json(indent=2))
        candles = client.get_candle_history(
            instrument=instrument,
            timeframe="M1",
            from_time=datetime.now(UTC) - timedelta(hours=1),
            count=3,
            price_type="both",
        )
        print(candles.model_dump_json(indent=2))
        if operation_id := os.environ.get("EXN_OPERATION_ID"):
            operation = client.get_operation_status(operation_id)
            print(operation.operation_id, operation.status, operation.is_terminal)


if __name__ == "__main__":
    try:
        main()
    except (ExnessError, ValueError, KeyError) as error:
        raise SystemExit(str(error)) from None
