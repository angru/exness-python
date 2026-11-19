"""Run with uv run python examples/read_account.py after configuring EXN_* locally."""

import os

from exness import Credentials, ExnessClient, ExnessError


def main() -> None:
    with ExnessClient(
        credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
        account_id=os.environ["EXN_ACCOUNT_ID"],
        discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
    ) as client:
        print(client.get_account().model_dump_json(indent=2))
        print(client.get_snapshot().model_dump_json(indent=2))
        print(client.get_instruments().model_dump_json(indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ExnessError, KeyError) as error:
        raise SystemExit(str(error)) from None
