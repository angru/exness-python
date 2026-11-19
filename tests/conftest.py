from pathlib import Path

import pytest

from exness import Credentials

# RFC 8032, section 7.1, TEST 1. Public synthetic key, never a live credential.
SEED = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
PUBLIC = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def credentials() -> Credentials:
    return Credentials("exnsk-synthetic-test", SEED + PUBLIC)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / f"{name}.json").read_bytes()
