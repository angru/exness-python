"""SDK errors. Exception messages never include signed requests or raw responses."""


class ExnessError(Exception):
    """Base class for SDK failures."""


class ConfigurationError(ExnessError, ValueError):
    """Invalid credentials, host, or account configuration."""


class TransportError(ExnessError):
    """A request failed before an HTTP response was received."""


class ProtocolError(ExnessError):
    """A response violated the expected API contract."""


class APIError(ExnessError):
    """HTTP failure, including optional structured public API error fields."""

    def __init__(
        self,
        status_code: int,
        *,
        code: int | None = None,
        error_message: str | None = None,
        retry_after: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.error_message = error_message
        self.retry_after = retry_after
        # Do not echo untrusted server text, which could contain request credentials.
        super().__init__(f"Exness API returned HTTP {status_code} (code={code})")
