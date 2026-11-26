"""SDK errors. Exception messages never include signed requests or raw responses."""


class ExnessError(Exception):
    """Base class for SDK failures."""


class ConfigurationError(ExnessError, ValueError):
    """Invalid credentials, host, or account configuration."""


class TransportError(ExnessError):
    """A REST request or WebSocket stream failed at the transport layer."""


class ProtocolError(ExnessError):
    """A response violated the expected API contract."""


class UnknownOutcomeError(ExnessError):
    """A write may have reached the server; reconcile before deciding whether to retry.

    Identity and sanitized cause are inspectable attributes, omitted from error text.
    Path excludes query values. This exception does not mean the operation failed.
    """

    def __init__(
        self, *, method: str, path: str, account_id: str, idempotency_key: str, cause: ExnessError
    ) -> None:
        self.method = method
        self.path = path
        self.account_id = account_id
        self.idempotency_key = idempotency_key
        self.cause = cause
        super().__init__("Trading request outcome is unknown; reconcile before retrying")


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


class WebSocketAPIError(ExnessError):
    """Subscription error; error_message is untrusted server text."""

    def __init__(self, request_id: str, code: int, error_message: str) -> None:
        self.request_id = request_id
        self.code = code
        self.error_message = error_message
        super().__init__(f"Exness WebSocket returned code {code}")


class StreamClosed(ExnessError):
    """A terminal close code; the server's arbitrary close reason is not exposed."""

    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"Exness WebSocket closed with code {code}")
