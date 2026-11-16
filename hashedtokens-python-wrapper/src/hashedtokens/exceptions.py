class HashedTokensError(RuntimeError):
    """Base error for the HashedTokens wrapper."""


class HTTPClientError(HashedTokensError):
    """Raised when the external HTTP client cannot complete a request."""


class EDMSRequestError(HashedTokensError):
    """Raised when the EDMS API rejects or cannot process a request."""


class WebSocketError(HashedTokensError):
    """Raised when the EDMS WebSocket lifecycle fails."""


class TimeoutExceeded(HashedTokensError):
    """Raised when the wrapper deadline is exceeded."""


class EDMSValidationError(EDMSRequestError):
    """Raised for invalid EDMS payload validation."""
