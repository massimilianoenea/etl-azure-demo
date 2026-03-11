class ConfigurationError(Exception):
    """Raised when a required environment variable is missing."""
    pass


class BlobUrlParseError(ValueError):
    """Raised when a blob URL cannot be parsed."""
    pass
