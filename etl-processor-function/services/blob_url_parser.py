from services.exceptions import BlobUrlParseError


def parse_blob_url(blob_url: str) -> tuple[str, str]:
    """Parse an Azure Blob URL into (container_name, blob_name).

    Expected format: https://<account>.blob.core.windows.net/<container>/<blob>
    """
    if not blob_url:
        raise BlobUrlParseError("Blob URL is empty")

    parts = blob_url.split(".blob.core.windows.net/", 1)
    if len(parts) < 2:
        raise BlobUrlParseError(f"Cannot parse blob URL: {blob_url}")

    path = parts[1]
    segments = path.split("/", 1)
    if len(segments) < 2:
        raise BlobUrlParseError(f"Blob URL missing blob name: {blob_url}")

    container_name = segments[0]
    blob_name = segments[1]
    return container_name, blob_name
