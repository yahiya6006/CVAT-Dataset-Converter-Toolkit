# config.py
import os
from pathlib import Path
from datetime import datetime, timezone

# Root directory where uploads are stored.
# Can be overridden by setting the CVAT_DCUT_UPLOAD_ROOT environment variable.
UPLOAD_ROOT = Path(os.getenv("CVAT_DCUT_UPLOAD_ROOT", "./cvat_dcut_uploads")).resolve()

# How large each chunk of the upload should be (in bytes).
UPLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Time-To-Live for tickets (seconds).
UPLOAD_TTL_SECONDS = 5 * 60  # 5 minutes

# Cleanup sleep interval (seconds).
CLEANUP_INTERVAL_SECONDS = 60  # 1 minute

# Name of the output zip for each ticket
OUTPUT_ZIP_NAME = "output.zip"


def utc_now() -> datetime:
    """Return the current UTC time as an aware datetime."""
    return datetime.now(timezone.utc)
