"""
FastAPI backend for CVAT Dataset Converter Utility.
"""

import argparse
import uvicorn
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import json
from contextlib import asynccontextmanager

# Root directory where uploads are stored.
# Can be overridden by setting the UPLOAD_ROOT environment variable.
UPLOAD_ROOT = Path(os.getenv("CVAT_DCUT_UPLOAD_ROOT", "./cvat_dcut_uploads")).resolve()

# How large each chunk of the upload should be (in bytes).
# 1 MiB is a reasonable default for local usage.
UPLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Time-To-Live for tickets that are stuck in "uploading" state.
# If last_seen is older than this, we drop the ticket and its file.
UPLOAD_TTL_SECONDS = 5 * 60  # 5 minutes

# How often the cleanup task should run (in seconds).
CLEANUP_INTERVAL_SECONDS = 60  # 1 minute

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for running the server.

    Examples:
        python main.py
        python main.py --host 0.0.0.0 --port 9000
    """
    parser = argparse.ArgumentParser(
        description="CVAT Dataset Converter Utility Backend",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host interface to bind (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6007,
        help="Port to listen on (default: 6007)",
    )
    return parser.parse_args()

def utc_now() -> datetime:
    """Return the current UTC time as an aware datetime."""
    return datetime.now(timezone.utc)

# -----------------------------------------------------------------------------
# Ticket model and global store
# -----------------------------------------------------------------------------

@dataclass
class Ticket:
    """
    Represents the state of a single upload/processing job.

    For now, we only handle "uploading" and "uploaded" states (plus "error").
    Later we can extend this with "processing", "finished", etc.
    """
    ticket_id: str
    created_at: datetime
    last_seen: datetime
    state: str  # "uploading" | "uploaded" | "error"

    # Upload metrics
    bytes_received: int = 0
    bytes_total: Optional[int] = None  # we may not know this up-front

    # Where the uploaded ZIP is stored on disk
    upload_path: Optional[Path] = None

    # Metadata from frontend
    input_format: Optional[str] = None
    target_format: Optional[str] = None
    feature_type: Optional[str] = None
    feature_params: Dict[str, Any] = field(default_factory=dict)

    # Error info, if any
    error_message: Optional[str] = None


# Global in-memory ticket store
TICKETS: Dict[str, Ticket] = {}

# Single lock to guard access to TICKETS and all Ticket modifications
TICKETS_LOCK = asyncio.Lock()

async def cleanup_expired_uploading_tickets() -> None:
    """
    Remove tickets that have been in 'uploading' state for longer than UPLOAD_TTL_SECONDS
    without any status calls or progress updates.

    This is a safety net for uploads that get stuck or for clients that disappear
    mid-upload. We also delete the partial upload file if present.
    """
    now = utc_now()
    cutoff = now - timedelta(seconds=UPLOAD_TTL_SECONDS)

    async with TICKETS_LOCK:
        to_delete = [
            t for t in TICKETS.values()
            if t.state == "uploading" and t.last_seen < cutoff
        ]

        if not to_delete:
            return

        print(
            "Cleanup: found %d ticket(s) stuck in 'uploading' older than %s",
            len(to_delete),
            cutoff.isoformat(),
        )

        for ticket in to_delete:
            # Delete the file on disk, if any.
            if ticket.upload_path is not None:
                try:
                    if ticket.upload_path.exists():
                        ticket.upload_path.unlink()
                    # Try to remove the directory if it's empty now.
                    parent = ticket.upload_path.parent
                    parent.rmdir()
                except Exception as exc:  # noqa: BLE001
                    print(
                        "Failed to clean up files for ticket %s: %s",
                        ticket.ticket_id,
                        exc,
                    )

            # Finally remove ticket from memory.
            TICKETS.pop(ticket.ticket_id, None)

async def get_or_create_ticket(
    ticket_id: str,
    *,
    input_format: Optional[str] = None,
    target_format: Optional[str] = None,
    feature_type: Optional[str] = None,
    feature_params: Optional[Dict[str, Any]] = None,
    upload_path: Optional[Path] = None,
) -> Ticket:
    """
    Get an existing ticket or create a new one in 'uploading' state.

    This function must be called before starting the upload write loop.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            ticket = Ticket(
                ticket_id=ticket_id,
                created_at=now,
                last_seen=now,
                state="uploading",
            )
            TICKETS[ticket_id] = ticket
        else:
            # We are reusing an existing ticket: refresh last_seen.
            ticket.last_seen = now

        # Update metadata if provided.
        if input_format is not None:
            ticket.input_format = input_format
        if target_format is not None:
            ticket.target_format = target_format
        if feature_type is not None:
            ticket.feature_type = feature_type
        if feature_params is not None:
            ticket.feature_params = feature_params
        if upload_path is not None:
            ticket.upload_path = upload_path

        return ticket
    
async def update_ticket_bytes(ticket_id: str, bytes_added: int) -> None:
    """
    Increment bytes_received for a ticket and refresh last_seen.

    Called from inside the upload loop for each chunk.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            # If ticket is missing, we silently ignore here. This should be rare.
            return
        ticket.bytes_received += bytes_added
        ticket.last_seen = now


async def mark_ticket_uploaded(ticket_id: str, bytes_total: Optional[int]) -> None:
    """
    Mark a ticket as 'uploaded'. This is called after the entire ZIP
    has been successfully written to disk.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        ticket.state = "uploaded"
        ticket.last_seen = now
        if bytes_total is not None:
            ticket.bytes_total = bytes_total


async def mark_ticket_error(ticket_id: str, message: str) -> None:
    """
    Mark a ticket as 'error' with an error message.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        ticket.state = "error"
        ticket.error_message = message
        ticket.last_seen = now


async def get_ticket_snapshot(ticket_id: str) -> Optional[Dict[str, Any]]:
    """
    Return a snapshot dictionary of the ticket suitable for JSON serialization.

    This function also refreshes last_seen to implement TTL reset on /status calls.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return None

        # Reset TTL: we saw activity for this ticket.
        ticket.last_seen = now

        # Compute upload progress if we know total size.
        if ticket.bytes_total and ticket.bytes_total > 0:
            progress = ticket.bytes_received / ticket.bytes_total
        else:
            progress = None

        snapshot = {
            "ticket_id": ticket.ticket_id,
            "state": ticket.state,
            "created_at": ticket.created_at.isoformat(),
            "last_seen": ticket.last_seen.isoformat(),
            "upload": {
                "bytes_received": ticket.bytes_received,
                "bytes_total": ticket.bytes_total,
                "progress": progress,
            },
            "input_format": ticket.input_format,
            "target_format": ticket.target_format,
            "feature_type": ticket.feature_type,
            "error_message": ticket.error_message,
        }

        return snapshot

async def cleanup_loop() -> None:
    while True:
        try:
            await cleanup_expired_uploading_tickets()
        except Exception as exc:  # noqa: BLE001
            print("Error during cleanup: %s", exc)
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context:
    - Runs once at startup before serving requests.
    - Yields while the app is running.
    - After yield, runs shutdown logic.
    """

    # === Startup logic (what you had in @app.on_event("startup")) ===
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    cleanup_task = asyncio.create_task(cleanup_loop())

    # Hand control back to FastAPI (app is now "running")
    try:
        yield
    finally:
        # === Shutdown logic ===
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass


def create_app() -> FastAPI:
    app = FastAPI(title="CVAT Dataset Converter Utility Backend.", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------------------------------------------------------
    # Upload endpoint
    # -------------------------------------------------------------------------
    @app.post("/upload")
    async def upload_dataset(
        # Order of parameters doesn't matter to FastAPI, but we group logically.
        session_id: str = Form(..., description="Ticket ID for this upload/job"),
        input_format: str = Form(..., description="Input annotation format"),
        target_format: str = Form(
            "",
            description="Target annotation format (may be empty for crop-only feature)",
        ),
        feature_type: str = Form(..., description="Feature type selected in UI"),
        feature_params: str = Form(
            "{}",
            description="JSON string of feature-specific options",
        ),
        file: UploadFile = File(..., description="ZIP file containing dataset"),
    ) -> Dict[str, Any]:
        """
        Receive an uploaded dataset ZIP and save it to disk, tracking progress
        in the global ticket store.

        The ticket ID is supplied by the frontend as 'session_id' and is treated
        as a "ticket" identifier.
        """
        ticket_id = session_id.strip()
        if not ticket_id:
            raise HTTPException(
                status_code=400,
                detail="session_id (ticket_id) is required",
            )

        # Parse feature_params JSON
        try:
            params_obj = json.loads(feature_params) if feature_params else {}
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"feature_params must be valid JSON: {exc}",
            ) from exc

        # Prepare directory and destination path for this ticket
        ticket_dir = UPLOAD_ROOT / ticket_id
        ticket_dir.mkdir(parents=True, exist_ok=True)
        dest_path = ticket_dir / "dataset.zip"

        # Initialize or update ticket metadata
        await get_or_create_ticket(
            ticket_id,
            input_format=input_format,
            target_format=target_format,
            feature_type=feature_type,
            feature_params=params_obj,
            upload_path=dest_path,
        )

        bytes_received = 0

        try:
            # Open destination file and stream upload in chunks
            with dest_path.open("wb") as out_file:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    chunk_size = len(chunk)
                    bytes_received += chunk_size
                    # Update ticket bytes + last_seen under lock
                    await update_ticket_bytes(ticket_id, chunk_size)

            # After full upload, mark as uploaded
            await mark_ticket_uploaded(ticket_id, bytes_total=bytes_received)

        except Exception as exc:  # noqa: BLE001
            # Mark ticket as error and try cleaning up partial file
            await mark_ticket_error(ticket_id, str(exc))
            try:
                if dest_path.exists():
                    dest_path.unlink()
                # Attempt to remove directory if now empty
                ticket_dir.rmdir()
            except Exception as cleanup_exc:  # noqa: BLE001
                print(
                    "Failed to clean up after upload error for ticket %s: %s",
                    ticket_id,
                    cleanup_exc,
                )
            raise HTTPException(status_code=500, detail=f"Upload failed: {exc}") from exc
        finally:
            # Make sure UploadFile is closed
            await file.close()

        # Respond quickly; further processing (unzip, parse XML, etc.)
        # will be implemented in later stages.
        return {
            "status": "ok",
            "ticket_id": ticket_id,
            "state": "uploaded",
            "bytes_received": bytes_received,
            "message": "File uploaded successfully. Further processing is pending.",
        }
    
    # -------------------------------------------------------------------------
    # Status endpoint
    # -------------------------------------------------------------------------
    @app.get("/status")
    async def get_status(ticket_id: str) -> Dict[str, Any]:
        """
        Return the current status and upload progress for a ticket.

        If the ticket is missing (not created or expired/cleaned-up), we return
        'state': 'unknown'.
        """
        snapshot = await get_ticket_snapshot(ticket_id)
        if snapshot is None:
            return {
                "ticket_id": ticket_id,
                "state": "unknown",
                "message": "Ticket not found or expired.",
            }
        return snapshot

    return app

if __name__ == "__main__":
    args = parse_args()
    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port)