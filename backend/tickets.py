# tickets.py
import asyncio
import shutil
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from .config import UPLOAD_ROOT, UPLOAD_TTL_SECONDS, utc_now
from datetime import datetime

@dataclass
class Ticket:
    """
    Represents the state of a single upload/processing job.

    States used so far:
    - "uploading"
    - "uploaded"
    - "extracting_label_meta"
    - "labels_meta_extracted"
    - "processing_dataset"
    - "ready"
    - "cancelled"
    - "error"
    """
    ticket_id: str
    created_at: datetime
    last_seen: datetime
    state: str  # see states above

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

    # Label metadata (filled after extracting annotations.xml)
    label_meta: Optional[Dict[str, Any]] = None

    # Path to output zip (once processing completes)
    output_zip_path: Optional[Path] = None


# Global in-memory ticket store
TICKETS: Dict[str, Ticket] = {}

# Single lock to guard access to TICKETS and all Ticket modifications
TICKETS_LOCK = asyncio.Lock()


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
            ticket.last_seen = now

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
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        ticket.bytes_received += bytes_added
        ticket.last_seen = now


async def set_ticket_total_bytes(ticket_id: str, total: int) -> None:
    """
    Set the expected total size of the upload (in bytes).
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        ticket.bytes_total = total
        ticket.last_seen = now


async def set_ticket_state(ticket_id: str, state: str) -> None:
    """
    Set the ticket's state and refresh last_seen.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        ticket.state = state
        ticket.last_seen = now


async def mark_ticket_uploaded(ticket_id: str, bytes_total: Optional[int]) -> None:
    """
    Mark a ticket as 'uploaded'.
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


async def set_ticket_label_meta(ticket_id: str, label_meta: Dict[str, Any]) -> None:
    """
    Attach label metadata to a ticket and mark it as 'labels_meta_extracted'.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        ticket.label_meta = label_meta
        ticket.state = "labels_meta_extracted"
        ticket.last_seen = now


async def get_ticket_snapshot(ticket_id: str) -> Optional[Dict[str, Any]]:
    """
    Return a snapshot dictionary of the ticket suitable for JSON serialization.
    """
    now = utc_now()
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return None

        ticket.last_seen = now

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
            "label_meta": ticket.label_meta,
            "feature_params": ticket.feature_params,
            "output_zip_path": str(ticket.output_zip_path) if ticket.output_zip_path else None,
        }

        return snapshot


async def delete_ticket_and_files(ticket_id: str) -> None:
    """
    Remove ticket from memory and delete its folder (and everything inside).
    Intended to be called after download, explicit cancel, or TTL cleanup.
    """
    async with TICKETS_LOCK:
        ticket = TICKETS.pop(ticket_id, None)

    if not ticket:
        return

    ticket_dir: Optional[Path] = None

    if ticket.upload_path:
        ticket_dir = ticket.upload_path.parent
    elif ticket.output_zip_path:
        ticket_dir = ticket.output_zip_path.parent

    # As a fallback, derive from UPLOAD_ROOT
    if ticket_dir is None:
        ticket_dir = UPLOAD_ROOT / ticket_id

    if ticket_dir.exists():
        try:
            shutil.rmtree(ticket_dir, ignore_errors=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Failed to delete ticket directory tree {ticket_dir}: {exc}")


async def cleanup_expired_uploading_tickets() -> None:
    """
    Remove tickets that have not been seen for longer than UPLOAD_TTL_SECONDS.

    Uses delete_ticket_and_files() so both dataset.zip and output.zip get removed.
    """
    now = utc_now()
    cutoff = now - timedelta(seconds=UPLOAD_TTL_SECONDS)

    # First decide *which* tickets to delete while holding the lock
    async with TICKETS_LOCK:
        to_delete_ids = [
            t.ticket_id
            for t in TICKETS.values()
            if t.last_seen < cutoff
        ]

    if not to_delete_ids:
        return

    print(
        "Cleanup: removing %d ticket(s) older than %s"
        % (len(to_delete_ids), cutoff.isoformat())
    )

    # Now actually delete them (includes file cleanup) *outside* the lock
    for tid in to_delete_ids:
        await delete_ticket_and_files(tid)
