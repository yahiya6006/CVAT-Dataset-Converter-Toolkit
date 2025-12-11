"""
FastAPI backend for CVAT Dataset Converter Utility.
"""

import argparse
import json
from typing import Any, Dict, Optional

import asyncio
import uvicorn
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    UploadFile,
    BackgroundTasks,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .config import UPLOAD_ROOT, UPLOAD_CHUNK_SIZE, utc_now
from .tickets import (
    TICKETS,
    TICKETS_LOCK,
    delete_ticket_and_files,
    get_or_create_ticket,
    get_ticket_snapshot,
    mark_ticket_uploaded,
    set_ticket_total_bytes,
    update_ticket_bytes,
)
from .cvat_parser import extract_label_meta_for_ticket
from .jobs import process_dataset_for_ticket
from .lifespan import lifespan

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for running the server.

    Examples:
        python app.py
        python app.py --host 0.0.0.0 --port 9000
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
        Receive an uploaded dataset ZIP and save it to disk, tracking progress.
        """
        ticket_id = session_id.strip()
        if not ticket_id:
            raise HTTPException(
                status_code=400,
                detail="session_id (ticket_id) is required",
            )

        try:
            params_obj = json.loads(feature_params) if feature_params else {}
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"feature_params must be valid JSON: {exc}",
            ) from exc

        ticket_dir = UPLOAD_ROOT / ticket_id
        ticket_dir.mkdir(parents=True, exist_ok=True)
        dest_path = ticket_dir / "dataset.zip"

        await get_or_create_ticket(
            ticket_id,
            input_format=input_format,
            target_format=target_format,
            feature_type=feature_type,
            feature_params=params_obj,
            upload_path=dest_path,
        )

        total_size: Optional[int]
        try:
            file.file.seek(0, os.SEEK_END)
            total_size = file.file.tell()
            file.file.seek(0)
        except Exception:  # noqa: BLE001
            total_size = None

        if total_size is not None:
            await set_ticket_total_bytes(ticket_id, total_size)

        bytes_received = 0

        try:
            with dest_path.open("wb") as out_file:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    chunk_size = len(chunk)
                    bytes_received += chunk_size
                    await update_ticket_bytes(ticket_id, chunk_size)

            await mark_ticket_uploaded(ticket_id, bytes_total=bytes_received)

            if input_format == "cvat_images_1_1":
                asyncio.create_task(
                    extract_label_meta_for_ticket(ticket_id, dest_path)
                )

            asyncio.create_task(process_dataset_for_ticket(ticket_id))

        except Exception as exc:  # noqa: BLE001
            from pathlib import Path  # local import if you really need Path here

            await mark_ticket_error(ticket_id, str(exc))
            try:
                if dest_path.exists():
                    dest_path.unlink()
                ticket_dir.rmdir()
            except Exception as cleanup_exc:  # noqa: BLE001
                print(
                    "Failed to clean up after upload error for ticket %s: %s"
                    % (ticket_id, cleanup_exc),
                )
            raise HTTPException(status_code=500, detail=f"Upload failed: {exc}") from exc
        finally:
            await file.close()

        return {
            "status": "ok",
            "ticket_id": ticket_id,
            "state": "uploaded",
            "bytes_received": bytes_received,
            "message": "File uploaded successfully. Extracting label metadata (if supported)…",
        }

    # -------------------------------------------------------------------------
    # Status endpoint
    # -------------------------------------------------------------------------
    @app.get("/status")
    async def get_status(ticket_id: str) -> Dict[str, Any]:
        """
        Return the current status and upload progress for a ticket.
        """
        snapshot = await get_ticket_snapshot(ticket_id)
        if snapshot is None:
            return {
                "ticket_id": ticket_id,
                "state": "unknown",
                "message": "Ticket not found or expired.",
            }
        return snapshot

    # -------------------------------------------------------------------------
    # Cancel endpoint
    # -------------------------------------------------------------------------
    @app.post("/cancel")
    async def cancel_ticket(ticket_id: str) -> Dict[str, Any]:
        """
        Cancel an ongoing job (upload or processing).
        Marks the ticket as 'cancelled' and deletes its files.
        """
        async with TICKETS_LOCK:
            ticket = TICKETS.get(ticket_id)
            if not ticket:
                raise HTTPException(
                    status_code=404,
                    detail="Ticket not found.",
                )
            ticket.state = "cancelled"
            ticket.error_message = "Cancelled by user."
            ticket.last_seen = utc_now()

        # Best-effort cleanup; any running background processing may still error
        # out, but ticket has been removed and files deleted.
        await delete_ticket_and_files(ticket_id)

        return {
            "ticket_id": ticket_id,
            "state": "cancelled",
            "message": "Ticket cancelled and cleaned up.",
        }

    # -------------------------------------------------------------------------
    # Download endpoint
    # -------------------------------------------------------------------------
    @app.get("/download")
    async def download_output(ticket_id: str, background_tasks: BackgroundTasks):
        """
        Download the processed output ZIP for a given ticket.
        After a successful response, the ticket and its files are deleted.
        """
        async with TICKETS_LOCK:
            ticket = TICKETS.get(ticket_id)
            if not ticket or not ticket.output_zip_path:
                raise HTTPException(
                    status_code=404,
                    detail="Output ZIP not ready or ticket not found.",
                )
            out_path = ticket.output_zip_path

        if not out_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Output ZIP file missing on disk.",
            )

        # Schedule cleanup after the response is sent
        background_tasks.add_task(delete_ticket_and_files, ticket_id)

        return FileResponse(
            path=str(out_path),
            media_type="application/zip",
            filename=f"{ticket_id}_output.zip",
        )

    return app


if __name__ == "__main__":
    args = parse_args()
    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port)
