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
    BackgroundTasks,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import asyncio
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
import json
from contextlib import asynccontextmanager
import zipfile
import xml.etree.ElementTree as ET
import io
import math
from PIL import Image
import shutil

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


async def cleanup_loop() -> None:
    while True:
        try:
            await cleanup_expired_uploading_tickets()
        except Exception as exc:  # noqa: BLE001
            print("Error during cleanup: %s" % exc)
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context.
    """
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    cleanup_task = asyncio.create_task(cleanup_loop())

    try:
        yield
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass


# -----------------------------------------------------------------------------
# CVAT parsing (per-image dict + summary)
# -----------------------------------------------------------------------------
def parse_cvat_annotations_from_zip(zip_path: Path) -> Dict[str, Any]:
    """
    Parse CVAT 1.1 annotations from dataset.zip into a per-image dict
    plus a compact summary meta.
    """
    if not zip_path.exists():
        raise RuntimeError(f"ZIP file not found at {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        try:
            with zf.open("annotations.xml") as f:
                tree = ET.parse(f)
        except KeyError as exc:
            raise RuntimeError("annotations.xml not found in ZIP") from exc

    root = tree.getroot()

    orig_width = None
    orig_height = None
    orig_size_node = root.find("./meta/original_size")
    if orig_size_node is not None:
        w_text = orig_size_node.findtext("width")
        h_text = orig_size_node.findtext("height")
        orig_width = int(w_text) if w_text is not None else None
        orig_height = int(h_text) if h_text is not None else None

    images: Dict[str, Dict[str, Any]] = {}
    label_counts: Dict[str, int] = {}

    for image_elem in root.findall("image"):
        name = image_elem.get("name")
        if not name:
            continue

        width_text = image_elem.get("width")
        height_text = image_elem.get("height")
        width = int(width_text) if width_text is not None else None
        height = int(height_text) if height_text is not None else None

        boxes: List[Dict[str, Any]] = []
        for box_elem in image_elem.findall("box"):
            label = box_elem.get("label")
            if not label:
                continue

            xtl = float(box_elem.get("xtl", "0"))
            ytl = float(box_elem.get("ytl", "0"))
            xbr = float(box_elem.get("xbr", "0"))
            ybr = float(box_elem.get("ybr", "0"))

            boxes.append(
                {
                    "label": label,
                    "xtl": xtl,
                    "ytl": ytl,
                    "xbr": xbr,
                    "ybr": ybr,
                }
            )

            label_counts[label] = label_counts.get(label, 0) + 1

        images[name] = {
            "width": width,
            "height": height,
            "boxes": boxes,
        }

    label_names = sorted(label_counts.keys())
    label_to_id = {name: idx for idx, name in enumerate(label_names)}

    labels_summary = [
        {"name": name, "count": label_counts[name]}
        for name in label_names
    ]

    image_count = len(images)
    box_count = sum(len(info["boxes"]) for info in images.values())

    meta = {
        "image_count": image_count,
        "box_count": box_count,
        "labels": labels_summary,
        "original_width": orig_width,
        "original_height": orig_height,
    }

    return {
        "images": images,
        "label_names": label_names,
        "label_to_id": label_to_id,
        "meta": meta,
    }


async def extract_label_meta_for_ticket(ticket_id: str, zip_path: Path) -> None:
    """
    Background task to extract label metadata for CVAT images 1.1 and
    attach it to the ticket.
    """
    try:
        await set_ticket_state(ticket_id, "extracting_label_meta")

        loop = asyncio.get_running_loop()
        parsed = await loop.run_in_executor(
            None, parse_cvat_annotations_from_zip, zip_path
        )

        label_meta = parsed.get("meta", {})
        await set_ticket_label_meta(ticket_id, label_meta)
    except Exception as exc:  # noqa: BLE001
        await mark_ticket_error(ticket_id, f"Label meta extraction failed: {exc}")


# -----------------------------------------------------------------------------
# Helper functions for label formats & image ops
# -----------------------------------------------------------------------------
def _guess_pil_format_from_extension(ext: str) -> str:
    ext = ext.lower()
    if ext in [".jpg", ".jpeg"]:
        return "JPEG"
    if ext == ".png":
        return "PNG"
    if ext == ".bmp":
        return "BMP"
    if ext in [".tif", ".tiff"]:
        return "TIFF"
    return "PNG"

def build_label_info_json(
    label_format: str,
    parsed: Dict[str, Any],
    feature_type: Optional[str] = None,
) -> str:
    """
    Build a JSON summary with:
    - label_format: "yolo" | "pascal_voc" | "tao_kitti" | "none"
    - feature_type: "convert_only" | "resize_and_convert" | "crop_objects" | None
    - image_count, box_count, original_width, original_height
    - labels: [{id, name, count}, ...] in a stable order
    """
    meta = parsed.get("meta") or {}
    labels_meta = meta.get("labels") or []

    # name -> count from meta
    count_by_name = {}
    for item in labels_meta:
        name = item.get("name")
        if not name:
            continue
        count_by_name[name] = item.get("count")

    label_names: List[str] = parsed.get("label_names") or list(count_by_name.keys())
    label_to_id: Dict[str, int] = parsed.get("label_to_id") or {}

    labels_payload: List[Dict[str, Any]] = []
    for name in label_names:
        labels_payload.append(
            {
                "id": label_to_id.get(name),   # 0,1,2,... for YOLO; may be None for others
                "name": name,
                "count": count_by_name.get(name),
            }
        )

    info: Dict[str, Any] = {
        "label_format": label_format,              # "yolo" | "pascal_voc" | "tao_kitti" | "none"
        "feature_type": feature_type,              # "convert_only" | "resize_and_convert" | "crop_objects" | None
        "image_count": meta.get("image_count"),
        "box_count": meta.get("box_count"),
        "original_width": meta.get("original_width"),
        "original_height": meta.get("original_height"),
        "num_classes": len(label_names),
        "labels": labels_payload,
    }

    return json.dumps(info, indent=2)

def build_yolo_label_file(
    boxes: List[Dict[str, Any]],
    img_w: int,
    img_h: int,
    label_to_id: Dict[str, int],
) -> str:
    """Return YOLOv8-style label file content for one image."""
    if not img_w or not img_h:
        return ""

    lines: List[str] = []
    for b in boxes:
        label = b["label"]
        if label not in label_to_id:
            continue
        class_id = label_to_id[label]

        xtl = b["xtl"]
        ytl = b["ytl"]
        xbr = b["xbr"]
        ybr = b["ybr"]

        x_center = (xtl + xbr) / 2.0 / img_w
        y_center = (ytl + ybr) / 2.0 / img_h
        bw = (xbr - xtl) / img_w
        bh = (ybr - ytl) / img_h

        x_center = max(0.0, min(1.0, x_center))
        y_center = max(0.0, min(1.0, y_center))
        bw = max(0.0, min(1.0, bw))
        bh = max(0.0, min(1.0, bh))

        lines.append(
            f"{class_id} {x_center:.6f} {y_center:.6f} {bw:.6f} {bh:.6f}"
        )

    return "\n".join(lines) + ("\n" if lines else "")


def build_pascal_voc_label_file(
    image_name: str,
    boxes: List[Dict[str, Any]],
    img_w: int,
    img_h: int,
    depth: int = 3,
) -> str:
    """Return Pascal VOC XML annotation for one image."""
    annotation = ET.Element("annotation")

    folder_el = ET.SubElement(annotation, "folder")
    folder_el.text = "images"

    filename_el = ET.SubElement(annotation, "filename")
    filename_el.text = image_name

    size_el = ET.SubElement(annotation, "size")
    w_el = ET.SubElement(size_el, "width")
    w_el.text = str(img_w)
    h_el = ET.SubElement(size_el, "height")
    h_el.text = str(img_h)
    d_el = ET.SubElement(size_el, "depth")
    d_el.text = str(depth)

    segmented_el = ET.SubElement(annotation, "segmented")
    segmented_el.text = "0"

    for b in boxes:
        obj_el = ET.SubElement(annotation, "object")

        name_el = ET.SubElement(obj_el, "name")
        name_el.text = b["label"]

        pose_el = ET.SubElement(obj_el, "pose")
        pose_el.text = "Unspecified"

        truncated_el = ET.SubElement(obj_el, "truncated")
        truncated_el.text = "0"

        difficult_el = ET.SubElement(obj_el, "difficult")
        difficult_el.text = "0"

        bbox_el = ET.SubElement(obj_el, "bndbox")
        xmin_el = ET.SubElement(bbox_el, "xmin")
        xmin_el.text = str(int(round(b["xtl"])))
        ymin_el = ET.SubElement(bbox_el, "ymin")
        ymin_el.text = str(int(round(b["ytl"])))
        xmax_el = ET.SubElement(bbox_el, "xmax")
        xmax_el.text = str(int(round(b["xbr"])))
        ymax_el = ET.SubElement(bbox_el, "ymax")
        ymax_el.text = str(int(round(b["ybr"])))

    return ET.tostring(annotation, encoding="unicode")


def build_tao_kitti_label_file(
    boxes: List[Dict[str, Any]],
    img_w: int,
    img_h: int,
) -> str:
    """
    Minimal TAO/KITTI-like 2D detection file.
    """
    lines: List[str] = []
    for b in boxes:
        xmin = int(round(b["xtl"]))
        ymin = int(round(b["ytl"]))
        xmax = int(round(b["xbr"]))
        ymax = int(round(b["ybr"]))
        label = b["label"]
        lines.append(
            f"{label} 0 0 0 {xmin} {ymin} {xmax} {ymax} 0 0 0 0 0 0 0"
        )
    return "\n".join(lines) + ("\n" if lines else "")


# -----------------------------------------------------------------------------
# ZIP image lookup helpers
# -----------------------------------------------------------------------------
def _build_zip_index(zin: zipfile.ZipFile) -> Dict[str, str]:
    """
    Build a mapping from base filename (no dir, no extension, lowercased)
    to the corresponding ZIP member name.
    """
    index: Dict[str, str] = {}
    for member in zin.namelist():
        if member.endswith("/"):
            continue
        base = os.path.splitext(os.path.basename(member))[0].casefold()
        existing = index.get(base)
        if existing is None:
            index[base] = member
        else:
            if member.lower().startswith("images/") and not existing.lower().startswith("images/"):
                index[base] = member
    return index


def _resolve_image_member(zip_index: Dict[str, str], annotated_name: str) -> Optional[str]:
    """
    Given the 'name' from annotations.xml, find a matching image member in the ZIP.
    """
    base = os.path.splitext(os.path.basename(annotated_name))[0].casefold()
    return zip_index.get(base)


# -----------------------------------------------------------------------------
# Feature-type job functions
# -----------------------------------------------------------------------------
def convert_labels_only_job(
    in_zip_path: Path,
    parsed: Dict[str, Any],
    out_zip_path: Path,
    target_format: str,
    output_prefix: str,
    include_images: bool,
) -> None:
    images = parsed["images"]
    label_to_id = parsed["label_to_id"]
    output_prefix = (output_prefix or "").strip()

    with zipfile.ZipFile(in_zip_path, "r") as zin, zipfile.ZipFile(
        out_zip_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        zip_index = _build_zip_index(zin)

        for name, info in images.items():
            img_w = info["width"]
            img_h = info["height"]
            boxes = info["boxes"]

            member = _resolve_image_member(zip_index, name)
            if member is None:
                print(f"[WARN] convert_labels_only: no image found in ZIP for annotation '{name}', skipping.")
                continue

            orig_basename = os.path.basename(member)
            base_name, ext = os.path.splitext(orig_basename)

            if output_prefix:
                out_base = f"{output_prefix}_{base_name}"
            else:
                out_base = base_name

            if target_format == "yolo":
                label_text = build_yolo_label_file(
                    boxes, img_w, img_h, label_to_id
                )
                label_filename = f"{out_base}.txt"
            elif target_format == "pascal_voc":
                label_text = build_pascal_voc_label_file(
                    f"{out_base}{ext}", boxes, img_w, img_h
                )
                label_filename = f"{out_base}.xml"
            elif target_format == "tao_kitti":
                label_text = build_tao_kitti_label_file(
                    boxes, img_w, img_h
                )
                label_filename = f"{out_base}.txt"
            else:
                raise RuntimeError(f"Unsupported target format: {target_format!r}")

            zout.writestr(f"labels/{label_filename}", label_text)

            if include_images:
                data = zin.read(member)
                out_image_name = f"{out_base}{ext}"
                zout.writestr(f"images/{out_image_name}", data)

        # After processing all images, add a label_info.json at the root of the ZIP
        label_info_json = build_label_info_json(
            label_format=target_format,
            parsed=parsed,
            feature_type="convert_only",
        )
        zout.writestr("label_info.json", label_info_json)

def resize_and_convert_job(
    in_zip_path: Path,
    parsed: Dict[str, Any],
    out_zip_path: Path,
    target_format: str,
    output_prefix: str,
    target_width: int,
    target_height: int,
    preserve_aspect_ratio: bool,
) -> None:
    if not target_width or not target_height:
        raise RuntimeError("Target width and height are required for resize_and_convert.")

    images = parsed["images"]
    label_to_id = parsed["label_to_id"]
    output_prefix = (output_prefix or "").strip()

    with zipfile.ZipFile(in_zip_path, "r") as zin, zipfile.ZipFile(
        out_zip_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        zip_index = _build_zip_index(zin)

        for name, info in images.items():
            img_w = info["width"]
            img_h = info["height"]
            boxes = info["boxes"]

            member = _resolve_image_member(zip_index, name)
            if member is None:
                print(f"[WARN] resize_and_convert: no image found in ZIP for annotation '{name}', skipping.")
                continue

            orig_basename = os.path.basename(member)
            base_name, ext = os.path.splitext(orig_basename)

            if output_prefix:
                out_base = f"{output_prefix}_{base_name}"
            else:
                out_base = base_name

            with zin.open(member) as img_file:
                img = Image.open(img_file).convert("RGB")

                if preserve_aspect_ratio and img_w and img_h:
                    scale = min(target_width / img_w, target_height / img_h)
                    new_w = int(round(img_w * scale))
                    new_h = int(round(img_h * scale))

                    resized = img.resize((new_w, new_h), Image.BILINEAR)

                    canvas = Image.new("RGB", (target_width, target_height))
                    offset_x = (target_width - new_w) // 2
                    offset_y = (target_height - new_h) // 2
                    canvas.paste(resized, (offset_x, offset_y))
                    final_img = canvas

                    new_boxes: List[Dict[str, Any]] = []
                    for b in boxes:
                        xtl = b["xtl"] * scale + offset_x
                        ytl = b["ytl"] * scale + offset_y
                        xbr = b["xbr"] * scale + offset_x
                        ybr = b["ybr"] * scale + offset_y
                        new_boxes.append(
                            {
                                "label": b["label"],
                                "xtl": xtl,
                                "ytl": ytl,
                                "xbr": xbr,
                                "ybr": ybr,
                            }
                        )

                    out_img_w = target_width
                    out_img_h = target_height
                else:
                    resized = img.resize(
                        (target_width, target_height), Image.BILINEAR
                    )
                    final_img = resized

                    scale_x = target_width / img_w if img_w else 1.0
                    scale_y = target_height / img_h if img_h else 1.0

                    new_boxes: List[Dict[str, Any]] = []
                    for b in boxes:
                        xtl = b["xtl"] * scale_x
                        ytl = b["ytl"] * scale_y
                        xbr = b["xbr"] * scale_x
                        ybr = b["ybr"] * scale_y
                        new_boxes.append(
                            {
                                "label": b["label"],
                                "xtl": xtl,
                                "ytl": ytl,
                                "xbr": xbr,
                                "ybr": ybr,
                            }
                        )

                    out_img_w = target_width
                    out_img_h = target_height

                buf = io.BytesIO()
                fmt = _guess_pil_format_from_extension(ext)
                final_img.save(buf, format=fmt)
                zout.writestr(f"images/{out_base}{ext}", buf.getvalue())

                if target_format == "yolo":
                    label_text = build_yolo_label_file(
                        new_boxes, out_img_w, out_img_h, label_to_id
                    )
                    label_filename = f"{out_base}.txt"
                elif target_format == "pascal_voc":
                    label_text = build_pascal_voc_label_file(
                        f"{out_base}{ext}", new_boxes, out_img_w, out_img_h
                    )
                    label_filename = f"{out_base}.xml"
                elif target_format == "tao_kitti":
                    label_text = build_tao_kitti_label_file(
                        new_boxes, out_img_w, out_img_h
                    )
                    label_filename = f"{out_base}.txt"
                else:
                    raise RuntimeError(
                        f"Unsupported target format for resize_and_convert: {target_format!r}"
                    )

                zout.writestr(f"labels/{label_filename}", label_text)
        
        label_info_json = build_label_info_json(
            label_format=target_format,
            parsed=parsed,
            feature_type="resize_and_convert",
        )
        zout.writestr("label_info.json", label_info_json)

def crop_objects_job(
    in_zip_path: Path,
    parsed: Dict[str, Any],
    out_zip_path: Path,
    output_prefix: str,
    padding: int,
    per_class_folders: bool,
) -> None:
    images = parsed["images"]
    output_prefix = (output_prefix or "").strip()
    padding = max(0, int(padding or 0))

    with zipfile.ZipFile(in_zip_path, "r") as zin, zipfile.ZipFile(
        out_zip_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        zip_index = _build_zip_index(zin)

        for name, info in images.items():
            img_w = info["width"]
            img_h = info["height"]
            boxes = info["boxes"]

            member = _resolve_image_member(zip_index, name)
            if member is None:
                print(f"[WARN] crop_objects: no image found in ZIP for annotation '{name}', skipping.")
                continue

            orig_basename = os.path.basename(member)
            base_name, ext = os.path.splitext(orig_basename)

            with zin.open(member) as img_file:
                img = Image.open(img_file).convert("RGB")

                for idx, b in enumerate(boxes):
                    xtl = b["xtl"]
                    ytl = b["ytl"]
                    xbr = b["xbr"]
                    ybr = b["ybr"]

                    x1 = max(0, int(math.floor(xtl - padding)))
                    y1 = max(0, int(math.floor(ytl - padding)))
                    if img_w:
                        x2 = min(img_w, int(math.ceil(xbr + padding)))
                    else:
                        x2 = int(math.ceil(xbr + padding))
                    if img_h:
                        y2 = min(img_h, int(math.ceil(ybr + padding)))
                    else:
                        y2 = int(math.ceil(ybr + padding))

                    crop = img.crop((x1, y1, x2, y2))

                    if output_prefix:
                        out_base = f"{output_prefix}_{base_name}_{idx:04d}"
                    else:
                        out_base = f"{base_name}_{idx:04d}"

                    if per_class_folders:
                        subdir = f"images/{b['label']}"
                    else:
                        subdir = "images"

                    buf = io.BytesIO()
                    fmt = _guess_pil_format_from_extension(ext)
                    crop.save(buf, format=fmt)
                    zout.writestr(f"{subdir}/{out_base}{ext}", buf.getvalue())

        zout.writestr(
            "labels/README.txt",
            "No labels are generated in crop_objects mode.\n",
        )


# -----------------------------------------------------------------------------
# Ticket + file cleanup helper
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Background dataset processing
# -----------------------------------------------------------------------------
async def process_dataset_for_ticket(ticket_id: str) -> None:
    """
    Background job that reads dataset.zip for a ticket and produces
    output.zip/images + output.zip/labels according to feature_type
    and feature_params.
    """
    async with TICKETS_LOCK:
        ticket = TICKETS.get(ticket_id)
        if ticket is None:
            return
        zip_path = ticket.upload_path
        feature_type = ticket.feature_type
        target_format = (ticket.target_format or "").strip()
        feature_params = ticket.feature_params or {}

    if not zip_path or not zip_path.exists():
        await mark_ticket_error(ticket_id, "Uploaded dataset.zip not found on disk.")
        return

    if feature_type not in {"convert_only", "resize_and_convert", "crop_objects"}:
        await mark_ticket_error(ticket_id, f"Unsupported feature_type: {feature_type!r}")
        return

    try:
        await set_ticket_state(ticket_id, "processing_dataset")

        output_prefix = str(feature_params.get("output_prefix", "") or "").strip()
        out_zip_path = zip_path.parent / OUTPUT_ZIP_NAME

        loop = asyncio.get_running_loop()

        parsed = await loop.run_in_executor(
            None, parse_cvat_annotations_from_zip, zip_path
        )

        if feature_type == "convert_only":
            include_images = bool(feature_params.get("include_images", True))
            if not target_format:
                raise RuntimeError("Target format is required for 'Convert labels only'.")
            await loop.run_in_executor(
                None,
                convert_labels_only_job,
                zip_path,
                parsed,
                out_zip_path,
                target_format,
                output_prefix,
                include_images,
            )

        elif feature_type == "resize_and_convert":
            if not target_format:
                raise RuntimeError("Target format is required for 'Resize + convert'.")
            width = feature_params.get("width")
            height = feature_params.get("height")
            preserve_aspect = bool(feature_params.get("preserve_aspect_ratio", True))
            await loop.run_in_executor(
                None,
                resize_and_convert_job,
                zip_path,
                parsed,
                out_zip_path,
                target_format,
                output_prefix,
                int(width) if width else None,
                int(height) if height else None,
                preserve_aspect,
            )

        elif feature_type == "crop_objects":
            padding = int(feature_params.get("padding", 0) or 0)
            per_class = bool(feature_params.get("per_class_folders", True))
            await loop.run_in_executor(
                None,
                crop_objects_job,
                zip_path,
                parsed,
                out_zip_path,
                output_prefix,
                padding,
                per_class,
            )

        async with TICKETS_LOCK:
            ticket = TICKETS.get(ticket_id)
            if ticket is not None:
                ticket.output_zip_path = out_zip_path
                ticket.state = "ready"
                ticket.last_seen = utc_now()

    except Exception as exc:  # noqa: BLE001
        await mark_ticket_error(ticket_id, f"Dataset processing failed: {exc}")


# -----------------------------------------------------------------------------
# FastAPI app factory
# -----------------------------------------------------------------------------
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
