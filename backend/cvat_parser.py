# cvat_parser.py
import asyncio
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional

from .tickets import set_ticket_state, set_ticket_label_meta, mark_ticket_error


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
