# jobs.py
import asyncio
import io
import json
import math
import os
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from PIL import Image

from .config import OUTPUT_ZIP_NAME, utc_now
from .cvat_parser import parse_cvat_annotations_from_zip
from .tickets import (
    TICKETS,
    TICKETS_LOCK,
    set_ticket_state,
    mark_ticket_error,
)
import xml.etree.ElementTree as ET

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
        "label_format": label_format,
        "feature_type": feature_type,
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
