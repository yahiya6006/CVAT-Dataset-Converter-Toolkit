// api.js
// ---------
// Contains thin wrappers around HTTP calls to the backend.
//
// For Stage 2, we focus on:
// - POST /upload  (uploadDataset)
// - GET  /status  (fetchUploadStatus)

import { API_BASE_URL } from "./config.js";

/**
 * Uploads the selected dataset ZIP and metadata to the backend.
 *
 * @param {Object} params
 * @param {File} params.file - The ZIP file selected by the user.
 * @param {string} params.inputFormat - Selected input format (e.g. "cvat_images_1_1").
 * @param {string} params.targetFormat - Selected target format (e.g. "yolo", or "" for crop mode).
 * @param {string} params.featureType - Selected feature type ("convert_only", "resize_and_convert", "crop_objects").
 * @param {Object} params.featureParams - Additional options depending on featureType.
 * @param {string} params.sessionId - Ticket ID for this upload/job.
 *
 * @returns {Promise<Object>} Parsed JSON response from backend.
 */
export async function uploadDataset({
  file,
  inputFormat,
  targetFormat,
  featureType,
  featureParams,
  sessionId,
}) {
  if (!file) {
    throw new Error("No file selected.");
  }

  const formData = new FormData();
  formData.append("file", file);
  formData.append("session_id", sessionId);
  formData.append("input_format", inputFormat);
  formData.append("target_format", targetFormat);
  formData.append("feature_type", featureType);
  formData.append("feature_params", JSON.stringify(featureParams || {}));

  const endpoint = `${API_BASE_URL}/upload`;

  const response = await fetch(endpoint, {
    method: "POST",
    body: formData,
  });

  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  if (!response.ok) {
    let errorMessage = `Upload failed with status ${response.status}`;
    if (isJson) {
      try {
        const errorJson = await response.json();
        if (errorJson && errorJson.detail) {
          errorMessage = Array.isArray(errorJson.detail)
            ? errorJson.detail.map((d) => d.msg).join("; ")
            : String(errorJson.detail);
        }
      } catch {
        // ignore JSON parsing error, keep generic message
      }
    }
    throw new Error(errorMessage);
  }

  if (!isJson) {
    const text = await response.text();
    return { raw: text };
  }

  return response.json();
}

/**
 * Fetches the current status for a ticket from the backend.
 *
 * Expected backend endpoint: GET /status?ticket_id=<ticket>
 *
 * @param {string} ticketId
 * @returns {Promise<Object>} Parsed JSON response with status info.
 */
export async function fetchUploadStatus(ticketId) {
  if (!ticketId) {
    throw new Error("ticketId is required for status polling.");
  }

  const url = `${API_BASE_URL}/status?ticket_id=${encodeURIComponent(
    ticketId
  )}`;

  const response = await fetch(url, { method: "GET" });

  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  if (!response.ok) {
    let errorMessage = `Status check failed with status ${response.status}`;
    if (isJson) {
      try {
        const errorJson = await response.json();
        if (errorJson && errorJson.detail) {
          errorMessage = Array.isArray(errorJson.detail)
            ? errorJson.detail.map((d) => d.msg).join("; ")
            : String(errorJson.detail);
        }
      } catch {
        // ignore JSON parsing errors
      }
    }
    throw new Error(errorMessage);
  }

  if (!isJson) {
    const text = await response.text();
    return { raw: text };
  }

  return response.json();
}

/**
 * Cancel an ongoing ticket on the backend.
 *
 * @param {string} ticketId
 * @returns {Promise<Object>}
 */
export async function cancelTicket(ticketId) {
  if (!ticketId) {
    throw new Error("ticketId is required to cancel a ticket.");
  }

  const url = `${API_BASE_URL}/cancel`;
  const body = new URLSearchParams({ ticket_id: ticketId });

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });

  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  if (!response.ok) {
    let errorMessage = `Cancel failed with status ${response.status}`;
    if (isJson) {
      try {
        const errorJson = await response.json();
        if (errorJson && errorJson.detail) {
          errorMessage = Array.isArray(errorJson.detail)
            ? errorJson.detail.map((d) => d.msg).join("; ")
            : String(errorJson.detail);
        }
      } catch {
        // ignore
      }
    }
    throw new Error(errorMessage);
  }

  return isJson ? response.json() : {};
}

/**
 * Download the output ZIP for a given ticket.
 *
 * @param {string} ticketId
 * @returns {Promise<{blob: Blob, filename: string}>}
 */
export async function downloadOutput(ticketId) {
  if (!ticketId) {
    throw new Error("ticketId is required to download output.");
  }

  const url = `${API_BASE_URL}/download?ticket_id=${encodeURIComponent(
    ticketId
  )}`;

  const response = await fetch(url);

  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");

  if (!response.ok) {
    let errorMessage = `Download failed with status ${response.status}`;
    if (isJson) {
      try {
        const errorJson = await response.json();
        if (errorJson && errorJson.detail) {
          errorMessage = Array.isArray(errorJson.detail)
            ? errorJson.detail.map((d) => d.msg).join("; ")
            : String(errorJson.detail);
        }
      } catch {
        // ignore
      }
    }
    throw new Error(errorMessage);
  }

  const blob = await response.blob();
  return {
    blob,
    filename: `${ticketId}_output.zip`,
  };
}