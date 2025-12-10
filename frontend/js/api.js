// api.js
// ---------
// Contains thin wrappers around HTTP calls to the backend.
// For Stage 1, we focus on /upload. The backend itself will be implemented in Stage 2.

/**
 * Base URL for the backend API.
 * For Stage 1, we assume FastAPI will run at http://localhost:8000.
 * You can change this if needed.
 */
const API_BASE_URL = "http://localhost:8000";

/**
 * Uploads the selected dataset ZIP and metadata to the backend.
 *
 * @param {Object} params
 * @param {File} params.file - The ZIP file selected by the user.
 * @param {string} params.inputFormat - Selected input format (e.g. "cvat_images_1_1").
 * @param {string} params.targetFormat - Selected target format (e.g. "yolo_v5").
 * @param {string} params.featureType - Selected feature type ("convert_only", ...).
 * @param {Object} params.featureParams - Additional options depending on featureType.
 * @param {string} params.sessionId - Client session ID.
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

  const isJson =
    response.headers.get("content-type") &&
    response.headers.get("content-type").includes("application/json");

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
        // ignore JSON parsing error, we already have a generic message
      }
    }
    throw new Error(errorMessage);
  }

  if (!isJson) {
    // Stage 2 backend is expected to return JSON, but we guard this anyway.
    const text = await response.text();
    return { raw: text };
  }

  return response.json();
}
