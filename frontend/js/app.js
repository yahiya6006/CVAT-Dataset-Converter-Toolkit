// app.js
// ---------
// Entry point for the frontend.
// Wires up UI events, keeps simple state, and calls the API wrappers.

import { getSessionId } from "./session.js";
import {
  showStatus,
  clearStatus,
  setNextButtonEnabled,
  toggleFeatureOptions,
  setAllInputsDisabled,
  updateFileInfo,
} from "./dom.js";
import { uploadDataset, fetchUploadStatus } from "./api.js";
import { STATUS_POLL_CONFIG } from "./config.js";

/**
 * Simple in-memory state for the current page.
 */
const state = {
  file: null,
  inputFormat: "",
  targetFormat: "",
  featureType: "",
  isUploading: false,

  // Ticket for the current upload/job
  ticketId: null,

  // Timers for status polling
  statusPollTimeoutId: null,
  statusPollIntervalId: null,

  // To avoid popping the label meta more than once
  labelMetaShown: false,
};

document.addEventListener("DOMContentLoaded", () => {
  // Connect form elements
  const fileInput = document.getElementById("zipFileInput");
  const inputFormatSelect = document.getElementById("inputFormatSelect");
  const targetFormatSelect = document.getElementById("targetFormatSelect");
  const featureTypeRadios = document.querySelectorAll(
    'input[name="featureType"]'
  );
  const nextButton = document.getElementById("nextButton");

  if (!fileInput || !inputFormatSelect || !targetFormatSelect || !nextButton) {
    console.error("One or more required DOM elements are missing.");
    return;
  }

  // 1) File input
  fileInput.addEventListener("change", (event) => {
    const input = event.target;
    const file = input.files && input.files[0] ? input.files[0] : null;
    state.file = file || null;
    updateFileInfo(state.file);
    refreshNextButtonState();
    clearStatus();
  });

  // 2) Input format select
  inputFormatSelect.addEventListener("change", (event) => {
    state.inputFormat = event.target.value || "";
    refreshNextButtonState();
    clearStatus();
  });

  // 3) Target format select (Step 4)
  targetFormatSelect.addEventListener("change", (event) => {
    state.targetFormat = event.target.value || "";
    refreshNextButtonState();
    clearStatus();
  });

  // 4) Feature type radios (Step 3)
  featureTypeRadios.forEach((radio) => {
    radio.addEventListener("change", (event) => {
      const value = event.target.value;
      state.featureType = value || "";
      toggleFeatureOptions(state.featureType);

      // If we switch to "crop_objects", Step 4 (target format) is irrelevant.
      refreshNextButtonState();
      clearStatus();
    });
  });

  // 5) Next button click
  nextButton.addEventListener("click", async () => {
    if (state.isUploading) {
      return; // Guard against double-click during upload
    }

    if (!isStateValidForUpload()) {
      showStatus(
        "error",
        "Please select a ZIP file, input format, feature type, and (if applicable) target format."
      );
      return;
    }

    // New ticket for this upload/job
    const ticketId = getSessionId();
    state.ticketId = ticketId;
    state.labelMetaShown = false;

    const featureParams = collectFeatureParams(state.featureType);

    // For cropping, target format is not used, so we send an empty string.
    const targetFormatToSend =
      state.featureType === "crop_objects" ? "" : state.targetFormat;

    // Cancel any previous polling timers (if user re-runs an upload)
    stopStatusPolling();

    // Start upload
    state.isUploading = true;
    setAllInputsDisabled(true); // includes disabling Next
    showStatus("info", "Uploading dataset to backend…");

    try {
      const result = await uploadDataset({
        file: state.file,
        inputFormat: state.inputFormat,
        targetFormat: targetFormatToSend,
        featureType: state.featureType,
        featureParams,
        sessionId: ticketId,
      });

      let message = "Upload request sent successfully.";
      if (result && typeof result === "object") {
        if (result.message) {
          message = result.message;
        } else if (result.status) {
          message = `${result.status}: upload request sent.`;
        }
      }

      showStatus(
        "success",
        `${message} Starting status polling for ticket ${ticketId}…`
      );

      // Start status polling:
      // - First call after STATUS_POLL_CONFIG.initialDelayMs
      // - Then every STATUS_POLL_CONFIG.intervalMs
      startStatusPolling();
    } catch (err) {
      console.error("Upload error:", err);
      const errorMessage =
        err && err.message
          ? err.message
          : "Network error while sending upload request.";
      showStatus(
        "error",
        `${errorMessage} (Is the backend running at the configured API URL?)`
      );
    } finally {
      state.isUploading = false;
      setAllInputsDisabled(false); // re-enable controls
      refreshNextButtonState();
    }
  });

  // Initial state
  updateFileInfo(null);
  refreshNextButtonState();
});

/**
 * Returns true if the user has provided enough information to start an upload.
 *
 * For "crop_objects", target format (Step 4) is not required.
 * For other feature types, target format is required.
 */
function isStateValidForUpload() {
  if (!state.file || !state.inputFormat || !state.featureType) {
    return false;
  }

  if (state.featureType === "crop_objects") {
    // Step 4 is not applicable in this case
    return true;
  }

  // For the other feature types, we expect a target format.
  return !!state.targetFormat;
}

/**
 * Updates the enabled/disabled state of the Next button
 * based on the current state.
 */
function refreshNextButtonState() {
  const canUpload = isStateValidForUpload() && !state.isUploading;
  setNextButtonEnabled(canUpload);
}

/**
 * Reads feature-specific parameters from the DOM for the selected feature type.
 *
 * @param {string} featureType
 * @returns {Object}
 */
function collectFeatureParams(featureType) {
  if (featureType === "resize_and_convert") {
    const widthEl = document.getElementById("resizeWidthInput");
    const heightEl = document.getElementById("resizeHeightInput");
    const preserveEl = document.getElementById("preserveAspectCheckbox");

    const width = widthEl && widthEl.value ? parseInt(widthEl.value, 10) : null;
    const height =
      heightEl && heightEl.value ? parseInt(heightEl.value, 10) : null;
    const preserveAspect = preserveEl ? !!preserveEl.checked : true;

    return {
      width: Number.isFinite(width) ? width : null,
      height: Number.isFinite(height) ? height : null,
      preserve_aspect_ratio: preserveAspect,
    };
  }

  if (featureType === "crop_objects") {
    const paddingEl = document.getElementById("cropPaddingInput");
    const perClassEl = document.getElementById("cropPerClassCheckbox");

    const padding =
      paddingEl && paddingEl.value ? parseInt(paddingEl.value, 10) : 0;
    const perClass = perClassEl ? !!perClassEl.checked : true;

    return {
      padding: Number.isFinite(padding) ? padding : 0,
      per_class_folders: perClass,
    };
  }

  // Default: no extra parameters
  return {};
}

/**
 * Starts polling /status for the current ticket in state.ticketId.
 * - First poll after STATUS_POLL_CONFIG.initialDelayMs.
 * - Then polls every STATUS_POLL_CONFIG.intervalMs.
 */
function startStatusPolling() {
  if (!state.ticketId) {
    console.warn("No ticketId set; cannot start status polling.");
    return;
  }

  // Ensure previous timers are cleared
  stopStatusPolling();

  state.statusPollTimeoutId = window.setTimeout(async () => {
    // First poll
    await pollStatusOnce();

    // Subsequent polls
    state.statusPollIntervalId = window.setInterval(
      pollStatusOnce,
      STATUS_POLL_CONFIG.intervalMs
    );
  }, STATUS_POLL_CONFIG.initialDelayMs);
}

/**
 * Stops any active status polling timers.
 */
function stopStatusPolling() {
  if (state.statusPollTimeoutId !== null) {
    window.clearTimeout(state.statusPollTimeoutId);
    state.statusPollTimeoutId = null;
  }
  if (state.statusPollIntervalId !== null) {
    window.clearInterval(state.statusPollIntervalId);
    state.statusPollIntervalId = null;
  }
}

/**
 * Performs a single /status call for the current ticket and updates the UI.
 */
async function pollStatusOnce() {
  if (!state.ticketId) {
    return;
  }

  try {
    const status = await fetchUploadStatus(state.ticketId);
    console.log("Status for ticket", state.ticketId, status);

    if (status && status.state) {
      let message = `Ticket ${state.ticketId} — state: ${status.state}`;

      // If backend returns upload progress, show it
      if (status.upload && typeof status.upload.progress === "number") {
        const pct = Math.round(status.upload.progress * 100);
        message += ` • upload: ${pct}%`;
      }

      showStatus("info", message);

      // If label metadata is ready, show a simple popup with details
      if (
        status.state === "labels_meta_extracted" &&
        status.label_meta &&
        !state.labelMetaShown
      ) {
        state.labelMetaShown = true;

        const meta = status.label_meta;
        const lines = [];

        if (typeof meta.image_count === "number") {
          lines.push(`Images: ${meta.image_count}`);
        }
        if (typeof meta.box_count === "number") {
          lines.push(`Total boxes: ${meta.box_count}`);
        }

        const labels = Array.isArray(meta.labels) ? meta.labels : [];
        if (labels.length > 0) {
          lines.push("");
          lines.push("Labels:");
          labels.forEach((lbl) => {
            const name = lbl.name ?? "unknown";
            const count = lbl.count ?? 0;
            lines.push(`- ${name}: ${count}`);
          });
        }

        alert(lines.join("\n"));

        // Once label meta is shown, we can stop polling.
        stopStatusPolling();
      }

      // Stop polling if backend reports terminal error
      if (status.state === "error") {
        stopStatusPolling();
      }
    } else {
      showStatus("info", `Status response: ${JSON.stringify(status)}`);
    }
  } catch (err) {
    console.error("Status polling error:", err);
    showStatus(
      "error",
      "Unable to fetch status from backend (is /status implemented?)."
    );
    // Optional: stop polling on error to avoid spamming the backend
    stopStatusPolling();
  }
}
