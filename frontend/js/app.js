// app.js
// ---------
// Entry point for the frontend.
// Wires up UI events, keeps simple state, and calls the API wrappers.

import { getSessionId } from "./session.js";
import {
  showStatus,
  clearStatus,
  setNextButtonEnabled,
  setNextButtonLabel,
  toggleFeatureOptions,
  setAllInputsDisabled,
  updateFileInfo,
  appendLogLine,
  clearLog,
} from "./dom.js";
import {
  uploadDataset,
  fetchUploadStatus,
  cancelTicket,
  downloadOutput,
} from "./api.js";
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
  ticketState: "idle",

  // Timers for status polling
  statusPollTimeoutId: null,
  statusPollIntervalId: null,

  // To avoid showing label meta more than once
  labelMetaShown: false,

  // For log deduplication
  lastStatusState: null,
  lastUploadProgressPct: null,
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
    clearStatus();
    refreshNextButtonState();
  });

  // 2) Input format select
  inputFormatSelect.addEventListener("change", (event) => {
    state.inputFormat = event.target.value || "";
    clearStatus();
    refreshNextButtonState();
  });

  // 3) Target format select (Step 4)
  targetFormatSelect.addEventListener("change", (event) => {
    state.targetFormat = event.target.value || "";
    clearStatus();
    refreshNextButtonState();
  });

  // 4) Feature type radios (Step 3)
  featureTypeRadios.forEach((radio) => {
    radio.addEventListener("change", (event) => {
      const value = event.target.value;
      state.featureType = value || "";
      toggleFeatureOptions(state.featureType);
      clearStatus();
      refreshNextButtonState();
    });
  });

  // 5) Main button click: Upload -> Cancel -> Download -> Start over
  nextButton.addEventListener("click", async () => {
    // Ignore clicks while the initial upload HTTP request is in-flight
    if (state.isUploading) {
      return;
    }

    // No active ticket yet -> start new upload
    if (!state.ticketId || state.ticketState === "idle") {
      await handleStartUpload();
      return;
    }

    // Ticket is in some processing state -> Cancel
    if (isProcessingTicketState(state.ticketState)) {
      await handleCancel();
      return;
    }

    // Ticket is ready -> Download
    if (state.ticketState === "ready") {
      await handleDownload();
      return;
    }

    // Error / cancelled / unknown -> Start over
    resetFrontendState();
  });

  // Initial state
  clearStatus();
  clearLog();
  appendLogLine('Ready. Select a dataset ZIP and click "Upload & Process".');
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
 * Helper: is the ticket in an in-progress state?
 */
function isProcessingTicketState(ticketState) {
  return (
    ticketState === "uploading" ||
    ticketState === "uploaded" ||
    ticketState === "extracting_label_meta" ||
    ticketState === "labels_meta_extracted" ||
    ticketState === "processing_dataset"
  );
}

/**
 * Sync the main button label + enabled/disabled state from current state.
 */
function refreshNextButtonState() {
  const hasTicket = !!state.ticketId;

  // No active ticket -> pure upload mode
  if (!hasTicket || state.ticketState === "idle") {
    setNextButtonLabel("Upload & Process");
    const canUpload = isStateValidForUpload() && !state.isUploading;
    setNextButtonEnabled(canUpload);
    return;
  }

  // While upload HTTP request is in-flight we don't allow cancelling
  if (state.isUploading) {
    setNextButtonLabel("Uploading…");
    setNextButtonEnabled(false);
    return;
  }

  // Processing states -> Cancel
  if (isProcessingTicketState(state.ticketState)) {
    setNextButtonLabel("Cancel");
    setNextButtonEnabled(true);
    return;
  }

  // Ready -> Download
  if (state.ticketState === "ready") {
    setNextButtonLabel("Download");
    setNextButtonEnabled(true);
    return;
  }

  // Error / cancelled / unknown -> Start over
  setNextButtonLabel("Start over");
  setNextButtonEnabled(true);
}

/**
 * Reads feature-specific parameters from the DOM for the selected feature type.
 *
 * @param {string} featureType
 * @returns {Object}
 */
function collectFeatureParams(featureType) {
  const prefixEl = document.getElementById("outputPrefixInput");
  const rawPrefix = prefixEl && prefixEl.value ? prefixEl.value.trim() : "";
  const outputPrefix = rawPrefix || "";

  if (featureType === "convert_only") {
    const includeEl = document.getElementById("includeImagesCheckbox");
    const includeImages = includeEl ? !!includeEl.checked : true;

    return {
      output_prefix: outputPrefix,
      include_images: includeImages,
    };
  }

  if (featureType === "resize_and_convert") {
    const widthEl = document.getElementById("resizeWidthInput");
    const heightEl = document.getElementById("resizeHeightInput");
    const preserveEl = document.getElementById("preserveAspectCheckbox");

    const width = widthEl && widthEl.value ? parseInt(widthEl.value, 10) : null;
    const height =
      heightEl && heightEl.value ? parseInt(heightEl.value, 10) : null;
    const preserveAspect = preserveEl ? !!preserveEl.checked : true;

    return {
      output_prefix: outputPrefix,
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
      output_prefix: outputPrefix,
      padding: Number.isFinite(padding) ? padding : 0,
      per_class_folders: perClass,
    };
  }

  // Default: only prefix (if any)
  return {
    output_prefix: outputPrefix,
  };
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

    const backendState = status && status.state ? status.state : "unknown";
    const upload = status && status.upload ? status.upload : null;

    state.ticketState = backendState;

    let progressPct = null;
    if (
      upload &&
      typeof upload.progress === "number" &&
      Number.isFinite(upload.progress)
    ) {
      progressPct = Math.round(upload.progress * 100);
    }

    // Log state changes and significant progress changes
    if (backendState !== state.lastStatusState) {
      state.lastStatusState = backendState;
      appendLogLine(`State: ${backendState}`);
    }

    if (progressPct !== null && progressPct !== state.lastUploadProgressPct) {
      state.lastUploadProgressPct = progressPct;
      appendLogLine(`Upload progress: ${progressPct}%`);
    }

    // Status line at top
    let message = `Ticket ${state.ticketId} — state: ${backendState}`;
    if (progressPct !== null) {
      message += ` • upload: ${progressPct}%`;
    }
    showStatus("info", message);

    // Label metadata (logged only once per ticket)
    if (status.label_meta && !state.labelMetaShown) {
      state.labelMetaShown = true;
      const meta = status.label_meta;
      appendLogLine("Label metadata:");
      if (typeof meta.image_count === "number") {
        appendLogLine(`  Images: ${meta.image_count}`);
      }
      if (typeof meta.box_count === "number") {
        appendLogLine(`  Boxes: ${meta.box_count}`);
      }
      const labels = Array.isArray(meta.labels) ? meta.labels : [];
      if (labels.length > 0) {
        appendLogLine("  Labels:");
        labels.forEach((lbl) => {
          const name = lbl.name ?? "unknown";
          const count = lbl.count ?? 0;
          appendLogLine(`    - ${name}: ${count}`);
        });
      }
    }

    // Stop polling for terminal failure states.
    // For "ready" we keep polling so TTL doesn't delete the ticket
    // while the user is looking at the page.
    if (
      backendState === "error" ||
      backendState === "cancelled" ||
      backendState === "unknown"
    ) {
      stopStatusPolling();
    }

    refreshNextButtonState();
  } catch (err) {
    console.error("Status polling error:", err);
    const msg =
      err && err.message
        ? err.message
        : "Unable to fetch status from backend.";
    showStatus("error", msg);
    appendLogLine("Status polling error: " + msg);
    stopStatusPolling();
    refreshNextButtonState();
  }
}

/**
 * Start a new upload + processing job for the current form state.
 */
async function handleStartUpload() {
  if (!isStateValidForUpload()) {
    showStatus(
      "error",
      "Please select a ZIP file, input format, feature type, and (if applicable) target format."
    );
    return;
  }

  const ticketId = getSessionId();
  state.ticketId = ticketId;
  state.ticketState = "uploading";
  state.labelMetaShown = false;
  state.lastStatusState = null;
  state.lastUploadProgressPct = null;

  clearStatus();
  clearLog();
  appendLogLine(`Starting new job: ${ticketId}`);
  appendLogLine("Uploading dataset to backend…");

  const featureParams = collectFeatureParams(state.featureType);

  const targetFormatToSend =
    state.featureType === "crop_objects" ? "" : state.targetFormat;

  // Cancel any previous polling timers (if user re-runs an upload)
  stopStatusPolling();

  state.isUploading = true;
  setAllInputsDisabled(true); // lock inputs while job is active
  refreshNextButtonState(); // shows "Uploading…"

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
      if (result.state) {
        state.ticketState = result.state;
      }
    }

    appendLogLine("Upload completed on backend.");
    showStatus("success", `${message} Tracking ticket ${ticketId}…`);

    // Start status polling:
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
    appendLogLine("Upload error: " + errorMessage);

    // Reset everything on hard failure
    resetFrontendState();
    return;
  } finally {
    state.isUploading = false;
    refreshNextButtonState();
  }
}

/**
 * Cancel the current job.
 */
async function handleCancel() {
  if (!state.ticketId) {
    return;
  }

  appendLogLine(`Cancelling ticket ${state.ticketId}…`);
  showStatus("info", "Cancelling current job…");

  try {
    await cancelTicket(state.ticketId);
    appendLogLine("Ticket cancelled and cleaned up by backend.");
  } catch (err) {
    console.error("Cancel error:", err);
    const msg =
      err && err.message ? err.message : "Failed to cancel ticket.";
    appendLogLine("Cancel error: " + msg);
    showStatus("error", msg);
  } finally {
    stopStatusPolling();
    resetFrontendState();
  }
}

/**
 * Download the processed output for the current ticket,
 * then reset the UI (backend cleans up via BackgroundTasks).
 */
async function handleDownload() {
  if (!state.ticketId) {
    return;
  }

  appendLogLine(`Downloading output for ticket ${state.ticketId}…`);
  showStatus("info", "Downloading processed dataset…");

  try {
    const { blob, filename } = await downloadOutput(state.ticketId);

    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    appendLogLine("Download complete. Cleaning up and resetting UI.");
    showStatus("success", "Download complete.");
  } catch (err) {
    console.error("Download error:", err);
    const msg =
      err && err.message ? err.message : "Download failed.";
    appendLogLine("Download error: " + msg);
    showStatus("error", msg);
    return;
  } finally {
    stopStatusPolling();
    resetFrontendState();
  }
}

/**
 * Reset the page to "idle" state while preserving user selections
 * (file, formats, feature type), so they can quickly run again.
 */
function resetFrontendState() {
  stopStatusPolling();

  state.ticketId = null;
  state.ticketState = "idle";
  state.isUploading = false;
  state.labelMetaShown = false;
  state.lastStatusState = null;
  state.lastUploadProgressPct = null;

  setAllInputsDisabled(false);
  clearStatus();
  clearLog();
  appendLogLine('Ready. Select a dataset ZIP and click "Upload & Process".');

  refreshNextButtonState();
}
