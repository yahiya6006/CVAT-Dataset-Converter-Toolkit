// app.js
// ---------
// Entry point for the Stage 1 frontend.
// Wires up UI events, keeps simple state, and calls the API wrapper.

import { getSessionId } from "./session.js";
import {
  showStatus,
  clearStatus,
  setNextButtonEnabled,
  toggleFeatureOptions,
  setAllInputsDisabled,
  updateFileInfo,
} from "./dom.js";
import { uploadDataset } from "./api.js";

/**
 * Simple in-memory state for the current page.
 * This is intentionally small and explicit.
 */
const state = {
  file: null,
  inputFormat: "",
  targetFormat: "",
  featureType: "",
  isUploading: false,
};

document.addEventListener("DOMContentLoaded", () => {
  // Ensure we have a session ID (used later in backend calls).
  // We no longer show it in the UI.
  getSessionId();

  // Connect form elements
  const fileInput = document.getElementById("zipFileInput");
  const inputFormatSelect = document.getElementById("inputFormatSelect");
  const targetFormatSelect = document.getElementById("targetFormatSelect");
  const featureTypeRadios = document.querySelectorAll(
    'input[name="featureType"]'
  );
  const nextButton = document.getElementById("nextButton");

  if (!fileInput || !inputFormatSelect || !targetFormatSelect || !nextButton) {
    // This should not happen unless the HTML was changed incorrectly.
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
      // We keep state.targetFormat as-is but adjust validation logic,
      // and we'll send an empty target format to the backend.
      refreshNextButtonState();
      clearStatus();
    });
  });

  // 5) Next button click
  nextButton.addEventListener("click", async () => {
    if (state.isUploading) {
      return; // Guard against double-click
    }

    // Final validation check
    if (!isStateValidForUpload()) {
      showStatus(
        "error",
        "Please select a ZIP file, input format, feature type, and (if applicable) target format."
      );
      return;
    }

    const sessionIdNow = getSessionId(); // ensure we always use the same ID
    const featureParams = collectFeatureParams(state.featureType);

    // For cropping, target format is not used, so we send an empty string.
    const targetFormatToSend =
      state.featureType === "crop_objects" ? "" : state.targetFormat;

    // Start upload
    state.isUploading = true;
    setAllInputsDisabled(true);
    showStatus("info", "Uploading dataset to backend (Stage 2)…");

    try {
      const result = await uploadDataset({
        file: state.file,
        inputFormat: state.inputFormat,
        targetFormat: targetFormatToSend,
        featureType: state.featureType,
        featureParams,
        sessionId: sessionIdNow,
      });

      // For Stage 1, we don't expect a particular schema; just show something.
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
        `${message} (Backend processing will be implemented in Stage 2.)`
      );
    } catch (err) {
      console.error("Upload error:", err);
      const errorMessage =
        err && err.message
          ? err.message
          : "Network error while sending upload request.";
      showStatus(
        "error",
        `${errorMessage} (Is the backend running at http://localhost:8000?)`
      );
    } finally {
      state.isUploading = false;
      setAllInputsDisabled(false);
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
