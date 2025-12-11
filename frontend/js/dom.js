// dom.js
// ---------
// Small helper utilities for DOM manipulation and UI updates.
// Keeps app.js focused on high-level logic.

const statusArea = document.getElementById("statusArea");
const nextButton = document.getElementById("nextButton");
const convertOptionsBlock = document.getElementById("convertOptions");
const resizeOptionsBlock = document.getElementById("resizeOptions");
const cropOptionsBlock = document.getElementById("cropOptions");
const targetFormatSection = document.getElementById("targetFormatSection");
const logArea = document.getElementById("logArea");

/**
 * Shows a status message in the status area.
 *
 * @param {"info" | "success" | "error"} type
 * @param {string} message
 */
export function showStatus(type, message) {
  if (!statusArea) return;
  statusArea.textContent = message || "";
  statusArea.classList.remove("info", "success", "error");
  if (type) {
    statusArea.classList.add(type);
  }
}

/** Clears any status message */
export function clearStatus() {
  if (!statusArea) return;
  statusArea.textContent = "";
  statusArea.classList.remove("info", "success", "error");
}

/**
 * Enables or disables the main button.
 *
 * @param {boolean} enabled
 */
export function setNextButtonEnabled(enabled) {
  if (!nextButton) return;
  nextButton.disabled = !enabled;
}

/**
 * Sets the main button label.
 *
 * @param {string} label
 */
export function setNextButtonLabel(label) {
  if (!nextButton) return;
  nextButton.textContent = label;
}

/**
 * Toggle visibility of feature-specific option blocks
 * and target format section based on the selected feature type.
 *
 * @param {string} featureType
 */
export function toggleFeatureOptions(featureType) {
  // Hide all feature-specific blocks first
  if (convertOptionsBlock) {
    convertOptionsBlock.classList.add("hidden");
  }
  if (resizeOptionsBlock) {
    resizeOptionsBlock.classList.add("hidden");
  }
  if (cropOptionsBlock) {
    cropOptionsBlock.classList.add("hidden");
  }

  // Show block for selected feature
  if (featureType === "convert_only") {
    convertOptionsBlock?.classList.remove("hidden");
  } else if (featureType === "resize_and_convert") {
    resizeOptionsBlock?.classList.remove("hidden");
  } else if (featureType === "crop_objects") {
    cropOptionsBlock?.classList.remove("hidden");
  }

  // Toggle Step 4 (target format) visibility:
  // - For "crop_objects" this step is not required and is hidden.
  // - For other feature types it remains visible.
  if (targetFormatSection) {
    if (featureType === "crop_objects") {
      targetFormatSection.classList.add("hidden");
    } else {
      targetFormatSection.classList.remove("hidden");
    }
  }
}

/**
 * Enables or disables all user inputs (EXCEPT the main button)
 * to avoid accidental changes during upload/processing.
 *
 * @param {boolean} disabled
 */
export function setAllInputsDisabled(disabled) {
  const selectors = [
    "#zipFileInput",
    "#inputFormatSelect",
    "#targetFormatSelect",
    'input[name="featureType"]',
    "#resizeWidthInput",
    "#resizeHeightInput",
    "#preserveAspectCheckbox",
    "#cropPaddingInput",
    "#cropPerClassCheckbox",
    "#includeImagesCheckbox",
    "#outputPrefixInput",
  ];

  selectors.forEach((sel) => {
    const elements = document.querySelectorAll(sel);
    elements.forEach((el) => {
      if (el instanceof HTMLInputElement || el instanceof HTMLSelectElement) {
        el.disabled = disabled;
      }
    });
  });
}

/**
 * Updates the displayed file information text.
 *
 * @param {File | null} file
 */
export function updateFileInfo(file) {
  const fileInfo = document.getElementById("fileInfo");
  if (!fileInfo) return;

  if (!file) {
    fileInfo.textContent = "No file selected yet.";
    return;
  }

  const sizeKb = file.size / 1024;
  const sizeStr =
    sizeKb > 1024
      ? `${(sizeKb / 1024).toFixed(2)} MB`
      : `${sizeKb.toFixed(1)} KB`;

  fileInfo.textContent = `Selected: ${file.name} (${sizeStr})`;
}

/**
 * Append a line to the terminal-style log.
 *
 * @param {string} message
 */
export function appendLogLine(message) {
  if (!logArea) return;
  const ts = new Date().toLocaleTimeString();
  logArea.textContent += `[${ts}] ${message}\n`;
  logArea.scrollTop = logArea.scrollHeight;
}

/** Clear the terminal-style log. */
export function clearLog() {
  if (!logArea) return;
  logArea.textContent = "";
}
