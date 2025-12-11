// config.js
// ----------
// Central configuration for the frontend.
// You can tweak the API base URL and status polling timing here,
// without touching the rest of the code.

/**
 * Base URL of the backend API.
 * Change this when your FastAPI server runs on a different host/port.
 */
function resolveApiBaseUrl() {
  const { protocol, hostname } = window.location;

  // Port set at runtime from /runtime-config.js
  const apiPort =
    (window.CVAT_CONFIG && window.CVAT_CONFIG.apiPort) || 6007;

  return `${protocol}//${hostname}:${apiPort}`;
}

export const API_BASE_URL = resolveApiBaseUrl();


/**
 * Configuration for status polling:
 * - initialDelayMs: how long to wait after upload completes before
 *   the first status check.
 * - intervalMs: how often to poll /status after the first check.
 */
export const STATUS_POLL_CONFIG = {
  initialDelayMs: 5000, // 5 seconds before first poll
  intervalMs: 2000,     // then every 2 seconds
};
