// session.js
// -------------
// Handles generation and persistence of a simple session ID on the client.
// This ID will be sent along with uploads so the backend can track sessions.

const SESSION_STORAGE_KEY = "datasetConverterSessionId";

/**
 * Ensures we have a stable session ID in localStorage and returns it.
 * The ID is generated once and reused across page reloads.
 */
export function getSessionId() {
  let existing = null;
  try {
    existing = window.localStorage.getItem(SESSION_STORAGE_KEY);
  } catch {
    // In very restricted environments localStorage may not be available.
    existing = null;
  }

  if (existing) {
    return existing;
  }

  const newId = generateSessionId();

  try {
    window.localStorage.setItem(SESSION_STORAGE_KEY, newId);
  } catch {
    // Ignore storage errors, we can still use in-memory session for this tab.
  }

  return newId;
}

/**
 * Generate a reasonably unique session identifier.
 * Uses crypto.randomUUID when available, falls back to timestamp + random.
 */
function generateSessionId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID();
  }

  // Fallback: timestamp + random suffix
  const timestamp = Date.now().toString(16);
  const random = Math.floor(Math.random() * 1e16)
    .toString(16)
    .padStart(12, "0");

  return `sess-${timestamp}-${random}`;
}
