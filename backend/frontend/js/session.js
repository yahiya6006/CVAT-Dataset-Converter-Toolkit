// session.js
// -------------
// Generates a new ticket ID every time it is called.
// This ID is NOT stored in localStorage or sessionStorage.
//
// The ticket ID is designed to be filesystem-friendly so it can be safely
// used as a folder name on the server side (only letters, numbers, dashes).

/**
 * Returns a new ticket ID.
 *
 * Every call returns a fresh value. The frontend should keep this ticket
 * in memory (e.g. in app.js state) for the duration of a single upload/job.
 */
export function getSessionId() {
  return generateTicketId();
}

/**
 * Generate a reasonably unique ticket identifier.
 * Uses crypto.randomUUID when available, falls back to timestamp + random.
 */
function generateTicketId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    // UUID is filesystem-friendly on both Windows and Linux.
    return `ticket-${window.crypto.randomUUID()}`;
  }

  // Fallback: timestamp + random suffix (hex)
  const timestamp = Date.now().toString(16);
  const random = Math.floor(Math.random() * 1e16)
    .toString(16)
    .padStart(12, "0");

  return `ticket-${timestamp}-${random}`;
}
