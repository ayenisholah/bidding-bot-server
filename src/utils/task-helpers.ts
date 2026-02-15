/**
 * Task utility functions
 * Pure helper functions for task logging, error parsing, and identifier masking
 */

/**
 * Extract task ID from various possible context locations
 */
export function getTaskIdForLog(context: any): string {
  // Check various possible locations for taskId
  return context?.taskId ||
    context?.data?.taskId ||
    context?.data?._id ||
    context?._id ||
    context?.id ||
    'N/A';
}

/**
 * Parse OpenSea API response data into human-readable error messages
 * Handles JSON strings, Cloudflare blocks, error arrays, and nested objects
 */
export function parseOpenSeaResponseData(data: any): string | null {
  if (!data) {
    return null;
  }

  if (typeof data === 'string') {
    const trimmed = data.trim();

    const looksLikeJson = (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
      (trimmed.startsWith('[') && trimmed.endsWith(']'));

    if (looksLikeJson) {
      try {
        return parseOpenSeaResponseData(JSON.parse(trimmed));
      } catch (_) {
        // fall through to string handling if JSON.parse fails
      }
    }

    const rayMatch = trimmed.match(/Ray ID:\s*<strong[^>]*>([^<]+)<\/strong>/i);
    const ipMatch = trimmed.match(/id=["']cf-footer-ip["'][^>]*>\s*([^<\s]+)\s*<\/span>/i);
    const headlineMatch = trimmed.match(/data-translate=["']block_headline["']>([^<]+)</i);
    const targetMatch = trimmed.match(/data-translate=["']unable_to_access["'][^>]*><span[^>]*>.*?<\/span>\s*([^<]+)/i);
    const reasonMatch = trimmed.match(/data-translate=["']blocked_why_detail["']>([^<]+)</i);

    if (rayMatch || ipMatch || headlineMatch || reasonMatch) {
      const parts: string[] = ['Cloudflare block detected'];
      if (headlineMatch) {
        parts.push(headlineMatch[1].trim());
      }
      if (targetMatch) {
        parts.push(`target=${targetMatch[1].trim()}`);
      }
      if (rayMatch) {
        parts.push(`ray=${rayMatch[1].trim()}`);
      }
      if (ipMatch) {
        parts.push(`ip=${ipMatch[1].trim()}`);
      }
      if (reasonMatch) {
        parts.push(reasonMatch[1].trim());
      }
      return parts.join(' | ');
    }

    const sanitized = trimmed.replace(/\s+/g, ' ');
    return sanitized.length > 200 ? `${sanitized.slice(0, 200)}…` : sanitized;
  }

  if (Array.isArray(data)) {
    const messages = data
      .map((entry) => parseOpenSeaResponseData(entry))
      .filter((value): value is string => Boolean(value));

    return messages.length ? messages.join('; ') : null;
  }

  if (typeof data === 'object') {
    const messages: string[] = [];

    if (Array.isArray((data as any).errors)) {
      const errorMessages = (data as any).errors
        .map((err: any) => {
          if (!err) {
            return null;
          }
          if (typeof err === 'string') {
            return err;
          }
          if (typeof err?.message === 'string') {
            return err.message;
          }
          if (typeof err?.code === 'string' && typeof err?.description === 'string') {
            return `${err.code}: ${err.description}`;
          }
          try {
            const stringified = JSON.stringify(err);
            return stringified.length > 200 ? `${stringified.slice(0, 200)}…` : stringified;
          } catch (_) {
            return String(err);
          }
        })
        .filter((value: any): value is string => Boolean(value));

      if (errorMessages.length) {
        messages.push(`errors: ${errorMessages.join('; ')}`);
      }
    }

    if (typeof (data as any).error === 'string') {
      messages.push((data as any).error);
    }

    if (typeof (data as any).message === 'string') {
      messages.push((data as any).message);
    }

    if (typeof (data as any).detail === 'string') {
      messages.push((data as any).detail);
    }

    if (messages.length) {
      return messages.join(' | ');
    }

    try {
      const stringified = JSON.stringify(data);
      if (stringified && stringified !== '{}') {
        return stringified.length > 200 ? `${stringified.slice(0, 200)}…` : stringified;
      }
    } catch (_) {
      return String(data);
    }

    return null;
  }

  return null;
}

/**
 * Mask sensitive identifiers (wallet addresses, order hashes) for safe logging
 * Shows first 6 and last 4 characters, e.g., "0x1234…abcd"
 */
export function maskSensitiveIdentifier(value: string | undefined | null): string {
  if (!value) {
    return 'N/A';
  }

  const trimmed = value.trim();
  if (trimmed.length <= 10) {
    return trimmed;
  }

  return `${trimmed.slice(0, 6)}…${trimmed.slice(-4)}`;
}

/**
 * Extract detailed OpenSea error messages from error objects
 * Combines HTTP status, error codes, URLs, and response data
 */
export function extractDetailedOpenSeaError(error: any): string {
  const parts: string[] = [];

  const status = error?.response?.status;
  const statusText = error?.response?.statusText;
  if (status) {
    parts.push(`HTTP ${status}${statusText ? ` ${statusText}` : ''}`);
  }

  const code = error?.code;
  if (code) {
    parts.push(`code=${code}`);
  }

  const url = error?.config?.url ?? error?.response?.config?.url;
  if (url) {
    parts.push(`url=${url}`);
  }

  const responseDetails = parseOpenSeaResponseData(error?.response?.data);
  if (responseDetails) {
    parts.push(responseDetails);
  }

  if (!parts.length && error?.message) {
    parts.push(error.message);
  }

  if (!parts.length) {
    return typeof error === 'string' ? error : 'Unknown OpenSea error';
  }

  return parts.join(' | ');
}
