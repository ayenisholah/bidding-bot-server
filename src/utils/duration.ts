/**
 * Converts listing duration to seconds with optional minimum duration enforcement
 * 
 * @param duration - Duration object containing value and unit
 * @param options - Optional configuration for minimum duration and marketplace name
 * @returns Duration in seconds with optional minimum enforced
 */
export function convertDurationToSeconds(
  duration: { value: number; unit: string },
  options?: {
    minimumSeconds?: number;
    marketplaceName?: string;
  }
): number {
  const { minimumSeconds = 0, marketplaceName: _marketplaceName = 'MARKETPLACE' } = options || {};

  let seconds: number;
  
  switch (duration.unit.toLowerCase()) {
    case 'minutes':
      seconds = duration.value * 60;
      break;
    case 'hours':
      seconds = duration.value * 60 * 60;
      break;
    case 'days':
      seconds = duration.value * 60 * 60 * 24;
      break;
    default:
      // console.log(`[${marketplaceName}] Unrecognized unit '${duration.unit}', defaulting to ${minimumSeconds || 900} seconds`);
      seconds = minimumSeconds || 900; // default to minimum or 15 minutes
  }
  
  // Enforce minimum duration if specified
  if (minimumSeconds && seconds < minimumSeconds) {
    // console.log(`[${marketplaceName}] Duration ${seconds}s is below minimum, using ${minimumSeconds}s (${minimumSeconds / 60} minutes)`);
    return minimumSeconds;
  }
  
  return seconds;
}

// Marketplace-specific convenience functions
export const convertDurationForBlur = (duration: { value: number; unit: string }) =>
  convertDurationToSeconds(duration, { minimumSeconds: 900, marketplaceName: 'BLUR' });

export const convertDurationForOpenSea = (duration: { value: number; unit: string }) =>
  convertDurationToSeconds(duration, { minimumSeconds: 900, marketplaceName: 'OPENSEA' });

export const convertDurationForMagicEden = (duration: { value: number; unit: string }) =>
  convertDurationToSeconds(duration, { marketplaceName: 'MAGICEDEN' });

/**
 * Convert task bidDuration to milliseconds
 * Handles all units: minutes, hours, days
 *
 * @param bidDuration - Duration object from task
 * @returns Duration in milliseconds
 * @throws Error if unit is invalid or value is negative
 */
export function convertBidDurationToMs(
  bidDuration: { value: number; unit: string }
): number {
  if (bidDuration.value < 0) {
    throw new Error(`Invalid bid duration value: ${bidDuration.value} (must be >= 0)`);
  }

  switch (bidDuration.unit.toLowerCase()) {
    case 'minutes':
    case 'm':
      return bidDuration.value * 60 * 1000;
    case 'hours':
    case 'h':
      return bidDuration.value * 60 * 60 * 1000;
    case 'days':
    case 'd':
      return bidDuration.value * 24 * 60 * 60 * 1000;
    default:
      throw new Error(`Invalid bid duration unit: ${bidDuration.unit} (expected: minutes, hours, or days)`);
  }
}

/**
 * Convert task bidDuration to hours (for logsCache.setTaskTTL compatibility)
 */
export function convertBidDurationToHours(
  bidDuration: { value: number; unit: string }
): number {
  return convertBidDurationToMs(bidDuration) / (60 * 60 * 1000);
}

/**
 * Convert task bidDuration to seconds (for bid expiration calculations)
 */
export function convertBidDurationToSeconds(
  bidDuration: { value: number; unit: string }
): number {
  return convertBidDurationToMs(bidDuration) / 1000;
}