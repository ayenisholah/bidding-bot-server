/**
 * Safely converts a token ID from any format (number, string, BigInt, scientific notation)
 * to a proper string representation suitable for BigInt parsing.
 *
 * @param tokenId - The token ID to convert (can be number, string, or BigInt)
 * @returns String representation of the token ID
 * @throws Error if tokenId is null, undefined, or invalid
 */
export function safeTokenIdToString(tokenId: any): string {
  // Handle null/undefined
  if (tokenId === null || tokenId === undefined) {
    throw new Error('Token ID cannot be null or undefined');
  }

  // Handle BigInt - convert directly to string
  if (typeof tokenId === 'bigint') {
    return tokenId.toString();
  }

  // Handle string
  if (typeof tokenId === 'string') {
    // Check for scientific notation in string (e.g., "1.091603475270973e+77")
    if (/e[+-]?\d+/i.test(tokenId)) {
      return convertScientificNotationToString(tokenId);
    }
    // Already a regular numeric string
    return tokenId;
  }

  // Handle number
  if (typeof tokenId === 'number') {
    // Check if in scientific notation
    const asString = tokenId.toString();
    if (/e[+-]?\d+/i.test(asString)) {
      return convertScientificNotationToString(asString);
    }
    // Safe integer, return as string
    return asString;
  }

  throw new Error(`Invalid token ID type: ${typeof tokenId}`);
}

/**
 * Converts a scientific notation string to a proper integer string.
 *
 * @param scientificStr - String in scientific notation (e.g., "1.23e+5")
 * @returns Proper integer string (e.g., "123000")
 */
function convertScientificNotationToString(scientificStr: string): string {
  try {
    // Parse scientific notation manually to avoid precision loss
    const [mantissaStr, exponentStr] = scientificStr.toLowerCase().split('e');
    const exponent = parseInt(exponentStr, 10);

    // Remove decimal point from mantissa
    const [intPart, fracPart = ''] = mantissaStr.split('.');
    const mantissa = intPart + fracPart;

    // Calculate the decimal position adjustment
    const decimalOffset = fracPart.length;
    const totalShift = exponent - decimalOffset;

    if (totalShift >= 0) {
      // Positive exponent: add zeros to the right
      return mantissa + '0'.repeat(totalShift);
    } else {
      // Negative exponent: this shouldn't happen for token IDs (they should be integers)
      // But handle it just in case by truncating decimals
      const absShift = Math.abs(totalShift);
      if (absShift >= mantissa.length) {
        return '0'; // Number is less than 1
      }
      return mantissa.slice(0, mantissa.length - absShift);
    }
  } catch (error) {
    throw new Error(`Failed to convert scientific notation "${scientificStr}": ${error instanceof Error ? error.message : String(error)}`);
  }
}
