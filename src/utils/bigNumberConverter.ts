/**
 * Converts BigNumber objects from ethers v5 to bigint for ethers v6 compatibility
 */
export function convertBigNumbers(obj: any): any {
  if (obj === null || obj === undefined) return obj;
  
  // Check if it's a BigNumber object (from ethers v5)
  if (obj.type === 'BigNumber' && obj.hex) {
    return BigInt(obj.hex);
  }
  
  // Handle arrays
  if (Array.isArray(obj)) {
    return obj.map(convertBigNumbers);
  }
  
  // Handle objects recursively
  if (typeof obj === 'object') {
    const converted: any = {};
    for (const key in obj) {
      converted[key] = convertBigNumbers(obj[key]);
    }
    return converted;
  }
  
  // Return primitive values as-is
  return obj;
}