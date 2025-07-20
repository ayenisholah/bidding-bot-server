import { logger, ErrorCategory } from './logger';

export interface OpenSeaError {
  response?: {
    status: number;
    data?: {
      errors?: Array<{
        message?: string;
        field?: string;
        code?: string;
      }>;
    };
  };
  message: string;
}

export function formatOpenSeaError(error: OpenSeaError): string {
  // Check if it's an OpenSea API error with specific error messages
  if (error.response?.data?.errors && Array.isArray(error.response.data.errors)) {
    const apiErrors = error.response.data.errors;
    
    // Extract the first error message, which is usually the most relevant
    const primaryError = apiErrors[0];
    if (primaryError?.message) {
      // Return just the message without the full error object
      return primaryError.message;
    }
  }
  
  // Fallback to the generic error message
  return error.message || 'Unknown OpenSea error';
}

export function isOpenSeaTradingDisabledError(error: OpenSeaError): boolean {
  if (!error.response?.data?.errors) return false;
  
  return error.response.data.errors.some(e => 
    e.message?.toLowerCase().includes('trading is disabled')
  );
}

export function logOpenSeaError(
  message: string,
  error: OpenSeaError,
  context: any = {}
): void {
  const formattedError = formatOpenSeaError(error);
  
  logger.logError(ErrorCategory.OPENSEA_BID, message, {
    ...context,
    error: formattedError,
    status: error.response?.status,
    fullError: process.env.NODE_ENV === 'development' ? error : undefined
  });
  
  // Log the specific formatted error to console
  console.error(`[ERROR] ${message}: ${formattedError}`);
}