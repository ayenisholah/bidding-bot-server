# OpenSea 404 Error Fix

## Problem
OpenSea API was returning a 404 error when trying to fetch floor price for "proof-moonbirds" collection, and the error was not being properly logged to Axiom. The error was being caught by the lock manager and logged as a database error instead of an API error.

## Error Details
- **Endpoint**: `https://nfttools.pro/kong/opensea/api/v2/listings/collection/proof-moonbirds/best?limit=1`
- **Status Code**: 404
- **Error Message**: Collection not found

## Root Cause
The collection "proof-moonbirds" might have been:
1. Delisted from OpenSea
2. Renamed on OpenSea
3. Temporarily unavailable

The error handling was inadequate - it was letting the error bubble up to the lock manager which logged it as a database operation error.

## Solution Implemented

### 1. Added Proper Error Handling in getOpenseaFloor
In `src/functions/collection.ts`, wrapped the API call in a try-catch block to handle errors at the source:

```typescript
try {
  const { data } = await limiter.schedule(() => axiosInstance.get<ListingsResponse>(
    `${BASE_URL}/opensea/api/v2/listings/collection/${collectionSlug}/best?limit=1`,
    {
      headers: { 'x-api-key': API_KEY }
    }
  ));
  // ... process data
} catch (error: any) {
  // Handle 404 specifically
  if (error?.response?.status === 404) {
    await logger.logOpenseaCollectionError('Collection not found on OpenSea', {
      collection: collectionSlug,
      statusCode: 404,
      errorCode: 'COLLECTION_NOT_FOUND',
      // ... other context
    });
    
    // Cache zero floor price for 5 minutes to avoid repeated 404s
    if (getRedis()) {
      await getRedis().set(cacheKey, JSON.stringify(0), 'EX', 300);
    }
    return 0; // Return 0 floor price for non-existent collections
  }
  
  // Handle other errors
  await logger.logApiError('Failed to fetch OpenSea floor price', {
    // ... error context
  });
  
  throw error;
}
```

### 2. Benefits of the Fix

1. **Proper Error Categorization**: 404 errors are now logged as `OPENSEA_COLLECTION` errors instead of database errors
2. **Better Context**: The error log includes the collection slug, endpoint, status code, and operation
3. **Performance Optimization**: 404 responses are cached for 5 minutes to avoid repeated failed requests
4. **Graceful Degradation**: Returns 0 floor price for non-existent collections instead of throwing
5. **Axiom Integration**: Errors are properly formatted and sent to Axiom with the correct category

### 3. Error Logging Format
The error will now be logged to Axiom with:
- **Category**: `MARKETPLACE` 
- **Subcategory**: `OPENSEA` / `COLLECTION`
- **Message**: Formatted as `USERNAME_SERVER_MARKETPLACE_OPENSEA_COLLECTION_COLLECTION_NOT_FOUND`
- **Context**: Includes collection slug, endpoint, status code, error message, etc.

## Monitoring
To monitor these errors in Axiom:
1. Filter by `category: "MARKETPLACE"`
2. Filter by `marketplace: "opensea"`
3. Filter by `errorCode: "COLLECTION_NOT_FOUND"`

This will help identify collections that are consistently returning 404s and may need to be removed from the system.