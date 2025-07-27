# Task Preprocessing Refactor

## Summary

Moved all bid type determination and job counting logic from individual `processScheduledBid` functions to the `startTask` function. This ensures consistent job creation across all marketplaces.

## Problem

Previously, each marketplace's `processScheduledBid` function independently determined:
- Whether to create token, trait, or collection bids
- How many jobs to create
- How to transform traits

This led to inconsistencies where:
- OpenSea created 454 trait bid jobs (correct)
- Blur created only 1 job (transformBlurTraits returned 1 combined trait)
- MagicEden created only 1 job (fell back to collection bid when no tokens found)

## Solution

### 1. Created TaskPreprocessing Interface

```typescript
interface TaskPreprocessing {
  bidType: 'token' | 'trait' | 'collection';
  opensea: {
    enabled: boolean;
    traits: { type: string; value: string }[];
    tokens: number[];
    jobCount: number;
    skipReason?: string;
  };
  blur: {
    enabled: boolean;
    traits: { [key: string]: string }[];
    tokens: number[];
    jobCount: number;
    skipReason?: string;
  };
  magiceden: {
    enabled: boolean;
    traits: { attributeKey: string; attributeValue: string }[];
    tokens: number[];
    jobCount: number;
    skipReason?: string;
  };
}
```

### 2. Added Preprocessing to startTask

The `startTask` function now:
1. Determines bid type once for all marketplaces
2. Fetches tokens upfront if needed
3. Transforms traits for each marketplace
4. Calculates exact job counts
5. Sets up parentJobs with accurate counts
6. Stores results in `taskPreprocessing` Map

### 3. Simplified processScheduledBid Functions

Each `processScheduledBid` function now:
1. Retrieves preprocessing data
2. Creates jobs based on preprocessed counts
3. No longer needs to determine bid types
4. No longer needs to fetch tokens
5. No longer needs to set up parentJobs

## Key Changes

### startTask Function
- Added preprocessing logic after marketplace approvals
- Clears parentJobs at start of each loop
- Fetches tokens for all marketplaces in parallel
- Handles trait-based token fetching for token bid type
- Sets up parentJobs with accurate totalJobs count

### processOpenseaScheduledBid
- Removed trait transformation logic
- Removed token fetching logic
- Removed parentJobs setup
- Uses preprocessing.opensea data

### processBlurScheduledBid
- Removed trait transformation logic
- Removed token bid check (handled in preprocessing)
- Removed parentJobs setup
- Uses preprocessing.blur data

### processMagicedenScheduledBid
- Removed trait transformation logic
- Removed token fetching logic
- Removed parentJobs setup
- Uses preprocessing.magiceden data

## Benefits

1. **Consistency**: All marketplaces create the same number of jobs
2. **Accuracy**: Running percentage calculation is accurate from the start
3. **Performance**: Token fetching happens once, not per marketplace
4. **Maintainability**: Bid type logic centralized in one place
5. **Debugging**: Preprocessing logs show exactly what will happen

## Edge Cases Handled

1. **Blur Token Bids**: Marked as skipped during preprocessing
2. **MagicEden Empty Tokens**: Falls back to collection bid gracefully
3. **OpenSea Trait Support**: Checks collection.trait_offers_enabled
4. **Currency Validation**: Blur WETH check remains in processBlurScheduledBid

## Example Output

```
[Preprocessing] Starting task preprocessing for 507f1f77bcf86cd799439011
[Preprocessing] OpenSea will create 454 trait bid jobs
[Preprocessing] Blur will create 454 trait bid jobs
[Preprocessing] MagicEden will create 454 trait bid jobs
[Parent Job Created] OpenSea | Task: 507f1f77bcf86cd799439011 | Total Jobs: 454
[Parent Job Created] Blur | Task: 507f1f77bcf86cd799439011 | Total Jobs: 454
[Parent Job Created] MagicEden | Task: 507f1f77bcf86cd799439011 | Total Jobs: 454
[Preprocessing] Complete for task 507f1f77bcf86cd799439011 - Total jobs across all marketplaces: 1362
  OpenSea: 454 jobs
  Blur: 454 jobs
  MagicEden: 454 jobs
```

## Bug Fixes

### Fixed "Cannot convert undefined or null to object" Error

The trait bid functions were trying to access `currentTraits` which wasn't populated after moving preprocessing. Fixed by:

1. **Removed trait validation** from `processMagicedenTraitBid`, `processOpenseaTraitBid`, and `processBlurTraitBid`
   - The traits passed in job data are already validated during preprocessing
   - No need to check if trait exists in selected traits

2. **Added backwards compatibility** in preprocessing
   - Populates `currentTraits` from preprocessed data
   - Converts trait arrays back to the expected format
   - Ensures other functions that rely on `currentTraits` continue to work