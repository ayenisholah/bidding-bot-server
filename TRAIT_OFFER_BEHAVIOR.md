# Trait Offer Behavior Documentation

## Overview
This document explains how the bidding bot handles trait offers for different task types.

## Task Types and Their Behavior

### 1. Token BidType with Selected Traits
- **Purpose**: Bid on specific tokens that have certain traits
- **Behavior on trait_offer event**: 
  - Fetches all tokens with the matching trait
  - Places individual token bids on each matching token
  - Does NOT place a trait offer

### 2. Collection BidType with Selected Traits  
- **Purpose**: Place trait offers on the collection for specific traits
- **Behavior on trait_offer event**:
  - Places a trait offer for the matching trait
  - Does NOT place individual token bids

### 3. Collection BidType without Traits
- **Purpose**: Place collection-wide offers
- **Behavior on collection_offer event**:
  - Places a collection offer
  
## Why This Separation?

When multiple tasks exist for the same collection:
- Each task type serves a different bidding strategy
- Prevents duplicate trait offers when multiple tasks are active
- Allows users to choose between:
  - Targeting specific tokens with traits (token bidType)
  - Placing trait-based collection offers (collection bidType)

## Example Scenario

If you have:
- Task A: `bidType: "token"`, selectedTraits: `{"Head": ["Horn"]}`
- Task B: `bidType: "collection"`, selectedTraits: `{"Head": ["Horn"]}`

When a trait offer for Head:Horn comes in:
- Task A will place bids on each individual token with Head:Horn
- Task B will place a trait offer for Head:Horn
- No duplication occurs

## Implementation Details

The logic is in `handleOpenseaCounterbid()`:
- For `data.event === "trait_offer"`:
  - If `task.bidType === "token"`: Places token bids
  - If `task.bidType === "collection"`: Places trait offer
- For `data.event === "collection_offer"`:
  - Only processes if `task.bidType === "collection"` AND no traits selected