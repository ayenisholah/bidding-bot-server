# Collection Offer Response Feature - FIXED

## Overview
This feature allows token and trait tasks to respond to incoming collection offers by counterbidding ONLY the offers that are being outbid (≤ incoming offer), while leaving higher offers untouched.

## Critical Fix
The previous implementation had a dangerous bug where it would skip ALL counterbids if ANY existing offer was higher than the incoming offer. This has been fixed.

## How It Works

### Previous (Buggy) Behavior
- System would check if ANY existing offer > incoming collection offer
- If true, it would skip ALL counterbids, missing opportunities to update lower offers
- Example: If you had offers at 0.1, 0.2, and 0.3 ETH, and incoming was 0.25 ETH, it would skip everything

### New (Fixed) Behavior
- System processes EACH existing offer individually
- For each offer ≤ incoming collection offer: counterbid it
- For each offer > incoming collection offer: leave it untouched
- Example: If you have offers at 0.1, 0.2, and 0.3 ETH, and incoming is 0.25 ETH:
  - Counterbids 0.1 ETH token offer → ~0.26 ETH
  - Counterbids 0.2 ETH token offer → ~0.26 ETH
  - Leaves 0.3 ETH token offer untouched

## Implementation Details

### OpenSea Handler
```typescript
// Process each existing order
for (const order of allTaskOrders) {
  // Only counterbid orders that are <= incoming collection offer
  if (Number(order.offerPrice) <= incomingBidAmount) {
    if (order.identifier.includes(':')) {
      // Queue trait counterbid
      await queue.add(OPENSEA_TRAIT_BID_COUNTERBID, ...);
    } else if (order.identifier === 'collection') {
      // Queue collection counterbid
      await queue.add(OPENSEA_COLLECTION_BID_COUNTERBID, ...);
    } else {
      // Queue token counterbid
      await queue.add(OPENSEA_TOKEN_BID_COUNTERBID, ...);
    }
  }
}
```

### MagicEden Handler
- Same logic with proper handling for USDC/ETH currency
- Queues appropriate counterbid types: MAGICEDEN_TOKEN_BID_COUNTERBID, MAGICEDEN_TRAIT_BID_COUNTERBID, MAGICEDEN_COLLECTION_BID_COUNTERBID
- Cancels only the orders being counterbid

### Blur Handler
- Same logic with wei conversion
- Note: Blur doesn't support individual token bids, only collection and trait offers
- Queues: BLUR_TRAIT_BID_COUNTERBID, BLUR_COLLECTION_BID_COUNTERBID

## Use Case Examples

### Example 1: Mixed Token Offers
- Task: Token bidType with tokenIds [1, 2, 3]
- Existing offers:
  - Token #1: 0.1 ETH
  - Token #2: 0.2 ETH
  - Token #3: 0.3 ETH
- Incoming collection offer: 0.25 ETH
- Result:
  - Counterbids Token #1 and #2 (both ≤ 0.25)
  - Leaves Token #3 unchanged (0.3 > 0.25)

### Example 2: Multiple Trait Offers
- Task: Has multiple trait offers
- Existing offers:
  - Head:Horn: 0.4 ETH
  - Body:Gold: 0.5 ETH
  - Eyes:Laser: 0.6 ETH
- Incoming collection offer: 0.55 ETH
- Result:
  - Counterbids Head:Horn and Body:Gold traits
  - Leaves Eyes:Laser unchanged

### Example 3: Collection + Token Mix
- Task: Has both collection and token offers
- Existing offers:
  - Collection offer: 0.2 ETH
  - Token #5: 0.25 ETH
  - Token #10: 0.3 ETH
- Incoming collection offer: 0.28 ETH
- Result:
  - Counterbids collection offer and Token #5
  - Leaves Token #10 unchanged

## Important Notes
- Each offer is evaluated independently
- Only offers ≤ incoming are counterbid
- The system queues appropriate job types based on offer type
- All existing safeguards (min/max price, stop options) still apply
- Outbid margins are calculated based on the incoming offer price