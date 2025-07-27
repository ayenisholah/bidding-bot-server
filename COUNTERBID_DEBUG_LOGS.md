# Counterbid Debug Logging Implementation

## Overview
Added comprehensive debug logging to all three counterbid handling functions to provide visibility into incoming bids and counterbid actions.

## Implementation Details

### Color Coding
- **OpenSea**: Blue (`\x1b[34m`)
- **Blur**: Gold (`\x1b[33m`)
- **MagicEden**: Magenta (`\x1b[35m`)
- **Success**: Green (`\x1b[32m`)

### Log Format

#### Incoming Bid Logs
All incoming bids are logged in a single line with marketplace-specific colors:

**MagicEden** (Magenta):
```
[MagicEden Counterbid] Task: {taskId} | Collection: {slug} | Type: {Token #123 or Collection} | Incoming: {eth} ETH (${usd}) | Maker: {0x1234...5678}
```

**OpenSea** (Blue):
```
[OpenSea Counterbid] Task: {taskId} | Collection: {slug} | Type: {Token #123 or Collection} | Incoming: {amount} {currency} ({converted}) | Quantity: {qty} | Maker: {0x1234...5678}
```

**Blur** (Gold):
```
[Blur Counterbid] Task: {taskId} | Collection: {slug} | Type: {criteriaType} | Incoming: {price} ETH | Criteria: {criteriaValue}
```

#### Success Logs
When we successfully prepare a counterbid, it's logged in green:

**Token Bids**:
```
[{Marketplace} Counterbid SUCCESS] Task: {taskId} | Placing bid: {amount} {currency} | Token: #{tokenId} | Outbidding: {incomingAmount} {currency}
```

**Trait Bids**:
```
[{Marketplace} Counterbid SUCCESS] Task: {taskId} | Placing bid: {amount} {currency} | Trait: {identifier} | Outbidding: {incomingAmount} {currency}
```

**Collection Bids**:
```
[{Marketplace} Counterbid SUCCESS] Task: {taskId} | Placing bid: {amount} {currency} | Collection{trait indicator} | Outbidding: {incomingAmount} {currency}
```

## Key Information Logged

1. **Task Identification**: Task ID and collection slug
2. **Bid Type**: Token (with ID), Trait (with identifier), or Collection
3. **Bid Amount**: Incoming bid amount with currency
4. **Maker Address**: Truncated for readability (first 6 and last 4 characters)
5. **Our Counterbid**: Amount we're placing to outbid
6. **Currency Conversion**: For OpenSea, shows both original and converted currencies

## Benefits

1. **Real-time Monitoring**: See all incoming bids across marketplaces
2. **Easy Marketplace Identification**: Color coding makes it instantly clear which marketplace the bid is from
3. **Success Tracking**: Green logs show when counterbids are successfully prepared
4. **Debug Information**: All relevant bid details in a single line for easy scanning
5. **Performance Analysis**: Can track bid patterns and counterbid frequency

## Usage

The logs will appear in the console output when running the server. To filter by marketplace:
- Search for `[MagicEden Counterbid]` for MagicEden bids
- Search for `[OpenSea Counterbid]` for OpenSea bids
- Search for `[Blur Counterbid]` for Blur bids
- Search for `SUCCESS` to see only successful counterbids

## Example Output

```
[35m[MagicEden Counterbid] Task: 507f1f77bcf86cd799439011 | Collection: azuki | Type: Token #1234 | Incoming: 2.5000 ETH ($4250.00) | Maker: 0x1234...5678[0m
[32m[MagicEden Counterbid SUCCESS] Task: 507f1f77bcf86cd799439011 | Placing bid: 2.5001 WETH | Token: #1234 | Outbidding: 2.5000 WETH[0m
[34m[OpenSea Counterbid] Task: 507f1f77bcf86cd799439012 | Collection: bayc | Type: Collection | Incoming: 0.15 WETH (0.15 WETH) | Quantity: 1 | Maker: 0xabcd...ef01[0m
[32m[OpenSea Counterbid SUCCESS] Task: 507f1f77bcf86cd799439012 | Placing bid: 0.1501 WETH | Collection | Outbidding: 0.1500 WETH[0m
[33m[Blur Counterbid] Task: 507f1f77bcf86cd799439013 | Collection: doodles | Type: trait | Incoming: 1.2 ETH | Criteria: {"type":"Background","value":"Blue"}[0m
[32m[Blur Counterbid SUCCESS] Task: 507f1f77bcf86cd799439013 | Placing bid: 1.21 ETH | Trait: Background:Blue | Outbidding: 1.2 ETH[0m
```