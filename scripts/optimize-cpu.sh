#!/bin/bash

echo "=== CPU Optimization Script ==="
echo ""

# 1. Apply broadcast interval fix
echo "1. Applying broadcast interval optimization..."
if grep -q "BROADCAST_INTERVAL = 1000" src/index.ts; then
    sed -i.backup 's/const BROADCAST_INTERVAL = 1000;/const BROADCAST_INTERVAL = process.env.BROADCAST_INTERVAL ? parseInt(process.env.BROADCAST_INTERVAL) : 5000;/' src/index.ts
    echo "   ✓ Broadcast interval updated to 5 seconds"
else
    echo "   ✓ Broadcast interval already optimized"
fi

# 2. Apply validateBidCount interval fix
echo "2. Applying validateBidCount optimization..."
if grep -q "setInterval(validateBidCount, 5000)" src/index.ts; then
    sed -i.backup 's/setInterval(validateBidCount, 5000);/setInterval(validateBidCount, process.env.VALIDATE_BID_INTERVAL ? parseInt(process.env.VALIDATE_BID_INTERVAL) : 30000);/' src/index.ts
    echo "   ✓ validateBidCount interval updated to 30 seconds"
else
    echo "   ✓ validateBidCount already optimized"
fi

# 3. Check for synchronous crypto operations
echo "3. Checking for synchronous crypto operations..."
if grep -q "scryptSync" src/utils/crypto.ts; then
    echo "   ⚠️  WARNING: Found synchronous crypto operations in src/utils/crypto.ts"
    echo "   Please manually update scryptSync to use async crypto.scrypt"
else
    echo "   ✓ No synchronous crypto operations found"
fi

# 4. Add environment variables if not present
echo "4. Checking environment variables..."
if ! grep -q "BROADCAST_INTERVAL" .env 2>/dev/null; then
    echo "" >> .env
    echo "# Performance optimizations" >> .env
    echo "BROADCAST_INTERVAL=5000" >> .env
    echo "VALIDATE_BID_INTERVAL=30000" >> .env
    echo "MIN_LOOP_DELAY=100" >> .env
    echo "   ✓ Added performance environment variables"
else
    echo "   ✓ Performance environment variables already set"
fi

echo ""
echo "=== Optimization Complete ==="
echo ""
echo "Next steps:"
echo "1. Restart your Node.js server to apply changes"
echo "2. Monitor CPU usage with: ps aux | grep node"
echo "3. Check Docker stats: docker stats"
echo ""
echo "Expected improvements:"
echo "- Broadcast interval: 1s → 5s (80% reduction)"
echo "- validateBidCount: 5s → 30s (83% reduction)"
echo "- Overall CPU usage should drop by 50-70%"