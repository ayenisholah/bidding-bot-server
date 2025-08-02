import { config } from 'dotenv';
import redisClient from '../src/utils/redis';

config();

async function clearBlurTokens() {
  try {
    console.log('Connecting to Redis...');
    const redis = redisClient.getClient();
    
    if (!redis) {
      console.error('Failed to connect to Redis');
      process.exit(1);
    }

    console.log('Searching for Blur access tokens...');
    const keys = await redis.keys('{blurAccessToken}:*');
    
    if (keys.length === 0) {
      console.log('No Blur access tokens found in cache');
      process.exit(0);
    }

    console.log(`Found ${keys.length} Blur access tokens`);
    console.log('Clearing tokens...');
    
    await redis.del(...keys);
    
    console.log(`✅ Successfully cleared ${keys.length} Blur authentication tokens`);
    console.log('All wallets will need to re-authenticate on next bid attempt');
    
    // Also reset rate limits for all wallets
    console.log('\nResetting rate limits...');
    const rateLimitKeys = await redis.keys('blur:ratelimit:*');
    if (rateLimitKeys.length > 0) {
      await redis.del(...rateLimitKeys);
      console.log(`✅ Cleared ${rateLimitKeys.length} rate limit entries`);
    }
    
    process.exit(0);
  } catch (error) {
    console.error('Error clearing Blur tokens:', error);
    process.exit(1);
  }
}

// Run the script
clearBlurTokens();