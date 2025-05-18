import redisClient from "./redis";

const OFFER_AMOUNT_TTL_SEC = 60 * 5; // Optional TTL of 5 minutes

const redis = redisClient.getClient()

export async function getCachedOfferAmount(OFFER_AMOUNT_KEY: string): Promise<number> {
  const value = await redis.get(OFFER_AMOUNT_KEY);
  // check the database value if this is 0
  return value ? parseFloat(value) : 0;
}

export async function setOfferAmount(OFFER_AMOUNT_KEY: string, amount: number) {
  await redis.setex(OFFER_AMOUNT_KEY, amount.toString(), OFFER_AMOUNT_TTL_SEC);
}

export async function incrementOfferAmount(OFFER_AMOUNT_KEY: string, by: number) {
  await redis.incrbyfloat(OFFER_AMOUNT_KEY, by);
}

export async function decrementOfferAmount(OFFER_AMOUNT_KEY: string, by: number) {
  await redis.incrbyfloat(OFFER_AMOUNT_KEY, -by);
}

export async function clearOfferAmountCache(OFFER_AMOUNT_KEY: string) {
  await redis.del(OFFER_AMOUNT_KEY);
}