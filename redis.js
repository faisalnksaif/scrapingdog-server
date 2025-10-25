// redis.js — shared Redis Cloud client for the broker (ioredis + TLS)
// Usage: const { redis, prefix, closeRedis } = require('./redis');

require('dotenv').config({ path: './config.env' });
const Redis = require('ioredis');

// Prefer REDIS_URL (e.g. rediss://default:<password>@<host>:<port/0>)
// Else fall back to discrete vars (TLS on by default for Redis Cloud).
const {
  REDIS_HOST,
  REDIS_PORT,
  REDIS_PASSWORD,
  REDIS_NAMESPACE = 'broker:prod' // namespace to avoid key collisions across envs
} = process.env;

let redis;

// Discrete config (Redis Cloud) with proper pooling and connection management
redis = new Redis({
  host: REDIS_HOST,
  port: Number(REDIS_PORT),
  password: REDIS_PASSWORD,
  
  // Connection pooling and management
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
  enableOfflineQueue: true,
  
  // Retry strategy to prevent connection spam
  retryStrategy(times) {
    const delay = Math.min(times * 50, 2000);
    console.log(`[redis] retry attempt ${times}, waiting ${delay}ms`);
    return delay;
  },
  
  // Reconnect strategy
  reconnectOnError(err) {
    const targetError = 'READONLY';
    if (err.message.includes(targetError)) {
      // Only reconnect when the error contains "READONLY"
      return true; // or `return 1;`
    }
    return false;
  },
  
  // Keep connection alive
  keepAlive: 30000,
  
  // Prevent connection timeout issues
  connectTimeout: 10000,
  
  // Auto-pipelining for better performance
  enableAutoPipelining: true,
  autoPipeliningIgnoredCommands: [],
  
  // Connection name for debugging
  connectionName: `broker-${process.pid}`,
  
  // Lazy connect disabled to fail fast if config is wrong
  lazyConnect: false
});

redis.on('connect', () => console.log('[redis] connect'));
redis.on('ready',   () => console.log('[redis] ready'));
redis.on('error',   (e) => console.error('[redis] error:', e.message));
redis.on('close',   () => console.warn('[redis] close'));
redis.on('reconnecting', (d) => console.log('[redis] reconnecting in', d, 'ms'));

function prefix(k) {
  return `${REDIS_NAMESPACE}:${k}`;
}

async function closeRedis() {
  console.log('[redis] shutting down gracefully...');
  try { 
    await redis.quit(); 
    console.log('[redis] closed cleanly');
  } catch (e) { 
    console.error('[redis] error during quit:', e.message);
    try { 
      await redis.disconnect(); 
      console.log('[redis] disconnected forcefully');
    } catch {} 
  }
}

module.exports = { redis, prefix, closeRedis };