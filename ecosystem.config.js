// ecosystem.config.js
module.exports = {
  apps: [{
    name: "playwright-broker",
    script: "server.js",
    exec_mode: "cluster",
    instances: "max",
    max_memory_restart: "900M",
    watch: false,
    env: {
      NODE_ENV: "production",
      PORT: 9080,
      BASE_PATH: "/playwright-9080",
      // CHROME_PATH: "/opt/google/chrome/google-chrome",
      MAX_BROWSERS: "350",
      BROWSER_TTL_MS: "60000",
      IDLE_TIMEOUT_MS: "30000",
      PING_INTERVAL_MS: "20000",
      API_KEY: "tokenforthescrapingdogbrowserforautomation@123",
      EXTRA_ARGS: "--disable-dev-shm-usage --no-sandbox --disable-gpu --disable-blink-features=AutomationControlled",
      REDIS_URL: "rediss://default:<PASSWORD>@<HOST>:<PORT>/0",
      REDIS_NAMESPACE: "broker:prod",
      LOG_LEVEL: "info"
      // DISPLAY is now set dynamically per browser in server.js
    }
  }]
}