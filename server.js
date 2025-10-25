// server.js — Redis-backed broker (multi-worker safe)
require("dotenv").config({ path: "./config.env" });
// const playwright = require("playwright");
const { chromium } = require('playwright-extra')
const { createServer } = require("http");
const WebSocket = require("ws");
const { execSync } = require("child_process");
const { redis, prefix, closeRedis } = require("./redis");

// const stealth = require('puppeteer-extra-plugin-stealth')()
// chromium.use(stealth);

const K_PORT_RELEASED_AT = prefix("port-released");
const { promisify } = require('util');

// ------- env -------
const MIN_PORT = 40000;
const MAX_PORT = 45000;

const PORT = Number(process.env.PORT || 9080);
const MAX_BROWSERS = Number(process.env.MAX_BROWSERS || 350);
console.log(MAX_BROWSERS, "MAX_BROWSERS on starting");
console.log(PORT, "PORT on starting");
const BROWSER_TTL_MS = Number(process.env.BROWSER_TTL_MS || 60000);
const IDLE_TIMEOUT_MS = Number(process.env.IDLE_TIMEOUT_MS || 10000); // 10 seconds
const API_KEY = process.env.API_KEY || "";
const EXTRA_ARGS = (
  process.env.EXTRA_ARGS ||
  "--disable-blink-features=AutomationControlled"
)
  .split(" ")
  .filter(Boolean);
const BASE_PATH = (process.env.BASE_PATH || "/").replace(/\/+$/, "");

// ------- redis keys -------
const K_COUNT = prefix("count");
const K_SET = prefix("ids");
const K_INFO = (id) => prefix(`b:${id}`);
const K_LOCK = (id) => prefix(`lock:${id}`);
const K_PORTS = prefix("ports"); // Redis Set for tracking used ports
const K_PORT_LOCK = prefix("port-lock"); // Lock for port allocation

// Local map only keeps handles for browsers launched by THIS worker
const local = new Map();

// Simple touch throttle
const lastTouch = new Map();
const TOUCH_MIN_MS = Number(process.env.TOUCH_MIN_MS || 3000);

const LOG = (...a) => console.log(...a);
const now = () => Date.now();

// ------- Xvfb Display Management (Auto-start on demand) -------
const { exec } = require('child_process');
const execAsync = promisify(exec);

// Track which displays are running (per worker)
const runningDisplays = new Set();

// Check if display is running
async function isDisplayRunning(displayNum) {
  try {
    const { stdout } = await execAsync(`ps aux | grep "Xvfb :${displayNum} " | grep -v grep`);
    return stdout.trim().length > 0;
  } catch {
    return false;
  }
}

// Ensure Xvfb display exists, start if needed
async function ensureDisplay(displayNum) {
  // Already tracking it in this worker
  if (runningDisplays.has(displayNum)) {
    return true;
  }

  // Check if running on system
  if (await isDisplayRunning(displayNum)) {
    runningDisplays.add(displayNum);
    LOG(`[Xvfb] Display :${displayNum} already running`);
    return true;
  }

  // Start new display
  try {
    LOG(`[Xvfb] Starting display :${displayNum}...`);

    // Spawn Xvfb in background with high client limit
    // Each Chrome creates 10-30+ connections, so set very high limit
    const xvfbProcess = exec(
      `Xvfb :${displayNum} -screen 0 1920x1080x24 -nolisten tcp -ac +extension GLX +render -noreset -maxclients 2048 > /dev/null 2>&1 &`
    );

    // Don't wait for it to exit
    xvfbProcess.unref();

    // Give it time to start
    await new Promise(r => setTimeout(r, 1000));

    // Verify it started
    if (await isDisplayRunning(displayNum)) {
      runningDisplays.add(displayNum);
      LOG(`[Xvfb] Display :${displayNum} started successfully`);
      return true;
    } else {
      LOG(`[Xvfb] Failed to verify display :${displayNum}`);
      return false;
    }
  } catch (err) {
    LOG(`[Xvfb] Error starting display :${displayNum}:`, err.message);
    return false;
  }
}

async function withLock(id, ttlMs, fn) {
  const token = `${process.pid}-${Math.random().toString(36).slice(2)}`;
  const ok = await redis.set(K_LOCK(id), token, "NX", "PX", ttlMs);
  if (!ok) return false;
  try {
    await fn();
  } finally {
    const val = await redis.get(K_LOCK(id));
    if (val === token) await redis.del(K_LOCK(id));
  }
  return true;
}

function killPort(port) {
  try {
    execSync(`fuser -k ${port}/tcp`, { stdio: "ignore" });
    console.log(`[CLEANUP] Killed process using port ${port}`);
  } catch (_) {
    // no process on that port
  }
}

// Redis-based port allocation (multi-worker safe)
async function getAvailablePort() {
  const PORT_RANGE = MAX_PORT - MIN_PORT + 1;

  // Randomize start point so all workers don't hit same port first
  const start = MIN_PORT + Math.floor(Math.random() * PORT_RANGE);
  const checked = new Set();

  while (checked.size < PORT_RANGE) {
    const port = MIN_PORT + ((start + checked.size) % PORT_RANGE);
    checked.add(port);

    const lockKey = `${K_PORT_LOCK}:${port}`;
    const acquired = await redis.set(lockKey, 1, 'NX', 'PX', BROWSER_TTL_MS + 10000);
    if (!acquired) continue;

    const inUse = await redis.sismember(K_PORTS, port);
    if (inUse) {
      await redis.del(lockKey);
      continue;
    }

    const recentlyReleased = await redis.exists(`${K_PORT_RELEASED_AT}:${port}`);
    if (recentlyReleased) {
      await redis.del(lockKey);
      continue;
    }

    // 🔥 Non-blocking port check (async)
    try {
      await execAsync(`lsof -ti :${port}`);
      // Port is in use
      await redis.del(lockKey);
      continue;
    } catch {
      // Port is free
      await redis.sadd(K_PORTS, port);
      console.log(`[DEBUG] Allocated port ${port}`);
      return port;
    }
  }

  // Fallback: try cleanup once
  console.warn('[PORT] No ports available - attempting cleanup...');
  await fullPortCleanup();

  // Retry one random port
  for (let i = 0; i < 10; i++) {
    const port = MIN_PORT + Math.floor(Math.random() * PORT_RANGE);
    const lockKey = `${K_PORT_LOCK}:${port}`;
    const acquired = await redis.set(lockKey, 1, 'NX', 'PX', BROWSER_TTL_MS + 10000);
    if (!acquired) continue;

    const inUse = await redis.sismember(K_PORTS, port);
    if (!inUse) {
      await redis.sadd(K_PORTS, port);
      console.log(`[DEBUG] Allocated port ${port} after cleanup`);
      return port;
    }
  }

  throw new Error("No available ports even after cleanup");
}




async function releasePort(port) {
  try {
    await redis.srem(K_PORTS, port);
    await redis.set(`${K_PORT_RELEASED_AT}:${port}`, Date.now(), "EX", 120);
    await redis.del(`${K_PORT_LOCK}:${port}`);
    console.log(`[DEBUG] Released port ${port} from Redis`);
  } catch (e) {
    console.warn("[WARN] Failed to release port:", port, e.message);
  }
}



async function saveInfo(id, info) {
  await redis.set(K_INFO(id), JSON.stringify(info), "PX", IDLE_TIMEOUT_MS);
  await redis.sadd(K_SET, id);
}

async function loadInfo(id) {
  const s = await redis.get(K_INFO(id));
  return s ? JSON.parse(s) : null;
}

async function touch(id) {
  const t = now();
  const last = lastTouch.get(id) || 0;
  if (t - last < TOUCH_MIN_MS) return;
  lastTouch.set(id, t);

  try {
    const info = await loadInfo(id);
    if (info) {
      info.lastActiveAt = t;
      await redis.set(K_INFO(id), JSON.stringify(info), "PX", IDLE_TIMEOUT_MS);
    }
  } catch { }
}

async function decCountSafe() {
  const n = await redis.decr(K_COUNT);
  if (n < 0) await redis.set(K_COUNT, 0);
}

async function closeById(id, reason = "close") {
  await withLock(id, 15000, async () => {
    const info = await loadInfo(id);
    let portToRelease = null;

    // 1. Try to close from local map first (if this worker owns it)
    const localEntry = local.get(id);
    if (localEntry?.server) {
      portToRelease = localEntry.port;
      try {
        await localEntry.server.close();
      } catch (e) {
        console.warn(`[WARN] Failed to close local server:`, e.message);
      }
      local.delete(id);
    }

    // 2. Extract port from Redis info if we don't have it yet
    if (!portToRelease && info?.wsEndpoint) {
      const portMatch = info.wsEndpoint.match(/:(\d+)\//);
      if (portMatch) {
        portToRelease = parseInt(portMatch[1]);
      }
    }

    // 3. Try to close the browser via connect (best-effort)
    if (info?.wsEndpoint) {
      try {
        const browser = await chromium.connect(info.wsEndpoint, {
          timeout: 5000, // Reduced timeout
        });
        await browser.close();
      } catch (e) {
        console.warn(`[WARN] Failed to connect/close browser ${id}:`, e.message);
      }
    }

    // 4. ALWAYS release the port, regardless of errors above
    if (portToRelease) {
      await releasePort(portToRelease);
      console.log(`[DEBUG] Released port ${portToRelease} for browser ${id}`);
    }

    // 5. Clean up Redis
    await redis.del(K_INFO(id));
    await redis.srem(K_SET, id);
    await decCountSafe();
    lastTouch.delete(id);

    LOG(`[broker] closed ${id} (${reason})`);
  });
}

// Hard TTL janitor - RUN MORE FREQUENTLY
setInterval(async () => {
  try {
    const ids = await redis.smembers(K_SET);
    const t = now();

    for (const id of ids) {
      const info = await loadInfo(id);

      if (!info) {
        // Ghost browser - clean it up immediately
        const localEntry = local.get(id);
        if (localEntry?.server) {
          try {
            await localEntry.server.close();
            if (localEntry.port) {
              await releasePort(localEntry.port);
            }
          } catch { }
          local.delete(id);
        }
        await redis.srem(K_SET, id);
        await decCountSafe();
        LOG(`[broker] reaped ghost ${id}`);
        continue;
      }

      const last = info.lastActiveAt || info.createdAt || 0;
      if (t - last > IDLE_TIMEOUT_MS) {
        await closeById(id, "idle");
        continue;
      }

      if (t - (info.createdAt || 0) > BROWSER_TTL_MS) {
        await closeById(id, "ttl");
      }
    }

    // ADDED: Also clean up orphaned ports immediately
    const portsInRedis = await redis.smembers(K_PORTS);
    const activePorts = new Set();
    for (const id of ids) {
      const info = await loadInfo(id);
      if (info?.wsEndpoint) {
        const portMatch = info.wsEndpoint.match(/:(\d+)\//);
        if (portMatch) activePorts.add(portMatch[1]);
      }
    }

    const orphanedPorts = portsInRedis.filter(p => !activePorts.has(p));
    if (orphanedPorts.length > 0) {
      LOG(`[janitor] Found ${orphanedPorts.length} orphaned ports, releasing...`);
      for (const port of orphanedPorts) {
        await releasePort(parseInt(port));
        LOG(`[janitor] Released orphaned port ${port}`);
      }
    }
  } catch (e) {
    LOG("[broker] janitor error", e.message);
  }
}, 5000); // Run every 5 seconds

// ADDED: Aggressive zombie Chrome killer - runs less frequently
// Kills Chrome processes older than MAX_BROWSER_AGE that aren't in Redis
setInterval(async () => {
  try {
    const MAX_BROWSER_AGE_MS = BROWSER_TTL_MS * 2; // 2x TTL = definitely a zombie
    const currentIds = await redis.smembers(K_SET);

    // Get all tracked profile IDs from Redis
    const trackedProfiles = new Set();
    for (const id of currentIds) {
      const info = await loadInfo(id);
      if (info?.wsEndpoint) {
        // Extract profile ID from user-data-dir in process
        const match = info.wsEndpoint.match(/playwright_chromiumdev_profile-(\w+)/);
        if (match) trackedProfiles.add(match[1]);
      }
    }

    // List all Chrome processes with their start times
    const { stdout } = await execAsync(
      'ps -eo pid,etimes,command | grep "playwright_chromiumdev_profile" | grep -v grep'
    );

    const lines = stdout.trim().split('\n').filter(l => l);
    let killedCount = 0;

    for (const line of lines) {
      const parts = line.trim().split(/\s+/);
      const pid = parts[0];
      const elapsedSeconds = parseInt(parts[1]);
      const command = parts.slice(2).join(' ');

      // Extract profile ID
      const profileMatch = command.match(/playwright_chromiumdev_profile-(\w+)/);
      if (!profileMatch) continue;

      const profileId = profileMatch[1];
      const ageMs = elapsedSeconds * 1000;

      // Kill if: old AND not tracked in Redis
      if (ageMs > MAX_BROWSER_AGE_MS && !trackedProfiles.has(profileId)) {
        try {
          await execAsync(`kill -9 ${pid}`);
          killedCount++;
          LOG(`[zombie-killer] Killed zombie Chrome PID ${pid}, age ${Math.floor(ageMs / 1000)}s, profile ${profileId}`);
        } catch (e) {
          // Already dead, ignore
        }
      }
    }

    if (killedCount > 0) {
      LOG(`[zombie-killer] Cleaned up ${killedCount} zombie Chrome processes`);

      // Also clean up temp directories
      try {
        await execAsync('find /tmp -name "playwright_chromiumdev_profile-*" -type d -mmin +5 -exec rm -rf {} + 2>/dev/null');
      } catch (e) {
        // Ignore errors
      }
    }
  } catch (e) {
    // No zombies found or error - that's OK
    if (!e.message.includes('Command failed')) {
      LOG('[zombie-killer] Error:', e.message);
    }
  }
}, 30000); // Run every 30 seconds


async function handleReserve(ws, payload) {
  const { apiKey, proxy, customArgs, headless } = payload || {};

  // --- API Key Check ---
  if (API_KEY && apiKey !== API_KEY) {
    return ws.send(
      JSON.stringify({ ok: false, code: 401, error: "invalid_api_key" })
    );
  }

  // --- Capacity Check ---
  const claimed = await redis.incr(K_COUNT);
  if (claimed > MAX_BROWSERS) {
    await decCountSafe();
    return ws.send(
      JSON.stringify({ ok: false, code: 429, error: "capacity_exhausted" })
    );
  }

  let assignedPort = null;
  let server = null;

  try {
    // --- Retry loop for EADDRINUSE or flaky Chrome startup ---
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        assignedPort = await getAvailablePort();
        console.log(`\n[INFO] Attempt ${attempt}: Got port ${assignedPort} from pool`);

        // Kill any lingering process on this port BEFORE launching
        killPort(assignedPort);
        await new Promise(r => setTimeout(r, 500)); // Give OS time to release

        // Assign unique DISPLAY for non-headless browsers
        // Use claimed count to cycle through displays 1-99
        const displayNum = !headless ? ((claimed % 99) + 1) : null;

        // Ensure Xvfb display is running for non-headless browsers
        if (!headless && displayNum) {
          await ensureDisplay(displayNum);
        }

        const launchOptions = {
          headless,
          executablePath: "/usr/bin/google-chrome-stable",
          port: assignedPort,
          args: Array.isArray(customArgs)
            ? customArgs
            : ["--disable-blink-features=AutomationControlled"],
          // Set DISPLAY environment variable for non-headless mode
          env: !headless ? { ...process.env, DISPLAY: `:${displayNum}` } : undefined
        };

        if (proxy) launchOptions.proxy = proxy;

        if (!headless) {
          console.log(`[INFO] Launching browser on port ${assignedPort} with DISPLAY :${displayNum}`);
        } else {
          console.log(`[INFO] Launching browser on port ${assignedPort}`);
        }
        console.log("Launch options:", JSON.stringify(launchOptions, null, 2));

        server = await chromium.launchServer(launchOptions);
        const wsEndpoint = server.wsEndpoint();

        console.log(`[DEBUG] Browser launched with endpoint: ${wsEndpoint}`);

        // --- Port mismatch sanity check ---
        if (!wsEndpoint.includes(`:${assignedPort}/`)) {
          throw new Error(`Port mismatch: expected ${assignedPort}, got ${wsEndpoint}`);
        }

        // --- Wait for Chrome WS listener to be ready ---
        await new Promise((r) => setTimeout(r, 1500));

        // --- Test connection to verify it's alive ---
        await new Promise((resolve, reject) => {
          const test = new WebSocket(wsEndpoint, "v1.playwright");
          const timeout = setTimeout(() => {
            test.close();
            reject(new Error("Test connection timeout"));
          }, 5000);

          test.once("open", () => {
            clearTimeout(timeout);
            test.close();
            console.log(`[DEBUG] Test connection to ${wsEndpoint} successful`);
            resolve();
          });

          test.once("error", (err) => {
            clearTimeout(timeout);
            reject(err);
          });
        });

        // --- Browser launched successfully, save metadata ---
        const id = Math.random().toString(36).slice(2);
        local.set(id, { server, port: assignedPort });

        const info = { wsEndpoint, createdAt: now(), lastActiveAt: now() };
        await saveInfo(id, info);

        // ADDED: Quick orphan port cleanup before responding
        (async () => {
          try {
            const allPorts = await redis.smembers(K_PORTS);
            const allIds = await redis.smembers(K_SET);
            const active = new Set();
            for (const bid of allIds) {
              const binfo = await loadInfo(bid);
              if (binfo?.wsEndpoint) {
                const pm = binfo.wsEndpoint.match(/:(\d+)\//);
                if (pm) active.add(pm[1]);
              }
            }
            const orphaned = allPorts.filter(p => !active.has(p));
            for (const p of orphaned) {
              await releasePort(parseInt(p));
            }
          } catch (e) {
            // Don't block browser launch if this fails
          }
        })();

        const publicWs = `${BASE_PATH || ""}/pw/${id}`;
        ws.send(
          JSON.stringify({
            ok: true,
            id,
            publicWs,
            ttlMs: BROWSER_TTL_MS,
            idleTimeoutMs: IDLE_TIMEOUT_MS,
            pingEveryMs: Number(process.env.PING_INTERVAL_MS || 20000),
          })
        );

        LOG(`[broker] started browser ${id} at ${wsEndpoint} (port ${assignedPort})`);
        return; // ✅ Success – exit retry loop

      } catch (err) {
        console.error(`[ERROR] Attempt ${attempt} failed:`, err.message);

        // --- CRITICAL: Release the port on failure ---
        if (assignedPort) {
          console.log(`[WARN] Releasing port ${assignedPort} after failed launch`);
          await releasePort(assignedPort);
          killPort(assignedPort); // Kill any partial Chrome process
          assignedPort = null;
        }

        // Close any partially-launched server
        if (server) {
          try {
            await server.close();
          } catch { }
          server = null;
        }

        const retryable =
          err.message.includes("EADDRINUSE") ||
          err.message.includes("bind") ||
          err.message.includes("Unexpected server response: 400") ||
          err.message.includes("Test connection timeout");

        if (retryable && attempt < 3) {
          console.log(`[WARN] Retrying launch (attempt ${attempt + 1} of 3)...`);
          await new Promise((r) => setTimeout(r, 2000));
          continue;
        }

        throw err; // ❌ Non-retryable error – propagate
      }
    }
  } catch (e) {
    if (assignedPort) await releasePort(assignedPort);
    await decCountSafe();
    console.error("[ERROR] Browser launch failed:", e.message);
    ws.send(
      JSON.stringify({
        ok: false,
        code: 500,
        error: e.message || "launch_failed",
      })
    );
  }
}


async function handlePing(ws, payload) {
  const id = payload?.id;
  if (id) await touch(id);
  ws.send(JSON.stringify({ ok: true, type: "pong" }));
}

async function handleRelease(ws, payload) {
  const id = payload?.id;
  if (!id)
    return ws.send(
      JSON.stringify({ ok: false, code: 400, error: "missing_id" })
    );
  await closeById(id, "release");
  ws.send(JSON.stringify({ ok: true, type: "released" }));
}

// Add this function after your existing helper functions
async function forceReclaimPort(port) {
  try {
    console.log(`[RECLAIM] Attempting to reclaim port ${port}`);

    // 1. Find what's using the port
    let pids = [];
    try {
      const result = execSync(`lsof -ti :${port}`, { encoding: 'utf8' }).trim();
      pids = result.split('\n').filter(Boolean);
    } catch (e) {
      // Port is already free
      console.log(`[RECLAIM] Port ${port} is already free`);
      return true;
    }

    // 2. Kill all processes using this port
    for (const pid of pids) {
      try {
        console.log(`[RECLAIM] Killing PID ${pid} on port ${port}`);
        process.kill(parseInt(pid), 'SIGKILL');
        console.log(`[RECLAIM] Killed PID ${pid}`);
      } catch (e) {
        console.warn(`[RECLAIM] Could not kill PID ${pid}:`, e.message);
      }
    }

    // 3. Double-check with fuser
    try {
      execSync(`fuser -k ${port}/tcp`, { stdio: 'ignore' });
    } catch (e) {
      // Expected if already killed
    }

    // 4. Wait for OS to release the port
    await new Promise(r => setTimeout(r, 1000));

    // 5. Verify port is now free
    try {
      execSync(`lsof -ti :${port}`, { encoding: 'utf8' });
      console.error(`[RECLAIM] Failed to reclaim port ${port} - still in use`);
      return false;
    } catch (e) {
      // Port is free now!
      console.log(`[RECLAIM] ✅ Successfully reclaimed port ${port}`);
      return true;
    }
  } catch (e) {
    console.error(`[RECLAIM] Error reclaiming port ${port}:`, e.message);
    return false;
  }
}

// Cleanup zombie/leaked ports in Redis
// Lightweight wrapper - just calls the full cleanup
async function cleanupZombiePorts() {
  await fullPortCleanup();
}

// Combined cleanup function
async function fullPortCleanup() {
  const lockKey = prefix('cleanup-lock');
  const lockToken = `${process.pid}-${Date.now()}`;
  const lockAcquired = await redis.set(lockKey, lockToken, 'NX', 'EX', 120);

  if (!lockAcquired) {
    console.log('[CLEANUP] Another worker is already cleaning, skipping...');
    return;
  }

  try {
    console.log('\n========================================');
    console.log('[CLEANUP] Starting full port cleanup...');
    console.log('========================================');

    const portsInRedis = await redis.smembers(K_PORTS);
    const browserIds = await redis.smembers(K_SET);

    // Determine active ports
    const activePorts = new Set();
    for (const id of browserIds) {
      const info = await loadInfo(id);
      if (info?.wsEndpoint) {
        const portMatch = info.wsEndpoint.match(/:(\d+)\//);
        if (portMatch) activePorts.add(portMatch[1]);
      }
    }

    // Clean up orphaned ports in Redis only
    const orphanedPorts = portsInRedis.filter(p => !activePorts.has(p));
    console.log(`[CLEANUP] Found ${orphanedPorts.length} orphaned ports in Redis`);

    for (const port of orphanedPorts) {
      console.log(`[CLEANUP] Releasing orphaned port ${port} from Redis`);
      await releasePort(parseInt(port));
    }

    const portsAfter = await redis.smembers(K_PORTS);
    const count = await redis.get(K_COUNT);

    console.log('========================================');
    console.log(`[CLEANUP] Cleanup complete!`);
    console.log(`[CLEANUP] Ports in Redis: ${portsAfter.length}`);
    console.log(`[CLEANUP] Browser count: ${count}`);
    console.log('========================================\n');
  } finally {
    const currentToken = await redis.get(lockKey);
    if (currentToken === lockToken) await redis.del(lockKey);
  }
}


// ---------- HTTP + WS servers ----------
const httpServer = createServer();
const controlWss = new WebSocket.Server({ noServer: true });
const tunnelWss = new WebSocket.Server({
  noServer: true,
  handleProtocols: (protocols) => {
    if (protocols?.includes("v1.playwright")) return "v1.playwright";
    return protocols?.[0];
  },
});

httpServer.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://x");
  const rawPath = url.pathname;
  const path =
    BASE_PATH && BASE_PATH !== "/" && rawPath.startsWith(BASE_PATH)
      ? rawPath.slice(BASE_PATH.length) || "/"
      : rawPath;

  if (path === "/") {
    controlWss.handleUpgrade(req, socket, head, (ws) =>
      controlWss.emit("connection", ws, req)
    );
    return;
  }

  const m = path.match(/^\/pw\/([a-z0-9]+)/i);
  if (m) {
    const requestedProto = req.headers["sec-websocket-protocol"];
    tunnelWss.handleUpgrade(req, socket, head, (ws) =>
      tunnelWss.emit("connection", ws, req, m[1], requestedProto)
    );
    return;
  }

  socket.destroy();
});

// Add this HTTP endpoint for debugging
httpServer.on('request', (req, res) => {
  const url = new URL(req.url, 'http://x');

  if (url.pathname === '/debug/ports' || url.pathname === `${BASE_PATH}/debug/ports`) {
    (async () => {
      try {
        const ports = await redis.smembers(K_PORTS);
        const ids = await redis.smembers(K_SET);
        const count = await redis.get(K_COUNT);

        // Get detailed info about each browser
        const browsers = [];
        for (const id of ids) {
          const info = await loadInfo(id);
          if (info?.wsEndpoint) {
            const portMatch = info.wsEndpoint.match(/:(\d+)\//);
            browsers.push({
              id,
              port: portMatch ? portMatch[1] : 'unknown',
              wsEndpoint: info.wsEndpoint,
              createdAt: new Date(info.createdAt).toISOString(),
              lastActiveAt: new Date(info.lastActiveAt).toISOString(),
              age: Math.round((Date.now() - info.createdAt) / 1000) + 's'
            });
          }
        }

        // Check for orphaned ports
        const activePorts = new Set(browsers.map(b => b.port));
        const orphanedPorts = ports.filter(p => !activePorts.has(p));

        const debugInfo = {
          timestamp: new Date().toISOString(),
          redis: {
            count: parseInt(count) || 0,
            portsInSet: ports.length,
            browsersInSet: ids.length,
            localMapSize: local.size
          },
          ports: {
            all: ports.sort((a, b) => a - b),
            active: Array.from(activePorts).sort((a, b) => a - b),
            orphaned: orphanedPorts.sort((a, b) => a - b)
          },
          browsers,
          issues: {
            countMismatch: parseInt(count) !== ids.length,
            portMismatch: ports.length !== activePorts.size,
            orphanedCount: orphanedPorts.length
          }
        };

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(debugInfo, null, 2));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    })();
    return;
  }

  // NEW: Force cleanup endpoint
  if (url.pathname === '/admin/force-cleanup' || url.pathname === `${BASE_PATH}/admin/force-cleanup`) {
    const authHeader = req.headers.authorization;
    if (API_KEY && authHeader !== `Bearer ${API_KEY}`) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    (async () => {
      try {
        console.log('[ADMIN] Manual cleanup triggered');
        await fullPortCleanup();

        const ports = await redis.smembers(K_PORTS);
        const count = await redis.get(K_COUNT);
        const ids = await redis.smembers(K_SET);

        // Check system ports
        let systemPorts = [];
        try {
          const output = execSync(
            `ss -tlnp | grep -E ':(4[0-4][0-9]{3})' | awk '{print $4}' | sed 's/.*://'`,
            { encoding: 'utf8' }
          ).trim();
          if (output) {
            systemPorts = output.split('\n').map(p => p.trim()).filter(Boolean);
          }
        } catch (e) {
          // No ports in use
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          ok: true,
          message: 'Force cleanup completed',
          before: {
            note: 'Check logs for before state'
          },
          after: {
            portsInRedis: ports.length,
            browserCount: parseInt(count) || 0,
            activeBrowsers: ids.length,
            systemPortsInUse: systemPorts.length,
            systemPorts: systemPorts
          }
        }, null, 2));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message, stack: e.stack }));
      }
    })();
    return;
  }

  // Health check endpoint
  if (url.pathname === '/health' || url.pathname === `${BASE_PATH}/health`) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

controlWss.on("connection", (ws) => {
  ws.on("message", async (buf) => {
    let msg = {};
    try {
      msg = JSON.parse(buf.toString());
    } catch {
      return;
    }
    const { type, payload } = msg;

    try {
      if (type === "reserve") await handleReserve(ws, payload);
      else if (type === "ping") await handlePing(ws, payload);
      else if (type === "release") await handleRelease(ws, payload);
      else
        ws.send(
          JSON.stringify({
            ok: false,
            code: 400,
            error: `unknown_type:${type}`,
          })
        );
    } catch (e) {
      ws.send(JSON.stringify({ ok: false, code: 500, error: e.message }));
    }
  });
});

tunnelWss.on("connection", async (clientWs, req, id, requestedProtoRaw) => {
  console.log(`[DEBUG] Tunnel connection for browser ${id}`);

  let info = await loadInfo(id);

  if (!info) {
    for (let i = 0; i < 5 && !info; i++) {
      await new Promise((r) => setTimeout(r, 100));
      info = await loadInfo(id);
    }
  }

  if (!info) {
    console.log(`[WARN] Rejecting tunnel for unknown or closed browser ${id}`);
    clientWs.close(1011, "unknown_browser");
    return;
  }

  console.log(`[DEBUG] Browser info:`, info);

  let requestedProto = (requestedProtoRaw || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)[0];
  if (!requestedProto) requestedProto = "v1.playwright";

  console.log(`[DEBUG] Connecting to upstream: ${info.wsEndpoint} with protocol: ${requestedProto}`);

  const upstream = new WebSocket(info.wsEndpoint, requestedProto);
  let connectionEstablished = false;

  const cleanup = (reason = "unknown") => {
    console.log(`[DEBUG] Cleaning up tunnel for ${id}, reason: ${reason}`);
    try { clientWs.close(); } catch { }
    try { upstream.close(); } catch { }
  };

  const connectionTimeout = setTimeout(() => {
    if (!connectionEstablished) {
      console.error(`[ERROR] Upstream connection timeout for ${id}`);
      cleanup("connection_timeout");
    }
  }, 10000);

  upstream.on("open", () => {
    console.log(`[DEBUG] Upstream connection established for ${id}`);
    connectionEstablished = true;
    clearTimeout(connectionTimeout);

    const refresh = () => touch(id).catch(() => { });

    clientWs.on("message", (data) => {
      refresh();
      try {
        upstream.send(data);
      } catch (e) {
        console.error(`[ERROR] Failed to send to upstream:`, e.message);
      }
    });

    upstream.on('message', (data) => {
      refresh();
      try {
        clientWs.send(data);
      } catch (e) {
        console.error(`[ERROR] Failed to send to client:`, e.message);
      }
    });

    // CHANGED: Don't close browser immediately, just cleanup tunnel
    clientWs.on("close", () => {
      console.log(`[DEBUG] Client disconnected for ${id}`);
      cleanup("client_close");
      // Let janitor handle browser cleanup via idle timeout
    });

    clientWs.on("error", (err) => {
      console.error(`[ERROR] Client error:`, err.message);
      cleanup("client_error");
    });

    // CHANGED: Don't close browser immediately
    upstream.on('close', (code, reason) => {
      console.log(`[DEBUG] Upstream closed for ${id}: code=${code} reason=${reason}`);
      cleanup("upstream_close");
      // Let janitor handle browser cleanup
    });

    upstream.on("error", (err) => {
      console.error(`[ERROR] Upstream error:`, err.message);
      cleanup("upstream_error");
    });
  });

  upstream.on("error", (err) => {
    clearTimeout(connectionTimeout);
    console.error(`[ERROR] Failed to connect to upstream ${info.wsEndpoint}:`, err.message);
    cleanup("upstream_connection_failed");
  });

  clientWs.on("error", (err) => {
    if (!connectionEstablished) {
      console.error(`[ERROR] Early client error:`, err.message);
      clearTimeout(connectionTimeout);
      cleanup("early_client_error");
    }
  });

  clientWs.on("close", () => {
    if (!connectionEstablished) {
      console.log(`[DEBUG] Client closed before upstream ready`);
      clearTimeout(connectionTimeout);
      cleanup("early_client_close");
    }
  });
});

// Add after your httpServer.listen() call
async function cleanupStalePorts() {
  try {
    const ports = await redis.smembers(K_PORTS);
    const ids = await redis.smembers(K_SET);

    // Get all ports that should be in use
    const activePorts = new Set();
    for (const id of ids) {
      const info = await loadInfo(id);
      if (info?.wsEndpoint) {
        const portMatch = info.wsEndpoint.match(/:(\d+)\//);
        if (portMatch) {
          activePorts.add(portMatch[1]);
        }
      }
    }

    // Release any ports that aren't associated with active browsers
    for (const port of ports) {
      if (!activePorts.has(port)) {
        console.log(`[STARTUP] Releasing stale port ${port}`);
        await releasePort(parseInt(port));
      }
    }

    console.log(`[STARTUP] Port cleanup complete. Active: ${activePorts.size}, Released: ${ports.length - activePorts.size}`);
  } catch (e) {
    console.error('[STARTUP] Port cleanup failed:', e.message);
  }
}

httpServer.listen(PORT, async () => {
  LOG(`[broker] listening on :${PORT} (base=${BASE_PATH})`);
  await cleanupStalePorts();
});

setInterval(async () => {
  try {
    const count = await redis.get(K_COUNT);
    const ids = await redis.smembers(K_SET);
    const ports = await redis.smembers(K_PORTS);
    const info = await redis.info('clients');
    const connectedClients = info.match(/connected_clients:(\d+)/)?.[1] || '?';

    console.log(`[health] Redis count: ${count}, Set size: ${ids.length}, Local map: ${local.size}, Ports used: ${ports.length}, Redis clients: ${connectedClients}`);
  } catch (e) {
    console.error('[health] check failed:', e.message);
  }
}, 30000);

// REPLACE WITH: Only ONE worker should run cleanup
const workerId = process.env.NODE_APP_INSTANCE || process.env.pm_id || '0';

// Only worker 0 runs periodic cleanup
if (workerId === '0') {
  console.log('[CLEANUP] This worker will handle periodic cleanup');

  // Run full cleanup every 10 minutes (less aggressive) - INCREASED
  setInterval(async () => {
    console.log('[CLEANUP] Periodic full cleanup starting...');
    await fullPortCleanup();
  }, 600000); // 10 minutes

  // Run zombie cleanup every 5 minutes - INCREASED
  setInterval(async () => {
    console.log('[CLEANUP] Periodic zombie cleanup starting...');
    await cleanupZombiePorts();
  }, 300000); // 5 minutes
} else {
  console.log(`[CLEANUP] Worker ${workerId} will NOT run periodic cleanup`);
}

process.on("SIGINT", async () => {
  await closeRedis();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await closeRedis();
  process.exit(0);
});