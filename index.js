// ============================================================
// 🚀 MULTI-COOKIE ROUND ROBIN MESSENGER BOT — PRODUCTION SAFE
// ------------------------------------------------------------
//  ✅ Multi-cookie support (1..N)
//  ✅ Independent per-cookie session
//  ✅ Round-robin message sending
//  ✅ Non-blocking background retry
//  ✅ Per-cookie + global exponential backoff
//  ✅ Timeout guard (connect 30s, send 15s)
//  ✅ Config file cache (10s TTL — less disk I/O)
//  ✅ Fatal-error cleanup (no orphan clients)
//  ✅ Anti-crash + graceful shutdown
//  ✅ Zero log spam (DEBUG=1 to enable console)
// ============================================================

// 🔇 Terminal silencer
if (process.env.DEBUG !== '1') {
    const noop = () => {};
    console.log = noop;
    console.info = noop;
    console.warn = noop;
    console.error = noop;
    console.debug = noop;
}

global.WebSocket = require('ws');

const express = require('express');
const { MessengerClient, Platform, CookieManager } = require('messagix-js');
const { setTimeout: sleep } = require('timers/promises');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// ==================== CONSTANTS ====================
const CONNECT_TIMEOUT_MS     = 30000;
const SEND_TIMEOUT_MS        = 15000;
const BACKOFF_STEPS_MS       = [10000, 30000, 60000, 120000, 300000];
const OUTER_BACKOFF_STEPS_MS = [30000, 60000, 120000, 300000];
const CONFIG_ERROR_WAIT_MS   = 10000;
const CONFIG_CACHE_TTL_MS    = 10000;
const NO_HEALTHY_LOG_MS      = 30000;
const MAX_LOG_LINES          = 200;

// ==================== STATE ====================
let isShuttingDown = false;
let globalStates   = null;
const logs         = [];

// ==================== ANTI-CRASH ====================
process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});

// ==================== HELPERS ====================
function pushLog(msg) {
    const line = `[${new Date().toLocaleTimeString()}] ${msg}`;
    logs.push(line);
    if (logs.length > MAX_LOG_LINES) logs.splice(0, logs.length - MAX_LOG_LINES);
}

function readTextFile(filename, defaultValue = '') {
    try {
        if (fs.existsSync(filename)) return fs.readFileSync(filename, 'utf8').trim();
    } catch (_) {}
    return defaultValue;
}

// Cached config reader (TTL 10s) — reduces disk I/O on fast loops
const configCache = new Map();
function readCached(filename, ttl = CONFIG_CACHE_TTL_MS, defaultValue = '') {
    const now = Date.now();
    const entry = configCache.get(filename);
    if (entry && now - entry.ts < ttl) return entry.value;
    const value = readTextFile(filename, defaultValue);
    configCache.set(filename, { ts: now, value });
    return value;
}

function parseCookies(raw) {
    return raw
        .split('\n')
        .map((l) => l.trim())
        .filter((l) => l && l.includes('c_user=') && l.includes('xs='));
}

function withTimeout(promise, ms, label) {
    let timer;
    const tp = new Promise((_, rej) => {
        timer = setTimeout(() => rej(new Error(`${label} timeout ${ms}ms`)), ms);
    });
    return Promise.race([promise, tp]).finally(() => clearTimeout(timer));
}

async function safeDisconnect(client) {
    if (!client) return;
    try { await client.disconnect(); } catch (_) {}
}

// 5s chunks — event loop friendly
async function interruptibleSleep(ms) {
    let remaining = ms;
    while (remaining > 0) {
        if (isShuttingDown) return false;
        const chunk = Math.min(remaining, 5000);
        await sleep(chunk);
        remaining -= chunk;
    }
    return !isShuttingDown;
}

function backoffMs(failCount) {
    const idx = Math.min(Math.max(failCount - 1, 0), BACKOFF_STEPS_MS.length - 1);
    return BACKOFF_STEPS_MS[idx];
}

function outerBackoffMs(failCount) {
    const idx = Math.min(Math.max(failCount - 1, 0), OUTER_BACKOFF_STEPS_MS.length - 1);
    return OUTER_BACKOFF_STEPS_MS[idx];
}

// ==================== ROUTES ====================
app.head('/', (req, res) => res.status(200).end());
app.get('/', (req, res) => res.send('raj mishra dc server running ✅️'));
app.get('/ping', (req, res) => res.send('Pong - Server is Active!'));

app.get('/health', (req, res) => {
    const cookieStatus = globalStates
        ? globalStates.map((s) => ({
              cookie: s.idx,
              healthy: s.healthy,
              failCount: s.failCount,
              retrying: s.retryInProgress,
          }))
        : [];

    const mem = process.memoryUsage();

    res.status(200).json({
        status: 'ONLINE',
        uptime: Math.round(process.uptime()),
        isShuttingDown,
        totalCookies: globalStates ? globalStates.length : 0,
        healthyCount: globalStates ? globalStates.filter((s) => s.healthy).length : 0,
        cookieStatus,
        memory: {
            rss_mb: Math.round(mem.rss / 1024 / 1024),
            heap_mb: Math.round(mem.heapUsed / 1024 / 1024),
        },
        logs: logs.slice(-40),
    });
});

app.listen(PORT, '0.0.0.0', () => {
    pushLog(`[SYSTEM] Live on port ${PORT}`);
    startMultiCookieBot();
});

// ==================== MAIN BOT ====================
async function startMultiCookieBot() {
    pushLog('[BOT] Starting multi-cookie round-robin bot...');

    let outerFailCount = 0;

    while (!isShuttingDown) {
        let states = null;   // ✅ OUTER scope — fix for Bug 1
        try {
            // ---- Read config ----
            const cookiesRaw  = readTextFile('cookies.txt');
            const threadId    = readCached('convo.txt');
            const delaySec    = parseInt(readCached('time.txt', CONFIG_CACHE_TTL_MS, '120'), 10) || 120;
            const messagesRaw = readCached('messages.txt', CONFIG_CACHE_TTL_MS, 'Hello');
            const messages    = messagesRaw.split('\n').map((m) => m.trim()).filter(Boolean);
            const cookieList  = parseCookies(cookiesRaw);

            if (cookieList.length === 0 || !threadId || messages.length === 0) {
                pushLog('[CONFIG ERROR] cookies.txt / convo.txt / messages.txt check karo. 10s retry...');
                await interruptibleSleep(CONFIG_ERROR_WAIT_MS);
                continue;
            }

            pushLog(`[INIT] ${cookieList.length} cookie(s) mile. Initial connect shuru...`);

            // ---- State per cookie ----
            states = cookieList.map((str, i) => ({
                idx: i + 1,
                cookieStr: str,
                client: null,
                healthy: false,
                failCount: 0,
                nextRetryAt: 0,
                retryInProgress: false,
            }));

            globalStates = states;

            // ---- Initial connects (sequential) ----
            for (const state of states) {
                if (isShuttingDown) break;
                await tryConnectState(state);
                await interruptibleSleep(2000);
            }

            const healthyCount = states.filter((s) => s.healthy).length;
            pushLog(`[INIT] ${healthyCount}/${cookieList.length} cookies connected.`);

            // ✅ Successful setup — reset outer fail count
            outerFailCount = 0;

            if (healthyCount === 0) {
                pushLog('[WARN] 0 cookies healthy. Background retry chalega.');
            }

            let msgIndex  = 0;
            let rrIndex   = 0;
            let loopCount = 1;
            let lastNoHealthLog = 0;

            // ============ SEND LOOP (ZERO BLOCK) ============
            while (!isShuttingDown) {
                const loopStart = Date.now();

                // ---- Live config re-read (cached 10s) ----
                const curDelay   = parseInt(readCached('time.txt', CONFIG_CACHE_TTL_MS, String(delaySec)), 10) || 120;
                const curHaters  = readCached('hatersname.txt');
                const curMsgsRaw = readCached('messages.txt', CONFIG_CACHE_TTL_MS);
                const curMsgs    = curMsgsRaw
                    ? curMsgsRaw.split('\n').map((m) => m.trim()).filter(Boolean)
                    : messages;

                if (curMsgs.length === 0) {
                    await interruptibleSleep(CONFIG_ERROR_WAIT_MS);
                    continue;
                }

                if (msgIndex >= curMsgs.length) {
                    msgIndex = 0;
                    loopCount++;
                    pushLog(`[LOOP] Round ${loopCount} starting`);
                }

                const rawMsg   = curMsgs[msgIndex] || 'Hello';
                const finalMsg = curHaters ? `${curHaters} ${rawMsg}` : rawMsg;

                // ---- Try send via round robin ----
                let attempts = 0;
                let sent = false;

                while (attempts < states.length && !isShuttingDown) {
                    const st = states[rrIndex % states.length];
                    rrIndex++;

                    // Unhealthy → background retry, skip
                    if (!st.healthy) {
                        if (!st.retryInProgress && Date.now() >= st.nextRetryAt) {
                            st.retryInProgress = true;
                            tryConnectState(st)
                                .catch(() => {})
                                .finally(() => { st.retryInProgress = false; });
                        }
                        attempts++;
                        continue;
                    }

                    // Healthy → send
                    try {
                        await withTimeout(
                            st.client.sendMessage(threadId, finalMsg),
                            SEND_TIMEOUT_MS,
                            `send[c${st.idx}]`
                        );
                        pushLog(`🚀 [Cookie ${st.idx}] Sent: ${finalMsg}`);
                        st.failCount = 0;
                        sent = true;
                        break;
                    } catch (err) {
                        st.failCount++;
                        pushLog(`⚠️ [Cookie ${st.idx}] fail (${st.failCount}): ${err.message}`);

                        const oldClient = st.client;
                        st.client  = null;
                        st.healthy = false;
                        st.nextRetryAt = Date.now() + backoffMs(st.failCount);
                        st.retryInProgress = false;

                        safeDisconnect(oldClient).catch(() => {});
                        attempts++;
                    }
                }

                if (!sent && Date.now() - lastNoHealthLog > NO_HEALTHY_LOG_MS) {
                    pushLog('⚠️ Koi cookie ready nahi. Round skip.');
                    lastNoHealthLog = Date.now();
                }

                msgIndex++;

                // Adjust delay for time already spent
                const elapsed   = Date.now() - loopStart;
                const remaining = curDelay * 1000 - elapsed;
                if (remaining > 0) {
                    const ok = await interruptibleSleep(remaining);
                    if (!ok) break;
                }
            }

        } catch (err) {
            // ✅ Cleanup orphan clients (Bug 1 fix)
            if (states) {
                for (const st of states) {
                    st.retryInProgress = false;
                    if (st.client) {
                        await safeDisconnect(st.client);
                        st.client = null;
                    }
                    st.healthy = false;
                }
                states = null;
                globalStates = null;
            }

            outerFailCount++;
            const wait = outerBackoffMs(outerFailCount);
            pushLog(`[FATAL] ${err.message}. Retry in ${Math.round(wait / 1000)}s (fail #${outerFailCount})`);
            await interruptibleSleep(wait);
        }
    }

    pushLog('[BOT] Shutdown complete.');
}

// ==================== CONNECT HELPER ====================
async function tryConnectState(state) {
    if (state.client) {
        await safeDisconnect(state.client);
        state.client = null;
    }

    try {
        const cm = CookieManager.fromString(Platform.Messenger, state.cookieStr);
        const client = new MessengerClient({
            platform: Platform.Messenger,
            cookies: cm.getAll(),
            enableE2EE: false,
        });

        await withTimeout(client.loadMessagesPage(), CONNECT_TIMEOUT_MS, `load[c${state.idx}]`);
        await withTimeout(client.connect(),           CONNECT_TIMEOUT_MS, `conn[c${state.idx}]`);

        state.client       = client;
        state.healthy      = true;
        state.failCount    = 0;
        state.nextRetryAt  = 0;
        pushLog(`✅ Cookie ${state.idx} connected`);
    } catch (err) {
        await safeDisconnect(state.client);
        state.client      = null;
        state.healthy     = false;
        state.failCount++;
        state.nextRetryAt = Date.now() + backoffMs(state.failCount);
        pushLog(`❌ Cookie ${state.idx} fail (${state.failCount}): ${err.message}`);
    }
}

// ==================== GRACEFUL SHUTDOWN ====================
async function gracefulShutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    pushLog(`[SHUTDOWN] ${signal}`);

    if (globalStates) {
        for (const st of globalStates) {
            st.retryInProgress = false;
            if (st.client) {
                try { await st.client.disconnect(); } catch (_) {}
                st.client = null;
            }
        }
    }

    await sleep(500);
    process.exit(0);
}

process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
