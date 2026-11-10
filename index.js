global.WebSocket = require('ws'); // Railway & Node WebSocket fix

const express = require('express');
const { MessengerClient, Platform, CookieManager } = require('messagix-js');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// Anti-Crash System: Server kabhi crash nahi hoga
process.on('uncaughtException', (err) => console.error('[ANTI-CRASH] Uncaught Exception:', err.message));
process.on('unhandledRejection', (reason) => console.error('[ANTI-CRASH] Unhandled Rejection:', reason));

// Keep-Alive & Health Check Routes (Render/Railway Sleep Prevention)
app.head('/', (req, res) => res.status(200).end());
app.get('/', (req, res) => res.send('raj mishra dc server running ✅️'));
app.get('/ping', (req, res) => res.status(200).send('Pong - Server is Active!'));
app.get('/health', (req, res) => res.status(200).json({ status: 'ONLINE', uptime: process.uptime() }));

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[SYSTEM LIVE] Headless server listening on port ${PORT}`);
    startInfiniteBot();
});

// Helper to read configuration text files safely
function readTextFile(filename, defaultValue = '') {
    try {
        if (fs.existsSync(filename)) {
            return fs.readFileSync(filename, 'utf8').trim();
        }
    } catch (e) {
        console.error(`Error reading ${filename}:`, e.message);
    }
    return defaultValue;
}

// 24/7 Infinite Loop Bot Core (Single Thread / Long Running)
async function startInfiniteBot() {
    console.log('[BOT] Starting Headless Infinite Loop Messenger Bot...');

    while (true) {
        try {
            // Read configuration from text files
            const cookies = readTextFile('cookies.txt');
            const threadId = readTextFile('convo.txt');
            const timeStr = readTextFile('time.txt', '120');
            const delaySec = parseInt(timeStr, 10) || 120;
            const hatersName = readTextFile('hatersname.txt');
            
            const messagesRaw = readTextFile('messages.txt', 'Hello');
            const messages = messagesRaw.split('\n').map(m => m.trim()).filter(Boolean);

            // Validation check
            if (!cookies || !threadId || messages.length === 0) {
                console.log('[CONFIG ERROR] Missing contents in cookies.txt, convo.txt, or messages.txt! Retrying check in 10 seconds...');
                await new Promise(resolve => setTimeout(resolve, 10000));
                continue;
            }

            console.log(`[AUTH] Authenticating via messagix-js for Thread ID: ${threadId}...`);
            const cookieManager = CookieManager.fromString(Platform.Messenger, cookies);
            const client = new MessengerClient({ 
                platform: Platform.Messenger, 
                cookies: cookieManager.getAll(), 
                enableE2EE: false 
            });

            await client.loadMessagesPage();
            await client.connect();
            console.log('[AUTH] ✅ Login Successful! Dispatching messages in infinite loop...');

            let msgIndex = 0;
            let loopCount = 1;

            // Inner Message Loop
            while (true) {
                // Dynamically re-read files so changes take effect instantly on the next message cycle
                const currentDelaySec = parseInt(readTextFile('time.txt', String(delaySec)), 10) || 120;
                const currentHaters = readTextFile('hatersname.txt');
                const currentMsgsRaw = readTextFile('messages.txt');
                const currentMsgs = currentMsgsRaw ? currentMsgsRaw.split('\n').map(m => m.trim()).filter(Boolean) : messages;

                if (msgIndex >= currentMsgs.length) {
                    msgIndex = 0;
                    loopCount++;
                    console.log(`[LOOP] 🔄 Completed full list. Restarting loop (Round ${loopCount})...`);
                }

                const rawMsg = currentMsgs[msgIndex] || "Hello";
                const finalMessage = currentHaters ? `${currentHaters} ${rawMsg}` : rawMsg;

                try {
                    await client.sendMessage(threadId, finalMessage);
                    console.log(`[${new Date().toLocaleTimeString()}] 🚀 Sent: ${finalMessage}`);
                } catch (err) {
                    console.error(`[${new Date().toLocaleTimeString()}] ⚠️ Send Error: ${err.message}`);
                    // Re-authenticate if session drops or expires
                    if (err.message.includes('login') || err.message.includes('auth') || err.message.includes('connect')) {
                        console.log('[REAUTH] Connection dropped. Re-authenticating...');
                        break;
                    }
                }

                msgIndex++;
                await new Promise(resolve => setTimeout(resolve, currentDelaySec * 1000));
            }

        } catch (err) {
            console.error(`[FATAL ERROR] ${err.message}. Reconnecting in 30 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 30000));
        }
    }
}
