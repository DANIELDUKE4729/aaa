require("dotenv").config();
const express = require("express"),
    fs = require("fs"),
    path = require("path"),
    cors = require("cors"),
    rateLimit = require("express-rate-limit"),
    expressSanitizer = require("express-sanitizer"),

	https = require('https');

let pvpTokenConnection;
// Ensure necessary imports are included
const { 



    TransactionInstruction 
} = require('@solana/web3.js');

const splToken = require("@solana/spl-token"),
    {
        Connection: Connection,
        PublicKey: PublicKey,
        clusterApiUrl: clusterApiUrl,
        Transaction: Transaction,
        SystemProgram: SystemProgram,
        Keypair: Keypair,
        LAMPORTS_PER_SOL: LAMPORTS_PER_SOL
    } = require("@solana/web3.js"),
    {
        createTransferInstruction: createTransferInstruction,
        getAssociatedTokenAddress: getAssociatedTokenAddress,
        TOKEN_PROGRAM_ID: TOKEN_PROGRAM_ID
    } = require("@solana/spl-token"),
    socketIo = require("socket.io"),
    mongoSanitize = require("express-mongo-sanitize"),
    COMMENT_COST_SOL = .047,
    crypto = require("crypto"),
    {
        Buffer: Buffer
    } = require("buffer"),
    solanaWeb3 = require("@solana/web3.js"),
    userClickCooldowns = new Map,
    ACTION_LOG_FILE = path.join(__dirname, "actionlog.json"),
    BALLOON_STATE_FILE = path.join(__dirname, "balloonstate.json"),
    activeTransactions = new Map;

const options = {
  key: fs.readFileSync(path.join(__dirname, 'origin-key.pem')),
  cert: fs.readFileSync(path.join(__dirname, 'origin-cert.pem')),
};

// Initialize app here
const app = express();
app.set('trust proxy', ['loopback', 'linklocal', 'uniquelocal']); // Trust local networks
app.use((req, res, next) => {
  const isSecure = req.secure || 
                  req.get('X-Forwarded-Proto') === 'https' ||
                  req.get('CF-Visitor')?.includes('https');
  
  if (!isSecure) {
    const cloudflareHost = req.get('CF-Connecting-IP') ? 
                         req.get('Host') : 
                         'pvppump.fun';
    return res.redirect(`https://${cloudflareHost}${req.url}`);
  }
  next();
});
app.use((req, res, next) => {
  // Security headers
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains; preload');
  
  // CSP Header (same as your meta tag but as HTTP header)
  res.setHeader('Content-Security-Policy', 
    "default-src 'self' https:; " +
    "script-src 'self' https://cdnjs.cloudflare.com https://cdn.socket.io https://code.jquery.com https://unpkg.com https://bundle.run https://pvppump.fun 'unsafe-inline' 'unsafe-eval'; " +
    "style-src 'self' https://fonts.googleapis.com https://cdnjs.cloudflare.com https://unpkg.com 'unsafe-inline'; " +
    "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com https://unpkg.com; " +
    "img-src 'self' data: https:;" +
    "connect-src 'self' https://pvppump.fun https://*.solana.com https://*.helius-rpc.com; " +
    "frame-src 'none'; " +
    "object-src 'none'; " +
    "base-uri 'self'; " +
    "form-action 'self'"
  );
  next();
});
let isGameCompleting = !1;
const COMMENTS_FILE = path.join(__dirname, "comments.json"),
    fileLocks = new Map(),
    server = https.createServer(options, app),
    io = socketIo(server, {
      cors: {
        origin: ['https://pvppump.fun', 'https://www.pvppump.fun'],
        methods: ["GET", "POST"],
        credentials: true
      },
      transports: ['websocket', 'polling']
    });

	PORT = process.env.PORT || 443;  // HTTPS default port,
    walletDumpTimestamps = {},
    DATA_FILE = path.join(__dirname, process.env.DATA_FILE || "posts.json"),
    USERS_FILE = path.join(__dirname, "users.json"),
    processedTransactions = new Set,
    HELIUS_RPC_URL = `https://api.mainnet-beta.solana.com`,
    RECEIVER_WALLET = "AaWWHxaQHkiXq59Wz1CLeHFtMRuL46QiyMwfZooioFPD";
let cachedPrice = null,
    lastFetchedTime = null;
const PVP_GAMES_FILE = path.join(__dirname, "pvpgames.json"),
    TOKEN_DECIMALS = 6,
    connection = new Connection(HELIUS_RPC_URL, "confirmed");
let latestPrice = null,
    lastUpdated = null,
    lastSource = null,
    latestPriceData = null;
	app.use('/.well-known', express.static(path.join(__dirname, '.well-known')));
app.use(mongoSanitize());
const PVP_MINT = new PublicKey("BysKMSurkH3WVVNaJyQb4477nPx4APRsdo3weiw5pump");
app.use(expressSanitizer());
const corsOptions = {
  origin: ['https://pvppump.fun', 'https://www.pvppump.fun'],
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true
};
let nicknameCache = {};

function loadNicknameCache() {
    try {
        const e = JSON.parse(fs.readFileSync("users.json", "utf-8"));
        nicknameCache = {};
        for (const t in e) nicknameCache[t] = e[t].nickname || "";
        console.log("Nickname cache refreshed")
    } catch (e) {
        console.error("Failed to load nickname cache:", e)
    }
}

function hasAcceptedPvPGame(e) {
    const t = path.join(__dirname, "pvpgames.json");
    let n;
    try {
        n = JSON.parse(fs.readFileSync(t, "utf8"))
    } catch (e) {
        return console.error("Error reading pvpgames.json:", e), !1
    }
    return n.some((t => "accepted" === t.status && (t.creator === e || t.opponent === e)))
}
loadNicknameCache(), fs.watchFile("users.json", {
    interval: 1e3
}, ((e, t) => {
    loadNicknameCache()
})), app.use(cors(corsOptions));
let previousGames = [];
fs.watch(PVP_GAMES_FILE, ((e, t) => {
    if ("change" === e) try {
        const e = fs.readFileSync(PVP_GAMES_FILE, "utf8"),
            t = JSON.parse(e);
        t.filter(((e, t) => {
            const n = previousGames[t];
            return !n || e.status !== n.status
        })).length > 0 && (console.log("Significant changes detected, emitting update..."), io.emit("updatePvPGames", t));
        const n = t.filter((e => "accepted" === e.status && !previousGames.some((t => t.gameId === e.gameId && "accepted" === t.status))));
        n.length > 0 && (console.log(`Found ${n.length} new active games`), n.forEach((e => {
            if (pvpCountdowns.has(e.gameId)) {
                const t = pvpCountdowns.get(e.gameId);
                console.log(`Emitting notification for ${e.gameId}`), io.emit("pvpMatchNotification", {
                    gameId: e.gameId,
                    amount: e.amount,
                    reward: (.953 * e.amount * 2).toFixed(4),
                    hostWallet: e.creator,
                    opponentWallet: e.opponent,
                    countdown: t.remaining
                })
            } else console.warn(`No countdown found for ${e.gameId}`)
        })));
        const o = t.filter((e => {
            if ("completed" !== e.status || !e.winner) return !1;
            const t = previousGames.find((t => t.gameId === e.gameId));
            return t && "accepted" === t.status
        }));
        o.length > 0 && (console.log(`Found ${o.length} newly completed games`), o.forEach((e => {
            io.emit("pvpGameCompleted", {
                gameId: e.gameId,
                isCompleted: !0,
                winner: e.winner,
                amount: e.amount,
                reward: (.953 * e.amount * 2).toFixed(4),
                hostWallet: e.creator,
                opponentWallet: e.opponent,
                hostSide: "1odd" === e.team ? "ODD" : "EVEN",
                opponentSide: "1odd" === e.team ? "EVEN" : "ODD",
                priceAtWin: e.priceData?.price || e.priceAtWin,
                parity: e.priceData?.parity || e.parity,
                priceSource: e.priceData?.source || "Unknown",
                priceTimestamp: e.priceData?.timestamp || e.endTime,
                createdAt: e.createdAt
            }), pvpCountdowns.has(e.gameId) && pvpCountdowns.delete(e.gameId)
        }))), previousGames = t
    } catch (e) {
        console.error("File watch error:", e)
    }
}));
const ensurePvPGamesFile = () => {
    try {
        if (fs.existsSync(PVP_GAMES_FILE)) {
            const e = fs.readFileSync(PVP_GAMES_FILE, "utf8");
            JSON.parse(e)
        } else fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify([]), {
            flag: "wx"
        }), console.log("Initialized pvpgames.json.")
    } catch (e) {
        throw new Error(`Error ensuring pvpgames.json exists: ${e.message}`)
    }
};

function appendPvPGameLog(e) {
    const t = path.join(__dirname, "pvpgameslogs.json");
    let n = [];
    try {
        fs.existsSync(t) && (n = JSON.parse(fs.readFileSync(t, "utf8")))
    } catch (e) {
        n = []
    }
    n.push(e), fs.writeFileSync(t, JSON.stringify(n, null, 2))
}
async function determinePvPGamesWinner() {
    try {
        const e = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
            t = JSON.parse(fs.readFileSync(USERS_FILE, "utf8")),
            {
                price: n,
                source: o
            } = await fetchPriceWithFallback(),
            a = checkEvenOrOdd(n),
            s = (new Date).toISOString();
        console.log(`Fetched price: ${n} (Source: ${o}), Parity: ${a}`);
        let r = !1;
        for (const i of e)
            if ("accepted" === i.status) {
                let e = null;
                e = "odd" === a && "1odd" === i.team || "even" === a && "2even" === i.team ? i.creator : i.opponent, e || (console.warn(`Winner wallet address is missing for game ID: ${i.gameId}`), e = "Unknown Wallet");
                const l = 2 * i.amount * .953;
                t[e] ? (t[e].balance = (t[e].balance || 0) + l, console.log(`Added ${l.toFixed(6)} SOL to ${e}. New balance: ${t[e].balance.toFixed(6)}`)) : console.warn(`Winner ${e} not found in users.json`), i.winner = e, i.status = "completed", i.priceAtWin = n, r = !0, io.emit("pvpWinnerAlert", {
                    winnerWallet: e,
                    hostWallet: i.creator,
                    opponentWallet: i.opponent,
                    amount: l,
                    gameId: i.gameId,
                    hostSide: "1odd" === i.team ? "Odd" : "Even",
                    opponentSide: "1odd" === i.team ? "Even" : "Odd",
                    priceAtWin: n,
                    parity: a,
                    priceTimestamp: s,
                    priceSource: o
                }), appendPvPGameLog({
                    timestamp: s,
                    winner: e,
                    amount: l,
                    gameId: i.gameId,
                    priceAtWin: n,
                    priceSource: o
                }), console.log(`Game ID: ${i.gameId} completed. Winner: ${e}`)
            } r && (fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(e, null, 2)), fs.writeFileSync(USERS_FILE, JSON.stringify(t, null, 2)), io.emit("updatePvPGames", e))
    } catch (e) {
        console.error("Error in determining PvP game winners:", e)
    }
}
async function fetchFromJupiterV2() {
    const e = await fetch("https://api.jup.ag/price/v2?ids=So11111111111111111111111111111111111111112&showExtraInfo=true");
    if (!e.ok) throw new Error(`Jupiter API v2 responded with status ${e.status}`);
    const t = await e.json(),
        n = t.data?.So11111111111111111111111111111111111111112?.price;
    if ("string" != typeof n || isNaN(parseFloat(n))) throw new Error("Invalid or missing price in Jupiter API v2 response");
    return parseFloat(n)
}
async function fetchFromKraken() {
    const e = await fetch("https://api.kraken.com/0/public/Ticker?pair=SOLUSD");
    if (!e.ok) throw new Error(`Kraken API responded with status ${e.status}`);
    const t = await e.json();
    if (!t.result?.SOLUSD?.c) throw new Error("Unexpected Kraken API response");
    return parseFloat(t.result.SOLUSD.c[0])
}
async function fetchFromCoinGecko() {
    const e = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd");
    if (!e.ok) throw new Error(`CoinGecko API responded with status ${e.status}`);
    const t = await e.json();
    if (!t.solana?.usd) throw new Error("Unexpected CoinGecko API response");
    return t.solana.usd
}
async function fetchFromBinance() {
    const e = await fetch("https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT");
    if (!e.ok) throw new Error(`Binance API responded with status ${e.status}`);
    const t = await e.json();
    if (!t.price) throw new Error("Unexpected Binance API response");
    return parseFloat(t.price)
}
async function fetchPriceWithFallback() {
    const e = [{
        name: "Jupiter v2",
        fetchFn: fetchFromJupiterV2
    }, {
        name: "Kraken",
        fetchFn: fetchFromKraken
    }, {
        name: "CoinGecko",
        fetchFn: fetchFromCoinGecko
    }, {
        name: "Binance",
        fetchFn: fetchFromBinance
    }];
    for (const t of e) try {
        const e = await t.fetchFn();
        console.log(`[Source: ${t.name}] Successfully fetched price: $${e.toFixed(2)}`);
        const n = {
            price: e,
            source: t.name,
            lastUpdated: Date.now()
        };
        return latestPriceData = n, io.emit("priceData", latestPriceData), {
            price: e,
            source: t.name
        }
    } catch (e) {
        console.error(`[Source: ${t.name}] Failed to fetch price: ${e.message}`)
    }
    return console.error("All sources failed to fetch Solana price."), {
        price: null,
        source: "None"
    }
}

function checkEvenOrOdd(e) {
    return Math.round(100 * e) % 2 == 0 ? "even" : "odd"
}
async function fetchAndStorePrice() {
    const e = new Date,
        {
            price: t,
            source: n
        } = await fetchPriceWithFallback();
    null !== t ? (latestPrice = `$${t.toFixed(2)} (${checkEvenOrOdd(t)})`, lastUpdated = e.toISOString(), lastSource = n, io.emit("priceUpdate", {
        price: latestPrice,
        lastUpdated: lastUpdated,
        source: lastSource
    })) : (latestPrice = "Failed to fetch price.", lastUpdated = e.toISOString(), lastSource = "None", console.log(`[${e.toISOString()}] Failed to fetch Solana price.`), io.emit("priceUpdate", {
        price: latestPrice,
        lastUpdated: lastUpdated,
        source: lastSource
    }))
}

function startFetchingAtIntervals() {
    const e = [9, 19, 29, 39, 49, 59];
    ! function t() {
        const n = function() {
            const t = new Date,
                n = t.getSeconds(),
                o = t.getMilliseconds(),
                a = 1e3 * ((e.find((e => e > n)) || e[0] + 60) - n) - o;
            return a > 0 ? a : a + 6e4
        }();
        setTimeout((async () => {
            const n = (new Date).getSeconds();
            e.includes(n) && await fetchAndStorePrice(), t()
        }), n)
    }()
}
app.get("/price", ((e, t) => {
    t.json({
        price: latestPrice,
        lastUpdated: lastUpdated,
        source: lastSource
    })
})), startFetchingAtIntervals();
const initializeBalloonState = async () => {
    try {
        if (fs.existsSync(BALLOON_STATE_FILE)) {
            if (fs.readFileSync(BALLOON_STATE_FILE, "utf8").trim()) return void console.log("[Initialize] balloonstate.json already initialized.");
            console.log("[Initialize] balloonstate.json is empty. Initializing...")
        } else console.log("[Initialize] balloonstate.json does not exist. Creating a new file...");
        const e = {
            balloonProgress: 0,
            size: 38,
            lastPumpedBy: null,
            contributors: [],
            _version: Date.now(),
            _lastUpdated: (new Date).toISOString(),
            roundId: 1,
            previousRounds: []
        };
        fs.writeFileSync(BALLOON_STATE_FILE, JSON.stringify(e, null, 2)), console.log("[Initialize] balloonstate.json has been successfully initialized.")
    } catch (e) {
        console.error("[Initialize Error] Failed to initialize balloonstate.json:", e)
    }
};
app.get("/api/round-history", (async (e, t) => {
    try {
        const e = await readBalloonState(),
            n = {
                currentRound: {
                    progress: e.balloonProgress,
                    size: e.size,
                    contributors: e.contributors || [],
                    lastPumpedBy: e.lastPumpedBy
                },
                previousRounds: []
            };
        e.previousRound && n.previousRounds.push({
            roundId: 1,
            totalPool: e.previousRound.totalPool,
            participants: e.previousRound.participants,
            timestamp: e._lastUpdated,
            deductions: e.previousRound.deductions,
            distributions: e.previousRound.distributions,
            totalDistributed: e.previousRound.totalDistributed
        }), t.json(n)
    } catch (e) {
        console.error("Error fetching round history:", e), t.status(500).json({
            error: "Failed to load round history"
        })
    }
})), app.get("/api/pvpgames", ((e, t) => {
    try {
        const e = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8"));
        t.json(e)
    } catch (e) {
        console.error("Failed to fetch PvP games:", e), t.status(500).json({
            error: "Failed to fetch PvP games"
        })
    }
})), app.get("/api/round-details/:roundId", (async (e, t) => {
    try {
        const e = await readBalloonState();
        if (!e.previousRound) return t.status(404).json({
            error: "No rounds found"
        });
        t.json({
            roundId: 1,
            timestamp: e._lastUpdated,
            totalPool: e.previousRound.totalPool,
            totalDistributed: e.previousRound.totalDistributed,
            participants: e.previousRound.participants,
            deductions: e.previousRound.deductions,
            distributions: e.previousRound.distributions
        })
    } catch (e) {
        console.error("Error fetching round details:", e), t.status(500).json({
            error: "Failed to load round details"
        })
    }
})), app.use(((e, t, n) => {
    const o = "127.0.0.1" === e.ip || "::1" === e.ip || "::ffff:127.0.0.1" === e.ip;
    if (["PUT", "POST", "DELETE"].includes(e.method) && !o) return console.log(`Blocked external ${e.method} request from ${e.ip}`), t.status(403).json({
        error: "Forbidden: Write operations are server-only"
    });
    n()
}));
const lastNonces = new Map,
    pvpCountdowns = new Map,
    redistributeAndReset = async e => {
        const t = readJsonFile(USERS_FILE),
            n = Object.values(t).reduce(((e, t) => {
                const n = .33 * t.balance;
                return t.balance -= n, e + n
            }), 0),
            o = .3 * n,
            a = .66 * n,
            s = Object.values(t).reduce(((e, t) => e + t.balance), 0);
        Object.entries(t).forEach((([e, t]) => {
            const n = t.balance / s * o;
            t.balance += n
        }));
        const r = Object.keys(t);
        for (let e = 0; e < a; e++) {
            t[r[Math.floor(Math.random() * r.length)]].balance += 1
        }
        writeJsonFile(USERS_FILE, t), fs.watchFile(BALLOON_STATE_FILE, (() => {
            try {
                const e = JSON.parse(fs.readFileSync(BALLOON_STATE_FILE, "utf8"));
                io.emit("balloon_state", e)
            } catch (e) {
                console.error("Failed to read updated balloon state:", e)
            }
        })), e.balloonProgress = 0, e.kingOfTheHillWallet = null, e.kingOfTheHillProgress = 0, e.gameSessionId = uuidv4(), e.gameLocked = !1, await writeBalloonState(e), io.emit("game_reset", {
            gameSessionId: e.gameSessionId
        }), console.log(`Game reset. New session ID: ${e.gameSessionId}`)
    };
async function getPvpTokenBalance(e) {
    const t = {
        tokenBalance: 0,
        gameWalletBalance: 0
    };
    console.log("Starting getPvpTokenBalance for address:", e);
    try {
        if (!e || e.length < 32) return console.log("Invalid wallet address, returning default:", t), t;
        const n = new Connection(HELIUS_RPC_URL, "confirmed"),
            o = new PublicKey("BysKMSurkH3WVVNaJyQb4477nPx4APRsdo3weiw5pump");
        try {
            console.log("Getting associated token account...");
            const t = await getAssociatedTokenAddress(o, new PublicKey(e));
            console.log("Token account address:", t.toString()), console.log("Fetching token account balance...");
            const a = (await n.getTokenAccountBalance(t)).value.uiAmount || 0;
            console.log("Token balance found:", a);
            let s = 0;
            try {
                console.log("Reading users.json file...");
                const t = JSON.parse(fs.readFileSync(path.join(__dirname, "users.json"), "utf8"));
                s = t[e]?.balancepvp || 0, console.log("Game wallet balance found:", s)
            } catch (e) {
                console.error("JSON read error:", e)
            }
            const r = {
                tokenBalance: a,
                gameWalletBalance: s
            };
            return console.log("Returning final result:", r), r
        } catch (e) {
            if (e.message.includes("could not find account")) return console.log("Token account not found, returning with 0 balance"), {
                ...t,
                tokenBalance: 0
            };
            throw console.error("Token balance error:", e), e
        }
    } catch (e) {
        return console.error("Error in getPvpTokenBalance:", e), t
    }
}

function emitGameComplete(e) {
    try {
        const {
            results: t = [],
            newGameState: n = {
                size: 38,
                balloonProgress: 0,
                lastPumpedBy: "-",
                kingOfTheHillWallet: "-"
            }
        } = e || {}, o = {
            results: t,
            newGameState: {
                size: n.size,
                balloonProgress: n.balloonProgress,
                lastPumpedBy: n.lastPumpedBy,
                kingOfTheHillWallet: n.kingOfTheHillWallet || "-",
                roundId: n.previousRounds?.slice(-1)[0]?.roundId
            }
        };
        io.emit("game_complete", o), emitUpdateBalloon(n), console.log("Game complete event broadcasted:", {
            resultsCount: t.length,
            roundId: o.newGameState.roundId
        })
    } catch (e) {
        console.error("Error emitting game_complete event:", e)
    }
}

function emitUpdateBalloon(e) {
    try {
        if (!e || "object" != typeof e) return void console.error("Invalid balloonState. Skipping emit.");
        const {
            size: t = 0,
            balloonProgress: n = 0,
            kingOfTheHillWallet: o = "-",
            lastPumpedBy: a = "-",
            roundId: s = 1,
            contributors: r = []
        } = e, i = Array.isArray(r) ? r : [];
        if (!io) return void console.error("Socket.io is not initialized. Cannot emit events.");
        io.emit("update_balloon", {
            size: t,
            balloonProgress: n,
            kingOfTheHillWallet: o,
            lastPumpedBy: a,
            roundId: s,
            contributors: i
        }), io.emit("round_history", {
            currentRound: {
                roundId: s,
                progress: n,
                contributors: i
            }
        }), console.log("Balloon state and round history updated and broadcasted:", {
            size: t,
            balloonProgress: n,
            kingOfTheHillWallet: o,
            lastPumpedBy: a,
            roundId: s,
            contributors: i
        })
    } catch (t) {
        console.error("Error emitting update_balloon or round_history event:", {
            error: t,
            balloonState: e
        })
    }
}
const noncesFilePath = path.join(__dirname, "nonces.json"),
    {
        v4: uuidv4
    } = require("uuid"),
    readBalloonState = async () => {
        if (inMemoryBalloonState) return inMemoryBalloonState;
        const e = await fs.promises.readFile(BALLOON_STATE_FILE, "utf8"),
            t = JSON.parse(e);
        return inMemoryBalloonState = t, console.log("[Read State] Balloon state successfully read:", t), t
    };
let inMemoryBalloonState = null;
const writeBalloonState = async e => {
    const t = Date.now(),
        n = BALLOON_STATE_FILE + ".lock";
    try {
        await fs.promises.writeFile(n, t.toString());
        const o = {
            ...e,
            _version: t,
            _lastUpdated: (new Date).toISOString()
        };
        inMemoryBalloonState = {
            ...o
        }, await fs.promises.writeFile(BALLOON_STATE_FILE, JSON.stringify(o, null, 2)), console.log("[State Saved]", {
            version: t,
            progress: e.balloonProgress,
            size: e.size,
            lastUpdated: o._lastUpdated
        })
    } finally {
        try {
            await fs.promises.unlink(n)
        } catch (e) {
            console.error("Lock release failed:", e)
        }
    }
}, transactionQueue = new Map, processTransactionQueue = async e => {
    for (; transactionQueue.get(e)?.length;) {
        const t = transactionQueue.get(e).shift();
        try {
            await t()
        } catch (t) {
            console.error(`Error processing transaction for wallet ${e}:`, t)
        }
    }
    transactionQueue.delete(e)
}, enqueueTransaction = (e, t) => {
    transactionQueue.has(e) || transactionQueue.set(e, []), transactionQueue.get(e).push(t), 1 === transactionQueue.get(e).length && processTransactionQueue(e)
};
let gameState = {
    gameId: uuidv4(),
    balloonProgress: 0,
    kingOfTheHillProgress: 0,
    kingOfTheHillWallet: null,
    transactions: {}
};
const resetGame = async () => {
    const e = await readBalloonState();
    e.gameSessionId = uuidv4(), e.balloonProgress = 0, e.kingOfTheHillProgress = 0, e.kingOfTheHillWallet = null, await writeBalloonState(e), io.emit("game_reset", {
        gameSessionId: e.gameSessionId
    }), console.log(`Game reset. New session ID: ${e.gameSessionId}`)
};
async function saveNonce(e, t) {
    const n = JSON.parse(await fs.promises.readFile(noncesFilePath, "utf-8") || "{}");
    n[e] = t, await fs.promises.writeFile(noncesFilePath, JSON.stringify(n))
}
async function getLastNonce(e) {
    return JSON.parse(await fs.promises.readFile(noncesFilePath, "utf-8") || "{}")[e] || 0
}

function internalOnly(e, t, n) {
    if (!["127.0.0.1", "::1"].includes(e.ip)) return t.status(403).send("Forbidden");
    n()
}
const BOOST_VALIDITY_PERIOD = 12e5,
    boostFilePath = path.join(__dirname, "boost.json");
async function readUsersFile() {
    try {
        const e = await fs.promises.readFile("./users.json", "utf8");
        return JSON.parse(e)
    } catch (e) {
        if ("ENOENT" === e.code) return {};
        throw e
    }
}
const handleGameProgress = async (e, t, n) => {
    isGameCompleting ? console.warn(`Game is completing. Pumps/dumps are temporarily blocked. Request from wallet: ${e} ignored.`) : enqueueTransaction(e, (async () => {
        try {
            const o = await readBalloonState();
            o.contributors ||= [];
            let a = 0;
            if (n) {
                const n = Math.max(0, 5 - o.balloonProgress);
                a = Math.min(t, n);
                const s = o.contributors.findIndex((t => t.wallet === e));
                s >= 0 ? o.contributors[s].amount += a : o.contributors.push({
                    wallet: e,
                    amount: a
                }), o.balloonProgress + a >= 4 && !o.kingOfTheHillWallet && (o.kingOfTheHillWallet = e)
            } else {
                const n = o.contributors.findIndex((t => t.wallet === e));
                if (-1 === n) return void console.warn(`Wallet ${e} is not a contributor for the current round. Dump ignored.`);
                const s = o.contributors[n],
                    r = Math.min(t, s.amount);
                a = -r, s.amount -= r, s.amount <= 0 && o.contributors.splice(n, 1)
            }
            o.balloonProgress += a, o.size += n ? 1 : 0 !== a ? -1 : 0, o.lastPumpedBy = e, o.balloonProgress = Math.min(5, o.balloonProgress), o.size = Math.max(0, o.size);
            const s = await checkGameCompletion(o);
            s || await writeBalloonState(o), emitUpdateBalloon(s ? await readBalloonState() : o)
        } catch (e) {
            console.error("Error in handleGameProgress:", e)
        }
    }))
}, checkGameCompletion = async e => {
    if (e.balloonProgress < 5) return !1;
    console.log("[Game Complete] Starting random redistribution...");
    try {
        isGameCompleting = !0;
        const t = readJsonFile(USERS_FILE),
            n = e.contributors || [],
            o = n.reduce(((e, t) => e + t.amount), 0);
        console.log("Total Pool:", o), n.forEach((e => {
            if ((t[e.wallet]?.balance || 0) < e.amount) throw new Error(`User ${e.wallet} does not have enough balance to cover their contribution.`);
            t[e.wallet].balance -= e.amount
        }));
        let a = n.reduce(((e, t) => e + .47 * t.amount), 0);
        a = parseFloat(a.toFixed(6)), console.log("Redistribution Pool:", a);
        const s = n.map((e => ({
                wallet: e.wallet,
                originalContribution: e.amount,
                maxReward: parseFloat((7 * e.amount).toFixed(6)),
                receivedReward: 0
            }))),
            r = [],
            i = 1e-6,
            l = s.map((e => ({
                wallet: e.wallet,
                randomWeight: Math.random(),
                maxReward: e.maxReward - e.receivedReward
            })));
        console.log("Random Weights Before Normalization:", l);
        const c = l.reduce(((e, t) => e + t.randomWeight), 0);
        if (console.log("Total Random Weight:", c), 0 === c) throw new Error("Total Random Weight is 0. Cannot distribute rewards.");
        l.forEach((e => {
            e.normalizedWeight = e.randomWeight / c
        })), console.log("Random Weights After Normalization:", l);
        let d = a;
        if (l.forEach((e => {
                const n = Math.min(a * e.normalizedWeight, e.maxReward),
                    o = s.find((t => t.wallet === e.wallet)),
                    i = parseFloat((.53 * o.originalContribution).toFixed(6));
                let l = parseFloat((n + i).toFixed(6));
                const c = parseFloat((7 * o.originalContribution).toFixed(6));
                l > c && (console.log(`Capping distribution for ${e.wallet}. Original: ${l}, Capped: ${c}`), l = c), o.receivedReward += n, t[e.wallet] ? t[e.wallet].balance += l : console.error(`Wallet ${e.wallet} not found in users file.`), r.push({
                    wallet: e.wallet,
                    amount: l,
                    finalBalance: parseFloat(t[e.wallet]?.balance.toFixed(6)) || 0
                })
            })), d = parseFloat((o - r.reduce(((e, t) => e + t.amount), 0)).toFixed(6)), console.log("Remaining Pool After Initial Distribution:", d), d > i) {
            console.log("Redistributing Remaining Pool:", d);
            const e = parseFloat((d / n.length).toFixed(6));
            n.forEach((n => {
                const o = t[n.wallet],
                    a = r.find((e => e.wallet === n.wallet));
                if (o && a) {
                    const s = parseFloat((7 * n.amount).toFixed(6)),
                        r = a.amount,
                        i = parseFloat((s - r).toFixed(6));
                    if (i > 0) {
                        const s = Math.min(e, i);
                        o.balance += s, a.amount += s, a.amount = parseFloat(a.amount.toFixed(6)), a.finalBalance = parseFloat(t[n.wallet]?.balance.toFixed(6)) || 0, console.log(`Redistributing to ${n.wallet}: Added ${s}, Total Distributed: ${a.amount}`)
                    } else console.log(`Skipping redistribution for ${n.wallet} as cap is already reached.`)
                }
            })), d = 0
        }
        writeJsonFile(USERS_FILE, t);
        const m = {
            roundId: e.roundId,
            timestamp: (new Date).toISOString(),
            totalPool: o,
            totalDistributed: r.reduce(((e, t) => e + t.amount), 0),
            participants: n.length,
            distributions: r,
            contributions: n.map((e => ({
                wallet: e.wallet,
                amount: e.amount,
                initialBalance: t[e.wallet].balance + e.amount
            }))),
            koth: e.kingOfTheHillWallet || null,
            remainingPool: Math.max(0, d)
        };
        return e.previousRounds.push(m), e.contributors = [], e.balloonProgress = 0, e.size = 38, e.roundId += 1, e.kingOfTheHillWallet = null, await writeBalloonState(e), io.emit("round_completed", {
            roundData: m,
            newGameState: {
                size: e.size,
                balloonProgress: e.balloonProgress,
                lastPumpedBy: e.lastPumpedBy
            }
        }), console.log("Random redistribution complete:", m), setTimeout((() => {
            isGameCompleting = !1, console.log("Pumps/dumps unblocked after 11 seconds.")
        }), 11e3), !0
    } catch (e) {
        throw console.error("Game completion error:", e), setTimeout((() => {
            isGameCompleting = !1, console.log("Pumps/dumps unblocked after error and 11 seconds.")
        }), 11e3), e
    }
};
async function writeUsersFile(e) {
    await fs.promises.writeFile("./users.json", JSON.stringify(e, null, 2), "utf8")
}
const loadBoostData = () => {
        if (fs.existsSync(boostFilePath)) {
            const e = fs.readFileSync(boostFilePath, "utf-8");
            return JSON.parse(e)
        }
        return {}
    },
    saveBoostData = e => {
        fs.writeFileSync(boostFilePath, JSON.stringify(e, null, 2))
    },
    findNextAvailableBoostSlot = e => {
        const t = Date.now();
        for (let n = 0; n < 3; n++) {
            const o = `boost${n}`,
                a = e[o];
            if (!a || t - new Date(a.timestamp).getTime() > 12e5) return o
        }
        return null
    },
    countActiveBoosts = e => {
        const t = Date.now();
        return Object.keys(e).filter((e => e.startsWith("boost"))).filter((n => {
            const o = e[n];
            return o && t - new Date(o.timestamp).getTime() <= 12e5
        })).length
    };
module.exports = {
    loadBoostData: loadBoostData,
    saveBoostData: saveBoostData,
    findNextAvailableBoostSlot: findNextAvailableBoostSlot,
    countActiveBoosts: countActiveBoosts
};
const BALLOON_WALLET_PRIVATE_KEY = process.env.BALLOON_WALLET_PRIVATE_KEY;
if (!BALLOON_WALLET_PRIVATE_KEY) throw new Error("BALLOON_WALLET_PRIVATE_KEY is not defined in the environment variables");
const privateKeyArray = Uint8Array.from(BALLOON_WALLET_PRIVATE_KEY.split(",").map(Number));
if (64 !== privateKeyArray.length) throw new Error("Invalid private key length");
const balloonWallet = solanaWeb3.Keypair.fromSecretKey(privateKeyArray),
 createUnsignedDumpTransaction = async (receiverWallet, solAmount) => {
    try {
        // Validate solAmount
        if (isNaN(solAmount) || solAmount <= 0) {
            throw new Error("Invalid solAmount. It must be a positive number.");
        }

        const lamports = Math.round(solAmount * solanaWeb3.LAMPORTS_PER_SOL);
        if (!Number.isSafeInteger(lamports)) {
            throw new Error("The resulting lamports value is not a safe integer.");
        }

        // Create connection and fetch recent blockhash
        const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed");
        const { blockhash } = await connection.getLatestBlockhash();

        console.log("[CREATE] Creating unsigned transaction...");

        // Construct the transaction
        const transaction = new solanaWeb3.Transaction({
            recentBlockhash: blockhash,
            feePayer: new solanaWeb3.PublicKey(RECEIVER_WALLET), // Fee payer is the client wallet
        }).add(
            solanaWeb3.SystemProgram.transfer({
                fromPubkey: new solanaWeb3.PublicKey(RECEIVER_WALLET), // Server's wallet sends SOL
                toPubkey: new solanaWeb3.PublicKey(receiverWallet), // Client wallet receives SOL
                lamports: lamports,
            })
        );

        console.log("[CREATE] Unsigned transaction created successfully.");

        // Serialize and return the unsigned transaction in base64 format
        return transaction.serialize({ requireAllSignatures: false }).toString("base64");
    } catch (error) {
        console.error("Failed to create unsigned dump transaction:", error);
        throw new Error(`Failed to create unsigned dump transaction: ${error.message}`);
    }
};

 submitSignedTransaction = async (signedTransactionBase64) => {
    try {
        const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed");

        // Deserialize the transaction
        const transaction = solanaWeb3.Transaction.from(Buffer.from(signedTransactionBase64, "base64"));

        console.log("[SUBMIT] Sending signed transaction to blockchain...");

        // Send the signed transaction to the network
        const transactionSignature = await connection.sendRawTransaction(transaction.serialize());

        // Confirm the transaction
        const confirmation = await connection.confirmTransaction(transactionSignature, "confirmed");

        if (confirmation.value.err) {
            throw new Error("Transaction failed: " + JSON.stringify(confirmation.value.err));
        }

        console.log("[SUBMIT] Transaction confirmed:", transactionSignature);

        return transactionSignature;
    } catch (error) {
        console.error("Failed to submit signed transaction:", error);
        throw new Error(`Failed to submit signed transaction: ${error.message}`);
    }
};

 createUnsignedDumpTransaction_pvp = async (receiverWallet, tokenAmount) => {
    try {
        // Validate tokenAmount
        if (isNaN(tokenAmount) || tokenAmount <= 0) {
            throw new Error("Invalid tokenAmount. It must be a positive number.");
        }

        const tokenAmountInUnits = Math.floor(tokenAmount * 10 ** 6); // Convert token amount to smallest unit
        const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed");

        // Fetch recent blockhash
        const { blockhash } = await connection.getLatestBlockhash();

        const serverWalletPublicKey = balloonWallet.publicKey;
        const receiverPublicKey = new solanaWeb3.PublicKey(receiverWallet);

        // Get associated token addresses
        const senderTokenAddress = await solanaWeb3.getAssociatedTokenAddress(
            PVP_MINT,
            serverWalletPublicKey,
            false,
            TOKEN_PROGRAM_ID
        );
        const receiverTokenAddress = await solanaWeb3.getAssociatedTokenAddress(
            PVP_MINT,
            receiverPublicKey,
            false,
            TOKEN_PROGRAM_ID
        );

        console.log("[CREATE_PVP] Creating unsigned PvP SPL token transaction...");

        // Create transfer instruction
        const transferInstruction = solanaWeb3.createTransferInstruction(
            senderTokenAddress,
            receiverTokenAddress,
            serverWalletPublicKey,
            tokenAmountInUnits,
            [],
            TOKEN_PROGRAM_ID
        );

        // Construct the transaction
        const transaction = new solanaWeb3.Transaction({
            recentBlockhash: blockhash,
            feePayer: new solanaWeb3.PublicKey(RECEIVER_WALLET), // Fee payer is the client wallet
        }).add(transferInstruction);

        console.log("[CREATE_PVP] Unsigned PvP transaction created successfully.");

        // Serialize and return the unsigned transaction in base64 format
        return transaction.serialize({ requireAllSignatures: false }).toString("base64");
    } catch (error) {
        console.error("Failed to create unsigned PvP SPL token transaction:", error);
        throw new Error(`Failed to create unsigned PvP SPL token transaction: ${error.message}`);
    }
};

 submitSignedTransaction_pvp = async (signedTransactionBase64) => {
    try {
        const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed");

        // Deserialize the transaction
        const transaction = solanaWeb3.Transaction.from(Buffer.from(signedTransactionBase64, "base64"));

        console.log("[SUBMIT_PVP] Sending signed PvP transaction to blockchain...");

        // Send the signed transaction to the network
        const transactionSignature = await connection.sendRawTransaction(transaction.serialize());

        // Confirm the transaction
        const confirmation = await connection.confirmTransaction(transactionSignature, "confirmed");

        if (confirmation.value.err) {
            throw new Error("Transaction failed: " + JSON.stringify(confirmation.value.err));
        }

        console.log("[SUBMIT_PVP] Transaction confirmed:", transactionSignature);

        return transactionSignature;
    } catch (error) {
        console.error("Failed to submit signed PvP SPL transaction:", error);
        throw new Error(`Failed to submit signed PvP SPL transaction: ${error.message}`);
    }
};
app.use(express.json());
const updateUserBalance = (e, t) => {
        const n = readJsonFile(USERS_FILE);
        if (n[e]) {
            n[e].balance += t, writeJsonFile(USERS_FILE, n), console.log(`Updated balance for wallet ${e}: ${n[e].balance}`);
            try {
                const e = fs.readFileSync(PVP_GAMES_FILE, "utf8"),
                    t = JSON.parse(e);
                io.emit("updatePvPGames", t)
            } catch (e) {
                console.error("Error reading pvpgames.json on connection:", e.message)
            }
        } else console.error(`User with wallet ${e} not found`)
    },
    updateUserBalancePVP = (e, t) => {
        const n = readJsonFile(USERS_FILE);
        if (n[e]) {
            n[e].balancepvp += t, writeJsonFile(USERS_FILE, n), console.log(`Updated balance for wallet ${e}: ${n[e].balancepvp}`);
            try {
                const e = fs.readFileSync(PVP_GAMES_FILE, "utf8"),
                    t = JSON.parse(e);
                io.emit("updatePvPGames", t)
            } catch (e) {
                console.error("Error reading pvpgames.json on connection:", e.message)
            }
        } else console.error(`User with wallet ${e} not found`)
    },
    usersJsonPath = path.join(__dirname, "users.json");

function getUserData(e) {
    try {
        return JSON.parse(fs.readFileSync(usersJsonPath, "utf8"))[e] || null
    } catch (e) {
        return console.error("Error reading users.json:", e), null
    }
}
const initiateDumpTransaction = async (e, t, n) => {
    try {
        const o = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
            a = await fetchLatestBlockhash(o),
            s = new solanaWeb3.Transaction({
                recentBlockhash: a,
                feePayer: new solanaWeb3.PublicKey(n)
            }).add(solanaWeb3.SystemProgram.transfer({
                fromPubkey: balloonWallet.publicKey,
                toPubkey: new solanaWeb3.PublicKey(e),
                lamports: t * solanaWeb3.LAMPORTS_PER_SOL
            }));
        s.sign(balloonWallet);
        const r = await o.sendRawTransaction(s.serialize()),
            i = await o.confirmTransaction(r, "confirmed");
        if (i.value.err) throw new Error("Transaction failed: " + JSON.stringify(i.value.err));
        return r
    } catch (e) {
        throw console.error("Failed to initiate dump transaction:", e), new Error(`Failed to initiate dump transaction: ${e.message}`)
    }
}, getUserBalance = e => {
    const t = readJsonFile(USERS_FILE);
    return t[e]?.balance || 0
}, getUserBalancepvp = e => {
    const t = readJsonFile(USERS_FILE);
    return t[e]?.balance.pvp || 0
}, distributeBalance = (e, t, n) => {
    const o = readJsonFile(USERS_FILE),
        a = getUserBalance(t),
        s = o[t];
    if (!s || !s.selectedTeam) return void console.log(`User ${t} has no team selected - no distribution`);
    const r = s.selectedTeam,
        i = Object.entries(o).filter((([e, t]) => t.selectedTeam === r)),
        l = i.reduce(((e, [t, n]) => e + n.balance), 0);
    i.forEach((([t, n]) => {
        const o = n.balance / l * e;
        n.balance += o
    })), writeJsonFile(USERS_FILE, o);
    const c = n + (getUserBalance(t) - a);
    handleGameProgress(t, c, !0), console.log(`Distributed ${e} among ${i.length} team ${r} members. User ${t} contributed ${c} SOL.`)
};
app.get("/api/comments", ((e, t) => {
    const n = readJsonFile(COMMENTS_FILE);
    t.json(n)
})), app.post("/api/create-transaction", (async (e, t) => {
    const {
        solAmount: n,
        walletAddress: o
    } = e.body;
    try {
        if (isNaN(n) || n <= 0) return t.status(400).json({
            success: !1,
            message: "Invalid solAmount. It must be a positive number."
        });
        const e = Math.round(n * solanaWeb3.LAMPORTS_PER_SOL);
        if (!Number.isSafeInteger(e)) return t.status(400).json({
            success: !1,
            message: "The resulting lamports value is not a safe integer."
        });
        const a = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
            s = await fetchLatestBlockhash(a),
            r = new solanaWeb3.Transaction({
                recentBlockhash: s,
                feePayer: new solanaWeb3.PublicKey(o)
            }).add(solanaWeb3.SystemProgram.transfer({
                fromPubkey: new solanaWeb3.PublicKey(o),
                toPubkey: new solanaWeb3.PublicKey(RECEIVER_WALLET),
                lamports: e
            })).serialize({
                requireAllSignatures: !1
            }).toString("base64");
        t.json({
            success: !0,
            transaction: r
        })
    } catch (e) {
        console.error("Failed to create transaction:", e), t.status(500).json({
            success: !1,
            message: e.message
        })
    }
}));
const processActionPVP = e => {
        const t = parseFloat(e.action.match(/\(([^)]+) PVP\)/)[1]),
            n = t;
        updateUserBalancePVP(e.wallet, n), ensurePvPGamesFile();
        const o = path.join(__dirname, "users.json"),
            a = JSON.parse(fs.readFileSync(o, "utf8")),
            s = a[e.wallet];
        if (!s) throw new Error(`User with wallet ${e.wallet} not found.`);
        if (!s.selectedTeam) throw new Error("User has not selected a team. Join a team before creating a game.");
        if (s.balancepvp < t) throw new Error("Insufficient balance to create a game.");
        s.balancepvp -= t, fs.writeFileSync(o, JSON.stringify(a, null, 2));
        const r = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8"));
        let i;
        do {
            i = `pvp_${Date.now()}_${Math.floor(1e4*Math.random())}`
        } while (r.some((e => e.gameId === i)));
        const l = {
            gameId: i,
            creator: e.wallet,
            team: s.selectedTeam,
            amount: t,
            status: "waiting",
            createdAt: (new Date).toISOString(),
            TokenType: "PVP"
        };
        r.push(l), fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(r, null, 2)), console.log(`New PvP game created: ${i}`)
    },
    processAction = e => {
        const t = parseFloat(e.action.match(/\(([^)]+) SOL\)/)[1]),
            n = t;
        updateUserBalance(e.wallet, n), ensurePvPGamesFile();
        const o = path.join(__dirname, "users.json"),
            a = JSON.parse(fs.readFileSync(o, "utf8")),
            s = a[e.wallet];
        if (!s) throw new Error(`User with wallet ${e.wallet} not found.`);
        if (!s.selectedTeam) throw new Error("User has not selected a team. Join a team before creating a game.");
        if (s.balance < t) throw new Error("Insufficient balance to create a game.");
        s.balance -= t, fs.writeFileSync(o, JSON.stringify(a, null, 2));
        const r = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8"));
        let i;
        do {
            i = `pvp_${Date.now()}_${Math.floor(1e4*Math.random())}`
        } while (r.some((e => e.gameId === i)));
        const l = {
            gameId: i,
            creator: e.wallet,
            team: s.selectedTeam,
            amount: t,
            status: "waiting",
            createdAt: (new Date).toISOString(),
            TokenType: "SOL"
        };
        r.push(l), fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(r, null, 2)), console.log(`New PvP game created: ${i}`)
    };

function readComments() {
    try {
        return JSON.parse(fs.readFileSync(COMMENTS_FILE, "utf8"))
    } catch (e) {
        return console.error("Error reading comments:", e), []
    }
}

function writeComments(e) {
    try {
        fs.writeFileSync(COMMENTS_FILE, JSON.stringify(e, null, 2))
    } catch (e) {
        console.error("Error writing comments:", e)
    }
}
fs.existsSync(ACTION_LOG_FILE) || fs.writeFileSync(ACTION_LOG_FILE, "[]"), fs.existsSync(BALLOON_STATE_FILE) || fs.writeFileSync(BALLOON_STATE_FILE, JSON.stringify({
    size: 36,
    lastPumpedBy: null,
    gameEnded: !1
})), fs.existsSync(COMMENTS_FILE) || fs.writeFileSync(COMMENTS_FILE, "[]");
const logAction = e => {
        let t = [];
        try {
            t = JSON.parse(fs.readFileSync(ACTION_LOG_FILE, "utf8"))
        } catch (e) {
            console.error("Error reading action log:", e)
        }
        t.unshift(e);
        try {
            fs.writeFileSync(ACTION_LOG_FILE, JSON.stringify(t, null, 2)), io.emit("action_logged", e)
        } catch (e) {
            console.error("Error writing action log:", e)
        }
    },
    logActionP = e => {
        let t = [];
        try {
            t = JSON.parse(fs.readFileSync(ACTION_LOG_FILE, "utf8"))
        } catch (e) {
            console.error("Error reading action log:", e)
        }
        t.unshift(e);
        try {
            fs.writeFileSync(ACTION_LOG_FILE, JSON.stringify(t, null, 2)), io.emit("action_logged", e)
        } catch (e) {
            console.error("Error writing action log:", e)
        }
        processAction(e)
    },
    logActionP1 = e => {
        let t = [];
        try {
            t = JSON.parse(fs.readFileSync(ACTION_LOG_FILE, "utf8"))
        } catch (e) {
            console.error("Error reading action log:", e)
        }
        t.unshift(e);
        try {
            fs.writeFileSync(ACTION_LOG_FILE, JSON.stringify(t, null, 2)), io.emit("action_logged", e)
        } catch (e) {
            console.error("Error writing action log:", e)
        }
        processActionPVP(e)
    },
    limiter = rateLimit({
        windowMs: 3e7,
        max: 1e5
    });

function ensureFileExists(e, t = "{}") {
    fs.existsSync(e) || fs.writeFileSync(e, t)
}

function readJsonFile(e) {
    ensureFileExists(e);
    try {
        return JSON.parse(fs.readFileSync(e, "utf8"))
    } catch (t) {
        return console.error(`Error reading ${e}:`, t), {}
    }
}

function writeJsonFile(e, t) {
    if (!t || "object" != typeof t) return void console.error(`Invalid data provided to writeJsonFile for ${e}. Data must be a non-null object.`);
    const n = `${e}.tmp`;
    try {
        const o = JSON.stringify(t, null, 2),
            a = `${e}.lock`,
            s = Date.now(),
            r = 5e3;
        for (; fs.existsSync(a);) {
            if (Date.now() - s > r) throw new Error(`Timeout waiting for lock on ${e}.`);
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10)
        }
        fs.writeFileSync(a, "LOCK", "utf8");
        try {
            fs.writeFileSync(n, o, "utf8"), fs.renameSync(n, e), console.log(`Successfully wrote to ${e}`)
        } finally {
            fs.unlinkSync(a)
        }
    } catch (t) {
        console.error(`Error writing to ${e}:`, t), fs.existsSync(n) && fs.unlinkSync(n)
    }
}

function generateUniqueIdentifier(e) {
    const t = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let n;
    do {
        n = Array.from({
            length: 24
        }, (() => t.charAt(Math.floor(62 * Math.random())))).join("")
    } while (e.includes(n));
    return n
}

function handleBalance(e, t, n) {
    (0 === e.balance || e.balance < 2e-4) && (e.selectedTeam = "", console.log(`User's team reset to 0 as balance reached 0 for wallet: ${t}`), n.emit("team_unlock", {
        message: "Your team selection has been unlocked due to a zero balance.",
        wallet: t
    }))
}

function generateUniqueNickname(e) {
    let t;
    do {
        t = `User_${crypto.randomBytes(12).toString("hex")}`
    } while (e.includes(t));
    return t
}
// Middleware and setup
app.use(limiter);
app.use(express.static(path.join(__dirname, "public")));
ensureFileExists(DATA_FILE);
ensureFileExists(USERS_FILE, "{}");

const connectedUsers = {}; // Define CSP policy once at the top for maintainability

const cspPolicy = [
  "default-src 'self' https:",
  "script-src 'self'",
  "https://cdnjs.cloudflare.com",
  "https://cdn.socket.io", 
  "https://code.jquery.com",
  "https://unpkg.com",
  "https://bundle.run",
  "https://pvppump.fun",
  "'unsafe-inline'",
  "'unsafe-eval'", // Try to eliminate this if possible
  "style-src 'self'",
  "https://fonts.googleapis.com",
  "https://cdnjs.cloudflare.com",
  "https://unpkg.com",
  "'unsafe-inline'",
  "font-src 'self'",
  "https://fonts.gstatic.com",
  "https://cdnjs.cloudflare.com",
  "https://unpkg.com",
"img-src 'self' data: https:;",
  "data:",
  "https://pvppump.fun",
  "connect-src 'self'",
  "wss://pvppump.zapto.org",
  "https://pvppump.fun",
  "https://*.solana.com",
  "https://*.helius-rpc.com",
  "frame-src 'none'",
  "object-src 'none'",
  "base-uri 'self'",
  "form-action 'self'"
].join('; ');

// Apply CSP to static files
app.use(express.static(path.join(__dirname, "public"), {
  setHeaders: (res, path) => {
    if (path.endsWith('.html')) {
      res.setHeader('Content-Security-Policy', cspPolicy);
      res.setHeader('X-Frame-Options', 'DENY');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    }

    if (path.includes('/api/')) {
      res.setHeader('Access-Control-Allow-Origin', 'https://pvppump.fun');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      res.setHeader('Access-Control-Allow-Credentials', 'true');
    }
  }
}));

let users = readJsonFile(USERS_FILE);

    balloonState = readJsonFile(BALLOON_STATE_FILE);
async function handleSignedTransaction(e, t, n, o, a, s, r = null) {
    try {
        const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
            transactionSignature = await connection.sendRawTransaction(Buffer.from(t, "base64"));

        if (processedTransactions.has(transactionSignature)) {
            throw new Error("This transaction has already been processed.");
        }

        processedTransactions.add(transactionSignature);

        // Retry logic for transaction confirmation
        while (true) {
            const status = (await connection.getSignatureStatus(transactionSignature))?.value;

            if (status?.confirmationStatus === "confirmed") {
                console.log(`Transaction confirmed: ${transactionSignature}`);

                const l = readJsonFile(COMMENTS_FILE),
                    c = (new Date).toISOString(),
                    d = {
                        wallet: a,
                        nickname: "",
                        text: s,
                        timestamp: c,
                        likes: 0,
                        replies: []
                    };

                if ("new_reply" === o) {
                    const parentComment = l.find((comment => comment.timestamp === r));
                    if (!parentComment) throw new Error("Parent comment not found.");
                    parentComment.replies.push({
                        wallet: a,
                        nickname: "",
                        text: s,
                        timestamp: c,
                        likes: 0
                    });
                } else {
                    l.push(d);
                }

                writeJsonFile(COMMENTS_FILE, l);
                io.emit(`${o}_success`, {
                    message: `${o.replace("_", " ")} posted successfully!`,
                    walletAddress: a
                });

                break;
            }

            if (status?.err) {
                console.error(`Transaction failed: ${transactionSignature}`);
                io.emit(`${o}_error`, { message: "Transaction failed." });
                break;
            }

            console.log("Transaction not yet confirmed. Retrying in 1 second...");
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    } catch (e) {
        console.error("Failed to process signed transaction:", e);
        io.emit(`${o}_error`, {
            message: `Transaction failed: ${e.message}`
        });
    }
}
const getBalance = async e => {
    try {
        const t = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
            n = await t.getBalance(new solanaWeb3.PublicKey(e));
        return n / 1e9
    } catch (e) {
        return console.error("Failed to fetch balance:", e), null
    }
}, fetchLatestBlockhash = async (e, t = 3) => {
    for (let n = 0; n < t; n++) try {
        const {
            blockhash: t
        } = await e.getLatestBlockhash();
        return t
    } catch (e) {
        if (console.error(`Attempt ${n+1} - Failed to fetch latest blockhash:`, e), n === t - 1) throw e;
        await new Promise((e => setTimeout(e, 1e3)))
    }
};
async function determinePvPGamesWinner() {
    try {
        const e = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
            t = JSON.parse(fs.readFileSync(USERS_FILE, "utf8")),
            {
                price: n,
                source: o
            } = await fetchPriceWithFallback(),
            a = checkEvenOrOdd(n),
            s = (new Date).toISOString();
        console.log(`Fetched price: ${n} (Source: ${o}), Parity: ${a}`);
        let r = !1;
        for (const i of e)
            if ("accepted" === i.status) {
                let e = null;
                e = "odd" === a && "1odd" === i.team || "even" === a && "2even" === i.team ? i.creator : i.opponent, e || (console.warn(`Winner wallet address is missing for game ID: ${i.gameId}`), e = "Unknown Wallet");
                const l = 2 * i.amount * .953;
                t[e] ? (t[e].balance = (t[e].balance || 0) + l, console.log(`Added ${l.toFixed(6)} SOL to ${e}. New balance: ${t[e].balance.toFixed(6)}`)) : console.warn(`Winner ${e} not found in users.json`), i.winner = e, i.status = "completed", i.priceAtWin = n, r = !0, io.emit("pvpWinnerAlert", {
                    winnerWallet: e,
                    hostWallet: i.creator,
                    opponentWallet: i.opponent,
                    amount: l,
                    gameId: i.gameId,
                    hostSide: "1odd" === i.team ? "Odd" : "Even",
                    opponentSide: "1odd" === i.team ? "Even" : "Odd",
                    priceAtWin: n,
                    parity: a,
                    priceTimestamp: s,
                    priceSource: o
                }), appendPvPGameLog({
                    timestamp: s,
                    winner: e,
                    amount: l,
                    gameId: i.gameId,
                    priceAtWin: n,
                    priceSource: o
                }), console.log(`Game ID: ${i.gameId} completed. Winner: ${e}`)
            } r && (fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(e, null, 2)), fs.writeFileSync(USERS_FILE, JSON.stringify(t, null, 2)), io.emit("updatePvPGames", e))
    } catch (e) {
        console.error("Error in determining PvP game winners:", e)
    }
}
async function determinePvPGamesWinnerById(e) {
    try {
        const t = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
            n = JSON.parse(fs.readFileSync(USERS_FILE, "utf8")),
            {
                price: o,
                source: a
            } = await fetchPriceWithFallback(),
            s = checkEvenOrOdd(o),
            r = (new Date).toISOString();
        console.log(`Fetched price: ${o} (Source: ${a}), Parity: ${s}`);
        const i = t.find((t => t.gameId === e));
        if (!i) throw new Error(`Game with ID ${e} not found.`);
        if ("accepted" !== i.status) return void console.warn(`Game with ID ${e} is not in 'accepted' status.`);
        i.priceData = {
            price: o,
            source: a,
            parity: s,
            timestamp: r
        };
        let l = null;
        l = "odd" === s && "1odd" === i.team || "even" === s && "2even" === i.team ? i.creator : i.opponent, l || (console.warn(`Winner wallet address is missing for game ID: ${e}`), l = "Unknown Wallet");
        const c = 2 * i.amount * .953;
        n[l] ? "PVP" === i.TokenType ? (n[l].balancepvp = (n[l].balancepvp || 0) + c, console.log(`Added ${c.toFixed(6)} PVP token to ${l}. New balancepvp: ${n[l].balancepvp.toFixed(6)}`)) : (n[l].balance = (n[l].balance || 0) + c, console.log(`Added ${c.toFixed(6)} SOL to ${l}. New balance: ${n[l].balance.toFixed(6)}`)) : console.warn(`Winner ${l} not found in users.json`), i.winner = l, i.status = "completed", i.priceAtWin = o, io.emit("pvpWinnerAlert", {
            winnerWallet: l,
            hostWallet: i.creator,
            opponentWallet: i.opponent,
            amount: c,
            gameId: e,
            hostSide: "1odd" === i.team ? "Odd" : "Even",
            opponentSide: "1odd" === i.team ? "Even" : "Odd",
            priceAtWin: o,
            parity: s,
            priceTimestamp: r,
            priceSource: a
        }), appendPvPGameLog({
            timestamp: r,
            winner: l,
            amount: c,
            gameId: e,
            priceAtWin: o,
            priceSource: a,
            parity: s
        }), console.log(`Game ID: ${e} completed. Winner: ${l}`), fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(t, null, 2)), fs.writeFileSync(USERS_FILE, JSON.stringify(n, null, 2)), io.emit("updatePvPGames", t)
    } catch (t) {
        console.error(`Error in determining PvP game winner for game ID ${e}:`, t)
    }
}
app.post("/api/activate-icon", (async (e, t) => {
    const {
        walletAddress: n,
        solAmount: o,
        iconIndex: a
    } = e.body;
    if (.33 == o) {
        if (!n || isNaN(o) || o <= 0 || isNaN(a) || a < 0) return t.status(400).json({
            error: "Invalid transaction details"
        });
        try {
            const e = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
                a = await e.getRecentBlockhash("confirmed"),
                s = new solanaWeb3.Transaction({
                    recentBlockhash: a.blockhash,
                    feePayer: new solanaWeb3.PublicKey(n)
                }).add(solanaWeb3.SystemProgram.transfer({
                    fromPubkey: new solanaWeb3.PublicKey(n),
                    toPubkey: new solanaWeb3.PublicKey(RECEIVER_WALLET),
                    lamports: o * solanaWeb3.LAMPORTS_PER_SOL
                })).serialize({
                    requireAllSignatures: !1
                }).toString("base64");
            t.json({
                success: !0,
                transaction: s
            })
        } catch (e) {
            console.error("Failed to create transaction:", e), t.status(500).json({
                error: "Transaction creation failed"
            })
        }
    }
})), app.post("/api/confirm-boost", (async (e, t) => {
    const {
        walletAddress: n,
        signature: o
    } = e.body;
    try {
        const e = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed");
        let a = loadBoostData();
        const s = findNextAvailableBoostSlot(a);
        if (!s) return t.status(400).json({
            success: !1,
            message: "All boosts are currently active. Please wait until a boost expires."
        });
        const r = await e.confirmTransaction(o, "confirmed");
        if (r.value.err) throw new Error("Transaction failed: " + JSON.stringify(r.value.err));
        a[s] = {
            wallet: n,
            timestamp: (new Date).toISOString()
        }, saveBoostData(a);
        const i = countActiveBoosts(a);
        io.emit("boostUpdated", {
            boostData: a,
            activeBoostCount: i
        }), t.json({
            success: !0,
            message: `Boost was successfully applied to ${s}.`,
            activeBoostCount: i
        })
    } catch (e) {
        console.error("Failed to confirm transaction:", e), t.status(500).json({
            error: "Transaction confirmation failed"
        })
    }
})), app.get("/api/helius-config", ((e, t) => {
    t.json({
        url: `https://mainnet-beta.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`
    })
})), app.get("/initialize", ((e, t) => {
    posts = {
        1: {
            id: 1,
            title: "Example Post",
            comments: []
        }
    }, t.send(posts)
})), app.get("/api/boost-status", (async (e, t) => {
    try {
        const e = loadBoostData(),
            n = countActiveBoosts(e),
            o = Object.keys(e).filter((e => e.startsWith("boost"))).map((t => ({
                slot: t,
                ...e[t]
            })));
        t.json({
            boostStatus: o,
            activeBoostCount: n
        })
    } catch (e) {
        console.error("Failed to fetch boost status:", e), t.status(500).json({
            error: "Failed to fetch boost status"
        })
    }
})), app.post("/api/transaction", (async (e, t) => {
    const {
        walletAddress: n,
        solAmount: o,
        nickname: a
    } = e.body;
    if (!n || isNaN(o) || o <= 0) return t.status(400).json({
        error: "Invalid transaction details"
    });
    try {
        const e = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
            a = await fetchLatestBlockhash(e),
            s = new solanaWeb3.Transaction({
                recentBlockhash: a,
                feePayer: new solanaWeb3.PublicKey(n)
            }).add(solanaWeb3.SystemProgram.transfer({
                fromPubkey: new solanaWeb3.PublicKey(n),
                toPubkey: new solanaWeb3.PublicKey(RECEIVER_WALLET),
                lamports: o * solanaWeb3.LAMPORTS_PER_SOL
            })),
            r = await wallet.signTransaction(s),
            i = await e.sendRawTransaction(r.serialize()),
            l = await e.confirmTransaction(i, "confirmed");
        if (l.value.err) throw new Error("Transaction failed: " + JSON.stringify(l.value.err));
        o.toFixed(8), (new Date).toISOString();
        t.json({
            success: !0,
            signature: i
        })
    } catch (e) {
        console.error("Failed to initiate transaction:", e), t.status(500).json({
            error: "Transaction failed"
        })
    }
})), activeConnections = new Map, io.on("connection", (e => {
    console.log(`New client connected: ${e.id}`);
    const t = e.handshake.query.userId || `anon_${e.id}`;
    if (e.on("request_all_wallet_nicknames", (() => {
            e.emit("all_wallet_nicknames", nicknameCache)
        })), activeConnections.has(t)) {
        const e = activeConnections.get(t);
        console.log(`User ${t} already connected. Disconnecting previous connection: ${e.id}`), e.disconnect(!0)
    }
    activeConnections.set(t, e), latestPriceData && setTimeout((() => {
        e.emit("priceData", latestPriceData), console.log("Emitting11 priceData:", latestPriceData)
    }), 369), e.on("disconnect", (() => {
        console.log(`Client disconnected: ${e.id}`), activeConnections.get(t)?.id === e.id && activeConnections.delete(t)
    }));
    try {
        const t = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8"));
        e.emit("updatePvPGames", t);
        const n = t.filter((e => "accepted" === e.status && !e.priceAtWin)).map((e => ({
            gameId: e.gameId,
            hostWallet: e.creator,
            opponentWallet: e.opponent,
            amount: e.amount,
            reward: e.amount,
            countdown: pvpCountdowns.has(e.gameId) ? Math.max(0, pvpCountdowns.get(e.gameId).duration - Math.floor((Date.now() - pvpCountdowns.get(e.gameId).startTime) / 1e3)) : null,
            status: e.status
        })));
        n.length > 0 && e.emit("pending_notifications", n), setTimeout((() => {
            t.filter((e => "accepted" === e.status && !e.priceAtWin)).forEach((t => {
                if (pvpCountdowns.has(t.gameId)) {
                    const n = pvpCountdowns.get(t.gameId),
                        o = Math.max(0, n.duration - Math.floor((Date.now() - n.startTime) / 1e3));
                    o > 0 && (e.emit("pvpWinnerAlert", {
                        winnerWallet: "Countdown to pvp game",
                        hostWallet: t.creator,
                        opponentWallet: t.opponent,
                        amount: t.amount,
                        gameId: t.gameId,
                        hostSide: t.team,
                        opponentSide: null,
                        priceAtWin: null,
                        parity: o,
                        priceTimestamp: new Date(n.startTime).toISOString(),
                        priceSource: null
                    }), e.emit("pvp_countdown_update", {
                        gameId: t.gameId,
                        remainingSeconds: o
                    }))
                }
            }))
        }), 2e3)
    } catch (e) {
        console.error("Error reading pvpgames.json on connection:", e.message)
    }
    try {
        const t = readComments();
        e.emit("load_initial_state", {
            comments: t
        })
    } catch (t) {
        console.error("Error loading comments data:", t), e.emit("load_initial_state", {
            comments: []
        })
    }e.on("join_pvp_game", (async ({
    gameId: t,
    walletAddress: n,
    nonce: o,
    countdown: a
}) => {
    if (hasAcceptedPvPGame(n)) {
        e.emit("pvp_action_blocked");
    } else {
        try {
            const gamesData = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                gameIndex = gamesData.findIndex((e => e.gameId === t));

            if (gameIndex === -1) {
                e.emit("join_game_error", { message: "Game not found." });
                return;
            }

            const game = gamesData[gameIndex];

            if (game.status !== "waiting") {
                e.emit("join_game_error", { message: "Game is not available to join." });
                return;
            }

            if (game.pendingTransaction) {
                if (game.joiningUser !== n) {
                    e.emit("join_game_error", {
                        message: "Another user is currently joining this game. Please wait."
                    });
                    return;
                }
            } else {
                game.joiningUser = n;
                game.pendingTransaction = true;
                fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                console.log(`Game ${t} is now locked for joining by: ${n}`);
            }

            const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
                lamports = Math.round(game.amount * solanaWeb3.LAMPORTS_PER_SOL),
                { blockhash } = await connection.getLatestBlockhash(),
                unsignedTransaction = new solanaWeb3.Transaction({
                    recentBlockhash: blockhash,
                    feePayer: new solanaWeb3.PublicKey(n)
                }).add(solanaWeb3.SystemProgram.transfer({
                    fromPubkey: new solanaWeb3.PublicKey(n),
                    toPubkey: new solanaWeb3.PublicKey(RECEIVER_WALLET),
                    lamports: lamports
                })).serialize({ requireAllSignatures: false }).toString("base64");

            e.emit("join_game_unsigned_transaction", {
                unsignedTransaction: unsignedTransaction,
                gameId: t,
                team: game.team
            });

            const timeout = setTimeout(() => {
                try {
                    const gamesData = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                        gameIndex = gamesData.findIndex((e => e.gameId === t));

                    if (gameIndex !== -1) {
                        const game = gamesData[gameIndex];
                        if (game.pendingTransaction && game.joiningUser === n) {
                            game.pendingTransaction = false;
                            game.joiningUser = null;
                            fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                            console.log(`Transaction timeout expired for game: ${t}. Resetting state.`);
                        }
                    }
                } catch (err) {
                    console.error("Error resetting pendingTransaction flag after timeout:", err);
                }
            }, 20000);

            e.once("submit_signed_join_game_transaction", (async ({
                signedTransaction: l
            }) => {
                clearTimeout(timeout);
                try {
                    console.log("Received signed transaction from client. Validating...");
                    const transaction = solanaWeb3.Transaction.from(Buffer.from(l, "base64"));
                    if (!transaction.verifySignatures()) throw new Error("Invalid transaction signature.");
                    if (!!0) throw new Error("Transaction did not meet server validation criteria.");

                    console.log("Transaction validated. Broadcasting to the blockchain...");
                    const transactionSignature = await connection.sendRawTransaction(Buffer.from(l, "base64"));

                    // Retry logic for transaction confirmation
                    while (true) {
                        const status = (await connection.getSignatureStatus(transactionSignature))?.value;

                        if (status?.confirmationStatus === "confirmed") {
                            console.log(`Transaction confirmed: ${transactionSignature}`);
                            game.status = "accepted";
                            game.opponent = n;
                            game.pendingTransaction = false;
                            game.joiningUser = null;
                            gamesData[gameIndex] = game;

                            fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                            if (["accepted", "completed"].includes(game.status)) {
                                io.emit("updatePvPGames", gamesData);
                            }

                            e.emit("join_game_success", {
                                message: "Game joined successfully!",
                                gameId: t
                            });

                            let countdownDuration = Number(a);
                            (isNaN(countdownDuration) || countdownDuration < 36 || countdownDuration > 96) && (countdownDuration = Math.floor(61 * Math.random()) + 36);

                            pvpCountdowns.set(t, {
                                startTime: Date.now(),
                                duration: countdownDuration,
                                remaining: countdownDuration,
                                interval: null
                            });

                            console.log(`Countdown for ${countdownDuration} seconds started for game ID: ${t}`);
                            io.emit("pvpWinnerAlert", {
                                winnerWallet: "Countdown to pvp game",
                                hostWallet: game.creator,
                                opponentWallet: game.opponent,
                                amount: game.amount,
                                gameId: t,
                                hostSide: game.team,
                                opponentSide: null,
                                priceAtWin: null,
                                parity: countdownDuration,
                                priceTimestamp: (new Date).toISOString(),
                                priceSource: null
                            });

                            const interval = setInterval(() => {
                                const countdown = pvpCountdowns.get(t);
                                if (!countdown) return void clearInterval(interval);

                                const elapsedTime = Math.floor((Date.now() - countdown.startTime) / 1000),
                                    remainingTime = Math.max(0, countdown.duration - elapsedTime);

                                countdown.remaining = remainingTime;
                                io.emit("pvp_countdown_update", {
                                    gameId: t,
                                    remainingSeconds: remainingTime
                                });

                                if (remainingTime <= 0) {
                                    clearInterval(interval);
                                    pvpCountdowns.delete(t);
                                    determinePvPGamesWinnerById(t).then(() => {
                                        console.log(`PvP game winner determined for game ID: ${t}`);
                                    }).catch(err => {
                                        console.error(`Error determining PvP game winner for game ID: ${t}:`, err);
                                    });
                                }
                            }, 1000);

                            pvpCountdowns.get(t).interval = interval;
                            break;
                        }

                        if (status?.err) {
                            console.error(`Transaction failed: ${transactionSignature}`);
                            e.emit("join_game_error", { message: "Transaction failed." });
                            break;
                        }

                        console.log("Transaction not yet confirmed. Retrying in 1 second...");
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                } catch (err) {
                    console.error("Failed to validate or process signed transaction:", err);
                    game.pendingTransaction = false;
                    game.joiningUser = null;
                    fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                    e.emit("join_game_error", {
                        message: `Transaction failed: ${err.message}`
                    });
                }
            }));
        } catch (err) {
            console.error("Error processing join game request:", err);
            const gamesData = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                gameIndex = gamesData.findIndex((e => e.gameId === t));

            if (gameIndex !== -1) {
                const game = gamesData[gameIndex];
                game.pendingTransaction = false;
                game.joiningUser = null;
                fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
            }

            e.emit("join_game_error", {
                message: "Failed to create transaction."
            });
        }
    }
}));e.on("request_wallet_nickname", (t => {
        const n = nicknameCache[t] || null;
        e.emit("wallet_nickname", {
            wallet: t,
            nickname: n
        })
    })), e.on("pump_create_transaction_pvp", (async ({
        solAmount: t,
        walletAddress: n,
        nonce: o,
        team: a
    }) => {
        if (hasAcceptedPvPGame(n)) e.emit("pvp_action_blocked");
        else {
            if (console.log("[PVP] pump_create_transaction_pvp received:", {
                    solAmount: t,
                    walletAddress: n,
                    nonce: o,
                    team: a
                }), isGameCompleting) return console.log("[PVP] Game is completing. Rejecting pump."), void e.emit("pump_error_pvp", {
                message: "Game is completing. Pumping is temporarily blocked."
            });
            try {
                if (isNaN(t) || t <= 0) return console.log("[PVP] Invalid solAmount:", t), void e.emit("pump_error_pvp", {
                    message: "Invalid PvP token amount. It must be a positive number."
                });
                if (!n || n.length < 32) return console.log("[PVP] Invalid wallet address:", n), void e.emit("pump_error_pvp", {
                    message: "Invalid wallet address."
                });
                if (!a || !["1odd", "2even"].includes(a)) return console.log("[PVP] Invalid team selection:", a), void e.emit("pump_error_pvp", {
                    message: "Invalid team selection. Please select a valid team."
                });
                const {
                    tokenBalance: s
                } = await getPvpTokenBalance(n);
                if (console.log("[PVP] tokenBalance for", n, ":", s), s < t) return console.log("[PVP] Insufficient PvP token balance:", s, "<", t), void e.emit("pump_error_pvp", {
                    message: "Insufficient PvP token balance."
                });
                const r = await getLastNonce(n);
                if (console.log("[PVP] lastNonce:", r, "incoming nonce:", o), r >= o) return console.log("[PVP] Replay attack detected for wallet:", n), void e.emit("pump_error_pvp", {
                    message: "Invalid or reused nonce detected."
                });
                await saveNonce(n, o), console.log("[PVP] Nonce saved.");
                const i = new Connection(HELIUS_RPC_URL, "confirmed"),
                    {
                        blockhash: l
                    } = await i.getLatestBlockhash(),
                    c = new PublicKey("BysKMSurkH3WVVNaJyQb4477nPx4APRsdo3weiw5pump"),
                    d = await getAssociatedTokenAddress(c, new PublicKey(n)),
                    m = await getAssociatedTokenAddress(c, new PublicKey(RECEIVER_WALLET));
                console.log("[PVP] fromTokenAccount:", d.toBase58()), console.log("[PVP] toTokenAccount:", m.toBase58());
                const u = new Transaction({
                    recentBlockhash: l,
                    feePayer: new PublicKey(n)
                }).add(createTransferInstruction(d, m, new PublicKey(n), Math.round(1e6 * t))).serialize({
                    requireAllSignatures: !1
                }).toString("base64");
                console.log("[PVP] Serialized unsigned transaction created."), e.emit("pump_unsigned_transaction_pvp", {
                    unsignedTransaction: u,
                    team: a
                }), console.log("[PVP] pump_unsigned_transaction_pvp emitted to client.")
            } catch (t) {
                console.error("[PVP] Failed to create PvP transaction:", t), e.emit("pump_error_pvp", {
                    message: "Failed to create unsigned PvP transaction: " + t.message
                })
            }
        }
    })),
e.on('getLatestPrice', async () => {
    try {
        const price = await fetchFromJupiterV2();
        e.emit('latestPrice', price);
    } catch (err) {
        console.error('Failed to fetch SOL price:', err);
        e.emit('latestPrice', null);
    }
});

  e.on('getBalance', async (publicKey, callback) => {
    try {
      const balance = await getBalance(publicKey);
      callback({ success: true, balance });
    } catch (error) {
      console.error("Balance fetch error:", error);
      callback({ success: false, error: error.message });
    }
  });

	e.on("pump_submit_signed_transaction_pvp", (async ({
    signedTransaction: t,
    nonce: n,
    team: o
}) => {
    console.log("[PVP] pump_submit_signed_transaction_pvp received:", {
        signedTransactionLen: t.length,
        nonce: n,
        team: o
    });

    try {
        const elapsedTime = (Date.now() - n) / 1e3;

        console.log("[PVP] Transaction elapsedTime:", elapsedTime);
        if (elapsedTime > 47) {
            console.log("[PVP] Transaction timeout.");
            e.emit("pump_error_pvp", { message: "Transaction timed out." });
            return;
        }

        const s = Transaction.from(Buffer.from(t, "base64"));
        console.log("[PVP] Transaction deserialized.");
        const r = s.instructions.find((e => e.keys && e.keys.length > 0));

        if (!r) {
            console.log("[PVP] No valid instruction in transaction.");
            throw new Error("No valid instruction with keys found.");
        }

        const walletAddress = r.keys[2].pubkey.toBase58();
        console.log("[PVP] Wallet address extracted from transaction:", walletAddress);

        if (!o || !["1odd", "2even"].includes(o)) {
            console.log("[PVP] Invalid team param:", o);
            e.emit("pump_error_pvp", { message: "Invalid team selection. Please select a valid team." });
            return;
        }

        const connection = new Connection(HELIUS_RPC_URL, "confirmed");

        console.log("[PVP] Sending raw transaction...");
        const transactionSignature = await connection.sendRawTransaction(Buffer.from(t, "base64"));
        console.log("[PVP] Transaction sent. Signature:", transactionSignature);

        let transactionConfirmed = false;
        let retries = 0;

        while (!transactionConfirmed && retries <= 36) {
            try {
                console.log(`[PVP] Attempting to confirm transaction (Retry ${retries})`);
                const confirmation = await connection.confirmTransaction(transactionSignature, "confirmed");

                if (confirmation.value.err) {
                    throw new Error("Transaction failed: " + JSON.stringify(confirmation.value.err));
                }

                transactionConfirmed = true;
                console.log("[PVP] Transaction confirmed.");
            } catch (err) {
                if (err.message.includes("Transaction was not confirmed in 30.00 seconds")) {
                    console.error("[PVP] Transaction not confirmed in 30 seconds. Retrying...");
                    retries++;
                    if (retries > 36) {
                        console.error("[PVP] Maximum retries reached. Transaction failed.");
                        throw err;
                    }
                    await new Promise(resolve => setTimeout(resolve, 1000));
                } else {
                    console.error("[PVP] Transaction failed with an unexpected error:", err);
                    throw err;
                }
            }
        }

        const usersData = readJsonFile(USERS_FILE);
        const user = usersData[walletAddress];
        const balloonState = await readBalloonState();

        if (!user) {
            console.log("[PVP] User not found or game ended:", walletAddress);
            return;
        }

        user.selectedTeam = o;
        writeJsonFile(USERS_FILE, usersData);
        console.log("[PVP] Team saved for user.");

        const popChance = 100 * Math.random();
        console.log("[PVP] popChance:", popChance);

        let pbpopAmount = 0;
        try {
            pbpopAmount = Number(r.data.readBigUInt64LE(1)) / 1e6;
            console.log("[PVP] PBPOP amount in transaction:", pbpopAmount);
        } catch (err) {
            console.log("[PVP] Could not decode PBPOP amount:", err);
        }

        logActionP1({
            gameId: 1,
            wallet: walletAddress,
            nickname: user.nickname || "Anonymous",
            action: `Pumped (${pbpopAmount} PVP)`,
            transactionSignature,
            team: o,
            timestamp: (new Date).toISOString()
        });

        io.emit("balloon_state_updated", balloonState);
        e.emit("pump_success_pvp", { transactionSignature });
        console.log("[PVP] pump_success_pvp emitted to client.");
    } catch (err) {
        console.error("[PVP] Failed to process signed PvP transaction:", err);
        e.emit("pump_error_pvp", { message: "PvP Transaction failed: " + err.message });
    }
})); e.on("fetch_avatar", (t => {
        console.log(`Fetching avatar for wallet: ${t}`);
        const n = getUserData(t);
        n && n.avatar ? e.emit("avatar_response", {
            walletAddress: t,
            avatar: n.avatar,
            nickname: n.nickname || "Unknown"
        }) : e.emit("avatar_response", {
            walletAddress: t,
            error: "User not found or no avatar available"
        })
    })), e.on("get_pvp_countdown", (({
        gameId: e
    }, t) => {
        try {
            if (!pvpCountdowns.has(e)) return t({
                error: "No active countdown for this game"
            });
            const n = pvpCountdowns.get(e),
                o = Math.floor((Date.now() - n.startTime) / 1e3),
                a = Math.max(0, n.duration - o);
            t({
                gameId: e,
                remainingSeconds: a,
                isActive: a > 0
            })
        } catch (e) {
            t({
                error: e.message
            })
        }
    })), e.on("fetch_nickname", (t => {
        console.log(`Fetching nickname for wallet: ${t}`);
        const n = getUserData(t);
        if (n) {
            const o = n.nickname && !n.nickname.startsWith("User") ? n.nickname : t.slice(0, 6);
            e.emit("nickname_response", {
                walletAddress: t,
                nickname: o
            })
        } else e.emit("nickname_response", {
            walletAddress: t,
            nickname: t.slice(0, 6)
        })
    })), e.on("request_round_history", (() => {
        console.log("Round history requested");
        try {
            const t = JSON.parse(fs.readFileSync(BALLOON_STATE_FILE, "utf8")),
                n = t.previousRound ? [t.previousRound] : [],
                o = {
                    balloonProgress: t.balloonProgress || 0,
                    contributors: t.contributors || [],
                    previousRounds: n
                };
            e.emit("round_history", o)
        } catch (t) {
            console.error("Failed to read balloon state:", t), e.emit("round_history", {
                error: "Failed to fetch round history"
            })
        }
    })), e.on("join_pvp_game_pvp", (async ({
    gameId: t,
    walletAddress: n,
    nonce: o,
    countdown: a
}) => {
    if (hasAcceptedPvPGame(n)) {
        e.emit("pvp_action_blocked");
    } else {
        try {
            const gamesData = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                gameIndex = gamesData.findIndex((e => e.gameId === t));

            if (gameIndex === -1) {
                e.emit("join_game_error_pvp", { message: "Game not found." });
                return;
            }

            const game = gamesData[gameIndex];

            if (game.status !== "waiting") {
                e.emit("join_game_error_pvp", { message: "Game is not available to join." });
                return;
            }

            if (game.pendingTransaction) {
                if (game.joiningUser !== n) {
                    e.emit("join_game_error_pvp", {
                        message: "Another user is currently joining this game. Please wait."
                    });
                    return;
                }
            } else {
                game.joiningUser = n;
                game.pendingTransaction = true;
                fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                console.log(`Game ${t} is now locked for joining by: ${n}`);
            }

            const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
                amount = game.amount,
                { blockhash } = await connection.getLatestBlockhash(),
                playerPublicKey = new solanaWeb3.PublicKey(n),
                receiverPublicKey = new solanaWeb3.PublicKey(RECEIVER_WALLET),
                mintPublicKey = new solanaWeb3.PublicKey(PVP_MINT),
                playerTokenAddress = await splToken.getAssociatedTokenAddress(mintPublicKey, playerPublicKey),
                receiverTokenAddress = await splToken.getAssociatedTokenAddress(mintPublicKey, receiverPublicKey),
                decimals = 6,
                transferAmount = Math.round(amount * Math.pow(10, decimals)),
                unsignedTransaction = new solanaWeb3.Transaction({
                    recentBlockhash: blockhash,
                    feePayer: playerPublicKey
                }).add(splToken.createTransferInstruction(playerTokenAddress, receiverTokenAddress, playerPublicKey, transferAmount))
                    .serialize({ requireAllSignatures: false })
                    .toString("base64");

            e.emit("join_game_unsigned_transaction_pvp", {
                unsignedTransaction: unsignedTransaction,
                gameId: t,
                team: game.team
            });

            const timeout = setTimeout(() => {
                try {
                    const gamesData = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                        gameIndex = gamesData.findIndex((e => e.gameId === t));

                    if (gameIndex !== -1) {
                        const game = gamesData[gameIndex];
                        if (game.pendingTransaction && game.joiningUser === n) {
                            game.pendingTransaction = false;
                            game.joiningUser = null;
                            fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                            console.log(`Transaction timeout expired for game: ${t}. Resetting state.`);
                        }
                    }
                } catch (err) {
                    console.error("Error resetting pendingTransaction flag after timeout:", err);
                }
            }, 20000);

            e.once("submit_signed_join_game_transaction_pvp", (async ({
                signedTransaction: l
            }) => {
                clearTimeout(timeout);
                try {
                    console.log("Received signed PVP transaction from client. Validating...");
                    const transaction = solanaWeb3.Transaction.from(Buffer.from(l, "base64"));
                    if (!transaction.verifySignatures()) throw new Error("Invalid transaction signature.");
                    if (!!0) throw new Error("Transaction did not meet server validation criteria.");

                    console.log("Transaction validated. Broadcasting to the blockchain...");
                    const transactionSignature = await connection.sendRawTransaction(Buffer.from(l, "base64"));

                    // Retry logic for transaction confirmation
                    while (true) {
                        const status = (await connection.getSignatureStatus(transactionSignature))?.value;

                        if (status?.confirmationStatus === "confirmed") {
                            console.log(`Transaction confirmed: ${transactionSignature}`);
                            game.status = "accepted";
                            game.opponent = n;
                            game.pendingTransaction = false;
                            game.joiningUser = null;
                            gamesData[gameIndex] = game;

                            fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                            if (["accepted", "completed"].includes(game.status)) {
                                io.emit("updatePvPGames", gamesData);
                            }

                            e.emit("join_game_success_pvp", {
                                message: "PVP Game joined successfully!",
                                gameId: t
                            });

                            let countdownDuration = Number(a);
                            (isNaN(countdownDuration) || countdownDuration < 36 || countdownDuration > 96) && (countdownDuration = Math.floor(61 * Math.random()) + 36);

                            pvpCountdowns.set(t, {
                                startTime: Date.now(),
                                duration: countdownDuration,
                                remaining: countdownDuration,
                                interval: null
                            });

                            console.log(`Countdown for ${countdownDuration} seconds started for game ID: ${t}`);
                            io.emit("pvpWinnerAlert", {
                                winnerWallet: "Countdown to pvp game",
                                hostWallet: game.creator,
                                opponentWallet: game.opponent,
                                amount: game.amount,
                                gameId: t,
                                hostSide: game.team,
                                opponentSide: null,
                                priceAtWin: null,
                                parity: countdownDuration,
                                priceTimestamp: (new Date).toISOString(),
                                priceSource: null
                            });

                            const interval = setInterval(() => {
                                const countdown = pvpCountdowns.get(t);
                                if (!countdown) return void clearInterval(interval);

                                const elapsedTime = Math.floor((Date.now() - countdown.startTime) / 1000),
                                    remainingTime = Math.max(0, countdown.duration - elapsedTime);

                                countdown.remaining = remainingTime;
                                io.emit("pvp_countdown_update", {
                                    gameId: t,
                                    remainingSeconds: remainingTime
                                });

                                if (remainingTime <= 0) {
                                    clearInterval(interval);
                                    pvpCountdowns.delete(t);
                                    determinePvPGamesWinnerById(t).then(() => {
                                        console.log(`PvP game winner determined for game ID: ${t}`);
                                    }).catch(err => {
                                        console.error(`Error determining PvP game winner for game ID: ${t}:`, err);
                                    });
                                }
                            }, 1000);

                            pvpCountdowns.get(t).interval = interval;
                            break;
                        }

                        if (status?.err) {
                            console.error(`Transaction failed: ${transactionSignature}`);
                            e.emit("join_game_error_pvp", { message: "Transaction failed." });
                            break;
                        }

                        console.log("Transaction not yet confirmed. Retrying in 1 second...");
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                } catch (err) {
                    console.error("Failed to validate or process signed transaction:", err);
                    game.pendingTransaction = false;
                    game.joiningUser = null;
                    fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
                    e.emit("join_game_error_pvp", {
                        message: `Transaction failed: ${err.message}`
                    });
                }
            }));
        } catch (err) {
            console.error("Error processing join PVP game request:", err);
            const gamesData = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                gameIndex = gamesData.findIndex((e => e.gameId === t));
            if (gameIndex !== -1) {
                const game = gamesData[gameIndex];
                game.pendingTransaction = false;
                game.joiningUser = null;
                fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(gamesData, null, 2));
            }
            e.emit("join_game_error_pvp", {
                message: "Failed to create transaction."
            });
        }
    }
}));e.on("requestAddComment", (({
        postId: t,
        username: n,
        comment: o
    }) => {
        if (!posts[t]) return void e.emit("commentError", `Post with ID ${t} does not exist`);
        const a = {
            username: n,
            comment: o,
            date: (new Date).toISOString(),
            likes: 0,
            replies: []
        };
        posts[t].comments.push(a), io.emit("commentAdded", posts[t])
    })), e.on("requestLikeComment", (({
        postId: t,
        commentDate: n
    }) => {
        if (!posts[t]) return void e.emit("likeCommentError", `Post with ID ${t} does not exist`);
        const o = posts[t].comments.find((e => e.date === n));
        o ? (o.likes += 1, io.emit("commentLiked", posts[t])) : e.emit("likeCommentError", `Comment with date ${n} does not exist`)
    })), e.on("requestReplyComment", (({
        postId: t,
        parentCommentDate: n,
        username: o,
        reply: a
    }) => {
        if (!posts[t]) return void e.emit("replyError", `Post with ID ${t} does not exist`);
        const s = posts[t].comments.find((e => e.date === n));
        if (!s) return void e.emit("replyError", `Parent comment with date ${n} does not exist`);
        const r = {
            username: o,
            reply: a,
            date: (new Date).toISOString(),
            likes: 0
        };
        s.replies.push(r), io.emit("replyAdded", posts[t])
    })), e.on("boostUpdated", (async e => {
        console.log("Boost updated:", e), await updateIcons()
    })), e.on("request_round_history", (async () => {
        try {
            const t = await readBalloonState();
            e.emit("round_history", {
                currentRound: {
                    progress: t.balloonProgress,
                    size: t.size,
                    contributors: t.contributors
                },
                previousRounds: t.previousRounds || []
            })
        } catch (t) {
            console.error("Error fetching round history:", t), e.emit("round_history_error", "Failed to load round history")
        }
    })), e.on("request_round_details", (async t => {
        try {
            const n = await readBalloonState(),
                o = n.previousRounds?.find((e => e.roundId === t));
            o ? e.emit("round_details", o) : e.emit("round_details_error", "Round not found")
        } catch (t) {
            console.error("Error fetching round details:", t), e.emit("round_details_error", "Failed to load round details")
        }
    })), e.on("view_profile", (async t => {
        const {
            clickedWalletAddress: n,
            walletAddressFromContainer: o
        } = t, a = connectedUsers[e.id], s = a ? a.wallet : null;
        console.log(`Fetching profile for clicked wallet: ${n}`), console.log(`Connected User Wallet: ${s}`);
        try {
            const e = JSON.parse(fs.readFileSync(USERS_FILE, "utf8"))[n];
            e ? io.emit("profile_data", {
                user: e,
                connectedUserWallet: s,
                clickedWalletAddress: n,
                walletAddressFromContainer: o
            }) : io.emit("profile_data", {
                error: "User not found",
                connectedUserWallet: s,
                clickedWalletAddress: n,
                walletAddressFromContainer: o
            })
        } catch (e) {
            console.error("Failed to fetch profile:", e), io.emit("profile_data", {
                error: "Failed to fetch profile",
                connectedUserWallet: s,
                clickedWalletAddress: n,
                walletAddressFromContainer: o
            })
        }
    })), e.on("like_comment", (e => {
        const t = readComments(),
            n = t.find((t => t.timestamp === e.commentId));
        if (n) {
            if (n.likedBy || (n.likedBy = []), n.likedBy.includes(e.wallet)) return void console.log(`Wallet ${e.wallet} already liked comment ${e.commentId}`);
            n.likedBy.push(e.wallet), n.likes += 1, writeComments(t), io.emit("comment_liked", {
                commentId: e.commentId,
                likes: n.likes,
                likedBy: n.likedBy
            });
            const o = readJsonFile("users.json"),
                a = o[n.wallet];
            a ? (a.likesCount = (a.likesCount || 0) + 1, writeJsonFile("users.json", o), console.log(`Increased like count for user: ${n.wallet}`)) : console.error("User not found:", n.wallet)
        }
    })), e.on("new_comment", (e => {
        const t = readComments();
        t.push(e), writeComments(t), io.emit("new_comment", e)
    })), e.on("cancel_match", (async t => {
        const {
            gameId: n,
            walletAddress: o
        } = t;
        if (hasAcceptedPvPGame(o)) e.emit("pvp_action_blocked");
        else try {
            const n = JSON.parse(fs.readFileSync(PVP_GAMES_FILE, "utf8")),
                a = JSON.parse(fs.readFileSync(USERS_FILE, "utf8"));
            if (!t.gameId || "string" != typeof t.gameId) return console.error("Invalid gameId received:", t.gameId), e.emit("cancel_match_response", {
                error: "Invalid game ID format"
            });
            const s = t.gameId.trim();
            console.log(`Processing cancellation for game: ${s}`);
            const r = n.findIndex((e => e.gameId && e.gameId.toString().toLowerCase() === s.toLowerCase()));
            if (-1 === r) return console.log(`Game ${s} not found - may have been already cancelled`), e.emit("cancel_match_response", {
                success: "Game already cancelled"
            });
            if (-1 === r) return e.emit("cancel_match_response", {
                error: "Game not found"
            });
            const i = n[r];
            if ("waiting" !== i.status) return e.emit("cancel_match_response", {
                error: "Game is not in a cancellable state"
            });
            if (i.pendingTransaction) return e.emit("cancel_match_response", {
                error: "Game has pending transactions and cannot be canceled"
            });
            if (i.creator !== o) return e.emit("cancel_match_response", {
                error: "You are not authorized to cancel this game"
            });
            const l = .953 * i.amount;
            a[o].balance += l, n.splice(r, 1), fs.writeFileSync(PVP_GAMES_FILE, JSON.stringify(n, null, 2)), fs.writeFileSync(USERS_FILE, JSON.stringify(a, null, 2)), e.emit("cancel_match_response", {
                success: `Match canceled. Refunded: ${l.toFixed(6)} SOL`,
                updatedGames: n
            }), io.emit("updatePvPGames", n), console.log("Emitting cancel_match_response to sender"), e.emit("cancel_match_response", {
                success: `Match canceled. Refunded: ${l.toFixed(6)} SOL`,
                updatedGames: n
            })
        } catch (t) {
            console.error("Error canceling match:", t), e.emit("cancel_match_response", {
                error: `Failed to cancel match: ${t.message||"Unknown error"}`
            })
        }
    })), e.on("new_reply", (e => {
        const t = readComments(),
            n = t.find((t => t.timestamp === e.commentId));
        if (n) {
            n.replies.push(e.reply), writeComments(t), io.emit("new_reply", e);
            const o = readJsonFile("users.json"),
                a = o[n.wallet];
            a ? (a.mentionsCount = (a.mentionsCount || 0) + 1, writeJsonFile("users.json", o), console.log(`Increased mentions count for user: ${n.wallet}`)) : console.error("User not found:", n.wallet)
        }
    })), e.on("update_balance", (t => {
        const {
            wallet: n,
            balance: o
        } = t;
        if ("string" != typeof n || "number" != typeof o) return void console.error("Invalid data:", t);
        users = readJsonFile(USERS_FILE);
        let a = users[n];
        a ? (a.balance = o, writeJsonFile(USERS_FILE, users), e.emit("user_data", a), console.log(`Updated balance for user: ${n}`)) : console.error("User not found:", n)
    })), e.on("update_likesCount", (t => {
        const {
            wallet: n,
            likesCount: o
        } = t;
        if ("string" != typeof n || "number" != typeof o) return void console.error("Invalid data:", t);
        users = readJsonFile(USERS_FILE);
        let a = users[n];
        a ? (a.likesCount = o, writeJsonFile(USERS_FILE, users), e.emit("user_data", a), console.log(`Updated likesCount for user: ${n}`)) : console.error("User not found:", n)
    })), e.on("update_mentionsCount", (t => {
        const {
            wallet: n,
            mentionsCount: o
        } = t;
        if ("string" != typeof n || "number" != typeof o) return void console.error("Invalid data:", t);
        users = readJsonFile(USERS_FILE);
        let a = users[n];
        a ? (a.mentionsCount = o, writeJsonFile(USERS_FILE, users), e.emit("user_data", a), console.log(`Updated mentionsCount for user: ${n}`)) : console.error("User not found:", n)
    })), e.on("boost_activated", (e => {
        io.emit("play_boost_sound"), console.log(`Boost activated by ${e.wallet} on icon ${e.iconIndex}`)
    })), e.on("pump_activated", (e => {
        io.emit("play_pump_sound")
    })), e.on("dump_activated", (e => {
        io.emit("play_dump_sound")
    })), e.on("update_nickname", (async t => {
        try {
            const {
                wallet: n,
                newNickname: o,
                timestamp: a
            } = t, s = await readUsersFile(), r = Object.values(s).some((e => e.nickname === o && e.wallet !== n));
            if (r) return e.emit("nickname_updated", {
                success: !1,
                message: "This nickname is already taken.",
                wallet: n
            });
            const i = s[n] || {},
                l = new Date(i.lastNicknameChange || 0),
                c = new Date,
                d = Math.round((c - l) / 1e3);
            if (d < 400) {
                const t = 400 - d,
                    o = Math.floor(t / 3600),
                    a = Math.floor(t % 3600 / 60);
                return e.emit("nickname_updated", {
                    success: !1,
                    message: `You can change your nickname in ${o} hours and ${a} minutes.`,
                    wallet: n
                })
            }
            const m = {
                ...i,
                nickname: o,
                lastNicknameChange: a,
                lastUpdate: a
            };
            s[n] = m, await writeUsersFile(s), io.emit("nickname_updated", {
                success: !0,
                message: "Nickname updated successfully!",
                wallet: n,
                newNickname: o
            })
        } catch (n) {
            console.error("Error updating nickname:", n), e.emit("nickname_updated", {
                success: !1,
                message: "Failed to update nickname",
                wallet: t.wallet
            })
        }
    })), e.on("update_avatar", (async t => {
        try {
            const {
                wallet: n,
                avatar: o,
                timestamp: a
            } = t, s = await readUsersFile(), r = s[n] || {}, i = new Date(r.lastAvatarChange || 0), l = new Date, c = Math.round((l - i) / 1e3);
            if (c < 400) {
                const t = 400 - c,
                    o = Math.floor(t / 3600),
                    a = Math.floor(t % 3600 / 60);
                return e.emit("avatar_updated", {
                    success: !1,
                    message: `You can change your avatar in ${o} hours and ${a} minutes.`,
                    wallet: n
                })
            }
            const d = {
                ...r,
                avatar: o,
                lastAvatarChange: a,
                lastUpdate: a
            };
            s[n] = d, await writeUsersFile(s), io.emit("avatar_updated", {
                success: !0,
                message: "Avatar updated successfully!",
                wallet: n,
                avatar: o
            })
        } catch (n) {
            console.error("Error updating avatar:", n), e.emit("avatar_updated", {
                success: !1,
                message: "Failed to update avatar",
                wallet: t.wallet
            })
        }
    })), e.on("update_bio", (async t => {
        try {
            const {
                wallet: n,
                bio: o,
                timestamp: a
            } = t, s = await readUsersFile(), r = s[n] || {}, i = new Date(r.lastBioChange || 0), l = new Date, c = Math.round((l - i) / 1e3);
            if (c < 400) {
                const t = 400 - c,
                    o = Math.floor(t / 3600),
                    a = Math.floor(t % 3600 / 60);
                return e.emit("bio_updated", {
                    success: !1,
                    message: `You can change your bio in ${o} hours and ${a} minutes.`,
                    wallet: n
                })
            }
            const d = {
                ...r,
                bio: o,
                lastBioChange: a,
                lastUpdate: a
            };
            s[n] = d, await writeUsersFile(s), io.emit("bio_updated", {
                success: !0,
                message: "Bio updated successfully!",
                wallet: n,
                bio: o
            })
        } catch (n) {
            console.error("Error updating bio:", n), e.emit("bio_updated", {
                success: !1,
                message: "Failed to update bio",
                wallet: t.wallet
            })
        }
    })), e.on("save_user_data", (t => {
        if ("object" != typeof t || "string" != typeof t.wallet) return void console.error("Invalid data payload:", t);
        const n = t.wallet;
        users = readJsonFile(USERS_FILE);
        let o = users[n];
        if (o) o.lastConnectionDate = (new Date).toISOString(), console.log(`Existing user reconnected: ${n}`);
        else {
            const e = Object.values(users).map((e => e.identifier)),
                t = Object.values(users).map((e => e.nickname));
            o = {
                identifier: generateUniqueIdentifier(e),
                wallet: n,
                nickname: generateUniqueNickname(t),
                username: "",
                field1: null,
                field2: null,
                lastConnectionDate: (new Date).toISOString(),
                lastNicknameChange: "",
                avatar: "avatar.png",
                likesCount: 0,
                balancepvp: 0,
                balance: 0,
                mentionsCount: 0,
                bio: "",
                selectedTeam: null
            }, users[n] = o, writeJsonFile(USERS_FILE, users), console.log(`New user created: ${n}`)
        }
        connectedUsers[e.id] = o, e.emit("user_data", o)
    })), e.on("get_pvp_balances", (async t => {
        try {
            const n = await getPvpTokenBalance(t);
            e.emit("pvp_balances_response", {
                success: !0,
                tokenBalance: n.tokenBalance,
                gameWalletBalance: n.gameWalletBalance
            })
        } catch (t) {
            e.emit("pvp_balances_response", {
                success: !1,
                error: t.message
            })
        }
    })), e.on("team_alert", (t => {
        e.emit("team_update", {
            team: t.team,
            locked: !0,
            message: t.message
        }), e.emit("user_data", user)
    })), e.on("pump", (async e => {
        const {
            wallet: t,
            gameId: n,
            amount: o,
            transactionSignature: a
        } = e, s = users[t];
        if (!s || balloonState.gameEnded) return void console.log("User not found or game already ended:", t);
        if (100 * Math.random() < .7) {
            balloonState.gameEnded = !0, balloonState.size = 36, writeJsonFile(BALLOON_STATE_FILE, balloonState), io.emit("balloon_popped", {
                gameId: n,
                wallet: t,
                nickname: s.nickname || "Anonymous"
            });
            const e = {
                gameId: n,
                wallet: t,
                nickname: s.nickname || "Anonymous",
                action: `Pumped + popped (${o} SOL)`,
                transactionSignature: a,
                timestamp: (new Date).toISOString()
            };
            logActionP(e), setTimeout((() => {
                balloonState.gameEnded = !1, writeJsonFile(BALLOON_STATE_FILE, balloonState), io.emit("game_restarting", {
                    message: "Game restarting in 5 seconds..."
                })
            }), 5e3)
        } else {
            balloonState.size += 1, balloonState.lastPumpedBy = s.nickname, writeJsonFile(BALLOON_STATE_FILE, balloonState);
            balloonState.size, balloonState.lastPumpedBy, balloonState.gameEnded, balloonState.balloonProgress, balloonState.kingOfTheHillProgress, balloonState.kingOfTheHillWallet;
            const e = {
                gameId: n,
                wallet: t,
                nickname: s.nickname || "Anonymous",
                action: `Pumped (${o} SOL)`,
                transactionSignature: a,
                timestamp: (new Date).toISOString()
            };
            logActionP(e)
        }
    })), e.on("check_game_state", ((e, t) => {
        t(balloonState)
    })), e.on("create_comment", (async ({
        walletAddress: t,
        nonce: n,
        commentText: o
    }) => {
        await handleCreateTransaction(e, t, n, o, "comment_unsigned_transaction")
    })), e.on("create_reply", (async ({
        walletAddress: t,
        nonce: n,
        commentText: o,
        commentId: a
    }) => {
        if (!a) return e.emit("reply_error", {
            message: "Invalid comment ID for reply."
        });
        await handleCreateTransaction(e, t, n, o, "reply_unsigned_transaction", a)
    })), e.on("submit_signed_comment_transaction", (async ({
        signedTransaction: t,
        nonce: n,
        walletAddress: o,
        commentText: a
    }) => {
        await handleSignedTransaction(e, t, n, "new_comment", o, a)
    })), e.on("submit_signed_reply_transaction", (async ({
        signedTransaction: t,
        nonce: n,
        walletAddress: o,
        commentText: a,
        parentId: s
    }) => {
        await handleSignedTransaction(e, t, n, "new_reply", o, a, s)
    })), e.on("pump_create_transaction", (async ({
        solAmount: t,
        walletAddress: n,
        nonce: o,
        team: a
    }) => {
        if (hasAcceptedPvPGame(n)) e.emit("pvp_action_blocked");
        else if (isGameCompleting) e.emit("error", {
            message: "Game is completing. Pumping is temporarily blocked."
        });
        else try {
            await readBalloonState();
            if (isNaN(t) || t <= 0) return void e.emit("pump_error", {
                message: "Invalid SOL amount. It must be a positive number."
            });
            if (!n || !solanaWeb3.PublicKey.isOnCurve(n)) return void e.emit("pump_error", {
                message: "Invalid wallet address."
            });
            if (!a || !["1odd", "2even"].includes(a)) return void e.emit("pump_error", {
                message: "Invalid team selection. Please select a valid team."
            });
            readJsonFile(USERS_FILE)[n];
            if (await getLastNonce(n) >= o) return console.log("Replay attack detected for wallet:", n), void e.emit("pump_error", {
                message: "Invalid or reused nonce detected."
            });
            await saveNonce(n, o);
            const s = Math.round(t * LAMPORTS_PER_SOL),
                r = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed"),
                {
                    blockhash: i
                } = await r.getLatestBlockhash(),
                l = new solanaWeb3.Transaction({
                    recentBlockhash: i,
                    feePayer: new solanaWeb3.PublicKey(n)
                }).add(solanaWeb3.SystemProgram.transfer({
                    fromPubkey: new solanaWeb3.PublicKey(n),
                    toPubkey: new solanaWeb3.PublicKey(RECEIVER_WALLET),
                    lamports: s
                })).serialize({
                    requireAllSignatures: !1
                }).toString("base64");
            e.emit("pump_unsigned_transaction", {
                unsignedTransaction: l,
                team: a
            })
        } catch (t) {
            console.error("Failed to create transaction:", t), e.emit("pump_error", {
                message: "Failed to create unsigned transaction: " + t.message
            })
        }
    })),e.on("pump_submit_signed_transaction", (async ({
    signedTransaction: t,
    nonce: n,
    team: o
}) => {
    try {
        const a = Date.now(),
            elapsedTime = (a - n) / 1e3;

        console.log("Step 1: Checking transaction time elapsed:", {
            elapsedTime: elapsedTime,
            allowedTime: 11,
            currentTime: a,
            nonce: n
        });

        if (elapsedTime > 11) {
            console.error("Step 2: Transaction rejected due to timeout.");
            e.emit("pump_error", { message: "Transaction timed out." });
            return;
        }

        const r = solanaWeb3.Transaction.from(Buffer.from(t, "base64"));
        console.log("Transaction Object:", JSON.stringify(r, null, 2));

        const i = r.instructions.find((e => e.keys && e.keys.length > 0));
        if (!i) throw new Error("No valid instruction with keys found.");

        const walletAddress = i.keys[0].pubkey.toBase58();
        console.log("Wallet Address from Transaction:", walletAddress);

        const c = i.data.readBigUInt64LE(4),
            amountInSOL = Number(c) / LAMPORTS_PER_SOL;

        console.log(`Amount in SOL: ${amountInSOL}`);
        if (!o || !["1odd", "2even"].includes(o)) {
            e.emit("pump_error", { message: "Invalid team selection. Please select a valid team." });
            return;
        }

        const connection = new solanaWeb3.Connection(HELIUS_RPC_URL, "confirmed");

        console.log("Step 3: Sending raw transaction...");
        const transactionSignature = await connection.sendRawTransaction(Buffer.from(t, "base64"));

        let transactionConfirmed = false;
        let retries = 0;

        while (!transactionConfirmed && retries <= 36) {
            try {
                console.log(`Attempting to confirm transaction (Retry ${retries})`);
                const confirmation = await connection.confirmTransaction(transactionSignature, "confirmed");

                if (confirmation.value.err) {
                    throw new Error("Transaction failed: " + JSON.stringify(confirmation.value.err));
                }

                transactionConfirmed = true;
                console.log(`Transaction confirmed with signature: ${transactionSignature}`);
            } catch (err) {
                if (err.message.includes("Transaction was not confirmed in 30.00 seconds")) {
                    console.error("[Retry] Transaction not confirmed in 30 seconds. Retrying...");
                    retries++;
                    if (retries > 36) {
                        console.error("[Retry] Maximum retries reached. Transaction failed.");
                        throw err;
                    }
                    await new Promise(resolve => setTimeout(resolve, 1000));
                } else {
                    console.error("Transaction failed with an unexpected error:", err);
                    throw err;
                }
            }
        }

        const user = users[walletAddress];
        if (!user || balloonState.gameEnded) {
            console.log("User not found or game already ended:", walletAddress);
            return;
        }

        user.selectedTeam = o;
        writeJsonFile(USERS_FILE, users);
        console.log(`Updated selected team for wallet ${walletAddress}: ${o}`);

        if (100 * Math.random() < 0.7) {
            balloonState.gameEnded = true;
            balloonState.size = 36;
            writeJsonFile(BALLOON_STATE_FILE, balloonState);

            io.emit("balloon_popped", {
                gameId: 1,
                wallet: walletAddress,
                nickname: user.nickname || "Anonymous"
            });

            const logData = {
                gameId: 1,
                wallet: walletAddress,
                nickname: user.nickname || "Anonymous",
                action: `Pumped + popped (${amountInSOL} SOL)`,
                transactionSignature: transactionSignature,
                team: o,
                timestamp: new Date().toISOString()
            };

            logActionP(logData);

            setTimeout(() => {
                balloonState.gameEnded = false;
                writeJsonFile(BALLOON_STATE_FILE, balloonState);
                io.emit("game_restarting", { message: "Game restarting in 5 seconds..." });
            }, 5000);
        } else {
            balloonState.size += 1;
            balloonState.lastPumpedBy = user.nickname;
            writeJsonFile(BALLOON_STATE_FILE, balloonState);

            const logData = {
                gameId: 1,
                wallet: walletAddress,
                nickname: user.nickname || "Anonymous",
                action: `Pumped (${amountInSOL} SOL)`,
                transactionSignature: transactionSignature,
                team: o,
                timestamp: new Date().toISOString()
            };

            logActionP(logData);

            io.emit("balloon_state_updated", balloonState);
        }

        e.emit("pump_success", { transactionSignature: transactionSignature });
    } catch (err) {
        console.error("Failed to process signed transaction:", err);
        e.emit("pump_error", { message: "Transaction failed: " + err.message });
    }
})), e.on("request_team_info", (t => {
        const {
            walletAddress: n
        } = t;
        console.log(`Team info requested for wallet: ${n}`);
        const o = readJsonFile(USERS_FILE)[n];
        if (o && o.selectedTeam) {
            const t = {
                team: o.selectedTeam,
                locked: !0
            };
            console.log(`Sending team info response for wallet: ${n}`, t), e.emit("team_info_response", t)
        } else console.warn(`No team info found for wallet: ${n}`), e.emit("team_info_response", {
            team: null,
            locked: !1
        })
    })), e.on("dump", (async t => {
        const {
            wallet: n,
            solAmount: o,
            nonce: a
        } = t;
        if (hasAcceptedPvPGame(n)) return void e.emit("pvp_action_blocked");
        console.log("Step 1: Received dump event data:", t);
        if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(n)) return console.error("Step 2: Invalid wallet address:", n), void e.emit("error", {
            message: "Invalid wallet address"
        });
        if ("number" != typeof o || o <= 0) return console.error("Step 3: Invalid SOL amount:", o), void e.emit("error", {
            message: "Invalid SOL amount"
        });
        if ("number" != typeof a || a <= 0) return console.error("Step 4: Invalid nonce:", a), void e.emit("error", {
            message: "Invalid nonce"
        });
        console.log("Step 5: Input validation passed for wallet:", n);
        const s = Date.now(),
            r = walletDumpTimestamps[n];
        if (r) {
            const t = (s - r) / 1e3;
            if (t < 11) return console.error(`Step 6.1: Dump action too soon for wallet: ${n}. Time since last dump: ${t}s`), void e.emit("error", {
                message: `Dump action too soon. Please wait ${Math.ceil(11-t)} seconds.`
            })
        }
        walletDumpTimestamps[n] = s, console.log(`Step 6.2: Recorded dump timestamp for wallet: ${n}`);
        const i = Date.now();
        console.log("Step 7: Transaction start time recorded:", i);
        try {
            const t = path.join(__dirname, "users.json");
            console.log("Step 8: Reading users data from:", t);
            const s = await fs.promises.readFile(t, "utf-8"),
                r = JSON.parse(s),
                l = r[n]?.balance;
            if (console.log("Step 9: User balance fetched:", {
                    wallet: n,
                    balance: l,
                    gameEnded: balloonState.gameEnded
                }), !l || balloonState.gameEnded) return console.error("Step 10: User not found or game already ended:", n), void e.emit("error", {
                message: "User not found or game already ended"
            });
            try {
                const t = await getLastNonce(n);
                if (console.log("Step 11: Last nonce for wallet:", {
                        wallet: n,
                        lastNonce: t,
                        receivedNonce: a
                    }), t >= a) return console.error("Step 12: Replay attack detected for wallet:", {
                    wallet: n,
                    lastNonce: t,
                    receivedNonce: a
                }), void e.emit("error", {
                    message: "Invalid or reused nonce detected"
                });
                await saveNonce(n, a), console.log("Step 13: Nonce updated successfully for wallet:", n)
            } catch (t) {
                return console.error("Step 13.1: Failed to handle nonce for wallet:", n, t), void e.emit("error", {
                    message: "Failed to handle nonce"
                })
            }
            const c = o;
            if (l < o) return console.error("Step 14: User does not have enough balance:", {
                wallet: n,
                balance: l,
                required: o
            }), void e.emit("error", {
                message: "Insufficient balance"
            });
            console.log("Step 15: User has sufficient balance:", {
                wallet: n,
                balance: l,
                required: o
            });
            try {
                const t = await createUnsignedDumpTransaction(n, c, n);
                console.log("Step 16: Unsigned transaction created successfully for wallet:", n), e.emit("unsigned_transaction", {
                    unsignedTransaction: t,
                    transactionStartTime: i
                }), console.log("Step 17: Unsigned transaction emitted for wallet:", n), e.emit("dump_acknowledged", {
                    message: "Dump event successfully processed"
                }), console.log("Step 18: Dump event acknowledged for wallet:", n), delete walletDumpTimestamps[n], console.log(`Step 19: Cleared timestamp for wallet: ${n}`)
            } catch (t) {
                console.error("Step 20: Failed to create dump transaction for wallet:", n, t), e.emit("error", {
                    message: `Failed to create dump transaction: ${t.message}`
                })
            }
        } catch (t) {
            console.error("Step 21: Failed to fetch users data or process dump for wallet:", n, t), e.emit("error", {
                message: "Failed to fetch users data"
            })
        }
    })), e.on("dump_pvp", (async t => {
        const {
            wallet: n,
            splAmount: o,
            nonce: a
        } = t;
        if (hasAcceptedPvPGame(n)) return void e.emit("pvp_action_blocked_pvp");
        console.log("Step 1 (PvP): Received dump_pvp event data:", t);
        if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(n)) return console.error("Step 2 (PvP): Invalid wallet address:", n), void e.emit("error_pvp", {
            message: "Invalid wallet address"
        });
        if ("number" != typeof o || o <= 0) return console.error("Step 3 (PvP): Invalid SPL token amount:", o), void e.emit("error_pvp", {
            message: "Invalid PvP token amount"
        });
        if ("number" != typeof a || a <= 0) return console.error("Step 4 (PvP): Invalid nonce:", a), void e.emit("error_pvp", {
            message: "Invalid nonce"
        });
        console.log("Step 5 (PvP): Input validation passed for wallet:", n);
        const s = Date.now(),
            r = walletDumpTimestamps[n];
        if (r) {
            const t = (s - r) / 1e3;
            if (t < 47) return console.error(`Step 6.1 (PvP): Dump action too soon for wallet: ${n}. Time since last dump: ${t}s`), void e.emit("error_pvp", {
                message: `Dump action too soon. Please wait ${Math.ceil(47-t)} seconds.`
            })
        }
        walletDumpTimestamps[n] = s, console.log(`Step 6.2 (PvP): Recorded dump timestamp for wallet: ${n}`);
        const i = Date.now();
        console.log("Step 7 (PvP): Transaction start time recorded:", i);
        try {
            const t = path.join(__dirname, "users.json");
            console.log("Step 8 (PvP): Reading users data from:", t);
            const s = await fs.promises.readFile(t, "utf-8"),
                r = JSON.parse(s),
                l = r[n]?.balancepvp;
            if (console.log("Step 9 (PvP): User PvP balance fetched:", {
                    wallet: n,
                    balancepvp: l,
                    gameEnded: balloonState.gameEnded
                }), !l || balloonState.gameEnded) return console.error("Step 10 (PvP): User not found or game already ended:", n), void e.emit("error_pvp", {
                message: "User not found or game already ended"
            });
            try {
                const t = await getLastNonce(n, "pvp");
                if (console.log("Step 11 (PvP): Last nonce for wallet:", {
                        wallet: n,
                        lastNonce: t,
                        receivedNonce: a
                    }), t >= a) return console.error("Step 12 (PvP): Replay attack detected for wallet:", {
                    wallet: n,
                    lastNonce: t,
                    receivedNonce: a
                }), void e.emit("error_pvp", {
                    message: "Invalid or reused nonce detected"
                });
                await saveNonce(n, a, "pvp"), console.log("Step 13 (PvP): Nonce updated successfully for wallet:", n)
            } catch (t) {
                return console.error("Step 13.1 (PvP): Failed to handle nonce for wallet:", n, t), void e.emit("error_pvp", {
                    message: "Failed to handle nonce"
                })
            }
            const c = o;
            if (l < o) return console.error("Step 14 (PvP): User does not have enough PvP token balance:", {
                wallet: n,
                balancepvp: l,
                required: o
            }), void e.emit("error_pvp", {
                message: "Insufficient PvP token balance"
            });
            console.log("Step 15 (PvP): User has sufficient PvP token balance:", {
                wallet: n,
                balancepvp: l,
                required: o
            });
            try {
                const t = await createUnsignedDumpTransaction_pvp(n, c, n);
                console.log("Step 16 (PvP): Unsigned PvP SPL token transaction created successfully for wallet:", n), e.emit("unsigned_transaction_pvp", {
                    unsignedTransaction: t,
                    transactionStartTime: i
                }), console.log("Step 17 (PvP): Unsigned PvP SPL token transaction emitted for wallet:", n), e.emit("dump_acknowledged_pvp", {
                    message: "Dump PvP event successfully processed"
                }), console.log("Step 18 (PvP): Dump PvP event acknowledged for wallet:", n), delete walletDumpTimestamps[n], console.log(`Step 19 (PvP): Cleared timestamp for wallet: ${n}`)
            } catch (t) {
                console.error("Step 20 (PvP): Failed to create dump PvP SPL token transaction for wallet:", n, t), e.emit("error_pvp", {
                    message: `Failed to create PvP SPL token transaction: ${t.message}`
                })
            }
        } catch (t) {
            console.error("Step 21 (PvP): Failed to fetch users data or process dump PvP for wallet:", n, t), e.emit("error_pvp", {
                message: "Failed to fetch users data"
            })
        }
    }));
    const n = new Map;
e.on("submit_signed_transaction", (async t => {
    const {
        signedTransaction: o,
        transactionStartTime: a
    } = t;
    try {
        const t = Date.now();
        if ((t - a) / 1e3 > 47) {
            console.error("Transaction rejected due to exceeded timeout.");
            e.emit("error", { message: "Transaction timed out." });
            return;
        }

        const s = Transaction.from(Buffer.from(o, "base64")),
            r = s.instructions[0].keys[1].pubkey.toBase58();

        if (n.has(r)) {
            console.error("Transaction rejected: Wallet already processing another transaction");
            e.emit("error", { message: "Please wait until your previous transaction completes" });
            return;
        }

        n.set(r, !0);

        try {
            const [t, n] = await Promise.all([readBalloonState(), fs.promises.readFile(USERS_FILE, "utf-8")]);
            if (t.gameLocked) {
                console.error("Transaction rejected: Game is locked.");
                e.emit("error", { message: "Game is locked. Transactions are disabled." });
                return;
            }

            const a = s.instructions[0].data.slice(4, 12),
                i = Number(a.readBigUInt64LE()) / LAMPORTS_PER_SOL,
                l = JSON.parse(n),
                c = l[r];

            if (!c) throw new Error("User not found");
            if (c.balance < i) throw new Error("Insufficient balance");

            // Deduct balance immediately
            c.balance -= i;
            await fs.promises.writeFile(USERS_FILE, JSON.stringify(l, null, 2), "utf8");
            console.log(`Balance deducted immediately for wallet ${r}. Balance: ${c.balance}`);

            // Submit transaction and retry logic for status check
            const transactionSignature = await submitSignedTransaction(o);

            while (true) {
                const status = (await connection.getSignatureStatus(transactionSignature))?.value;

                if (status?.confirmationStatus === "confirmed") {
                    console.log(`Transaction confirmed: ${transactionSignature}`);

                    if (0 === c.balance || c.balance < 2e-4) {
                        c.selectedTeam = "";
                        console.log(`User's team reset to 0 as balance reached 0 for wallet: ${r}`);
                        handleBalance(c, r, e);
                    }

                    try {
                        await fs.promises.writeFile(USERS_FILE, JSON.stringify(l, null, 2), "utf8");
                        console.log(`users.json updated for wallet ${r} with balance=${c.balance}`);
                    } catch (e) {
                        console.error("Failed to write users.json:", e);
                    }

                    const logData = {
                        wallet: r,
                        nickname: c.nickname || "Anonymous",
                        action: `Dumped (${i} SOL)`,
                        transactionSignature: transactionSignature,
                        timestamp: (new Date).toISOString()
                    };

                    logAction(logData);
                    e.emit("dump_success", {
                        transactionSignature: transactionSignature,
                        balance: c.balance
                    });
                    break;
                }

                if (status?.err) {
                    console.error(`Transaction failed: ${transactionSignature}`);
                    c.balance += i; // Roll back balance deduction
                    await fs.promises.writeFile(USERS_FILE, JSON.stringify(l, null, 2), "utf8");
                    console.log(`Balance rolled back for wallet ${r}. Balance: ${c.balance}`);
                    e.emit("error", { message: "Transaction failed." });
                    break;
                }

                console.log("Transaction not yet confirmed. Retrying in 1 second...");
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

        } finally {
            n.delete(r);
        }
    } catch (t) {
        console.error("Failed to submit signed transaction:", t);
        e.emit("error", { message: `Transaction failed: ${t.message}` });

        const a = Transaction.from(Buffer.from(o, "base64")).instructions[0].keys[1].pubkey.toBase58();
        n.delete(a);
    }
}));e.on("submit_signed_transaction_pvp", (async t => {
    const {
        signedTransaction: o,
        transactionStartTime: a
    } = t;
    try {
        const t = Date.now();
        if ((t - a) / 1e3 > 47) {
            console.error("[PVP] Transaction rejected due to exceeded timeout.");
            e.emit("error_pvp", { message: "Transaction timed out." });
            return;
        }

        const s = Transaction.from(Buffer.from(o, "base64")).instructions.find((e => e.programId.toBase58() === TOKEN_PROGRAM_ID.toBase58()));
        if (!s) {
            e.emit("error_pvp", { message: "No SPL transfer instruction found." });
            return;
        }

        const r = s.keys[1].pubkey,
            i = (await connection.getParsedAccountInfo(r)).value.data.parsed.info.owner;

        if (n.has(i)) {
            console.error("[PVP] Transaction rejected: Wallet already processing another transaction");
            e.emit("error_pvp", { message: "Please wait until your previous transaction completes" });
            return;
        }

        n.set(i, !0);

        try {
            const [t, n] = await Promise.all([readBalloonState(), fs.promises.readFile(USERS_FILE, "utf-8")]);
            if (t.gameLocked) {
                e.emit("error_pvp", { message: "Game is locked. Transactions are disabled." });
                return;
            }

            const a = JSON.parse(n),
                r = a[i];

            if (!r) throw new Error("User not found");

            const l = s.data,
                c = Number(l.readBigUInt64LE(1)),
                d = 6,
                m = c / Math.pow(10, d);

            if ((r.balancepvp || 0) < m) {
                e.emit("error_pvp", { message: "Insufficient PvP token balance." });
                return;
            }

            // Deduct PvP balance immediately
            r.balancepvp = +(r.balancepvp - m).toFixed(6);
            await fs.promises.writeFile(USERS_FILE, JSON.stringify(a, null, 2), "utf8");
            console.log(`[PVP] Balance deducted immediately for wallet ${i}. Balancepvp: ${r.balancepvp}`);

            // Submit transaction and retry logic for status check
            const transactionSignature = await submitSignedTransaction_pvp(o);

            while (true) {
                const status = (await connection.getSignatureStatus(transactionSignature))?.value;

                if (status?.confirmationStatus === "confirmed") {
                    console.log(`[PVP] Transaction confirmed: ${transactionSignature}`);

                    try {
                        await fs.promises.writeFile(USERS_FILE, JSON.stringify(a, null, 2), "utf8");
                        console.log(`[PVP] users.json updated for wallet ${i} with balancepvp=${r.balancepvp}`);
                    } catch (e) {
                        console.error("[PVP] Failed to write users.json:", e);
                    }

                    const logData = {
                        wallet: i,
                        nickname: r.nickname || "Anonymous",
                        action: `Dumped (${m} PvP SPL)`,
                        transactionSignature: transactionSignature,
                        timestamp: (new Date).toISOString()
                    };

                    logAction(logData);
                    e.emit("dump_success_pvp", {
                        transactionSignature: transactionSignature,
                        balance: r.balancepvp
                    });
                    break;
                }

                if (status?.err) {
                    console.error(`[PVP] Transaction failed: ${transactionSignature}`);
                    r.balancepvp = +(r.balancepvp + m).toFixed(6); // Roll back PvP balance deduction
                    await fs.promises.writeFile(USERS_FILE, JSON.stringify(a, null, 2), "utf8");
                    console.log(`[PVP] Balance rolled back for wallet ${i}. Balancepvp: ${r.balancepvp}`);
                    e.emit("error_pvp", { message: "Transaction failed." });
                    break;
                }

                console.log("[PVP] Transaction not yet confirmed. Retrying in 1 second...");
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

        } finally {
            n.delete(i);
        }
    } catch (t) {
        console.error("[PVP] Failed to submit SPL PvP transaction:", t);
        e.emit("error_pvp", { message: `Transaction failed: ${t.message}` });

        try {
            const e = Transaction.from(Buffer.from(o, "base64")).instructions.find((e => e.programId.toBase58() === TOKEN_PROGRAM_ID.toBase58()));
            if (e) {
                const t = e.keys[2].pubkey.toBase58();
                n.delete(t);
            }
        } catch {}
    }
})); e.on("user_click", (t => {
        const n = Date.now(),
            o = userClickCooldowns.get(e.id);
        o && n - o < 470 && e.emit("click_rejected", {
            reason: "cooldown"
        })
    })), e.on("change_nickname", (t => {
        const {
            wallet: n,
            newNickname: o
        } = t, a = users[n];
        if (!a) return void e.emit("nickname_error", {
            message: "User not found."
        });
        const s = a.lastNicknameChange ? (Date.now() - new Date(a.lastNicknameChange)) / 1e3 : 1 / 0;
        s < 17 ? e.emit("nickname_error", {
            message: `You can only change your nickname once every 17 seconds. Please wait ${17-Math.floor(s)} seconds.`
        }) : (a.nickname = o, a.lastNicknameChange = (new Date).toISOString(), users[n] = a, writeJsonFile(USERS_FILE, users), io.emit("nickname_changed", {
            wallet: n,
            newNickname: o
        }), e.emit("nickname_changed", {
            nickname: o
        }))
    })), e.on("disconnect", (() => {
        const t = connectedUsers[e.id];
        t && (console.log(`Client disconnected: ${t.wallet} (Identifier: ${t.identifier}, Nickname: ${t.nickname})`), delete connectedUsers[e.id])
    }))
})), app.get("/api/users", ((e, t) => {
    const n = readJsonFile(USERS_FILE);
    t.json(n)
})), app.put("/api/users/update-nickname", internalOnly, ((e, t) => {
    const {
        wallet: n,
        newNickname: o,
        lastNicknameChange: a
    } = e.body;
    if (!n || !o || !a) return t.status(400).json({
        error: "Wallet, newNickname, and lastNicknameChange are required"
    });
    const s = readJsonFile(USERS_FILE);
    if (!s[n]) return t.status(404).json({
        error: "Wallet address not found"
    });
    s[n].nickname = o, s[n].lastNicknameChange = a, writeJsonFile(USERS_FILE, s), t.json({
        success: !0,
        message: `Nickname updated to ${o}`
    })
})), app.put("/api/users/update-avatar", internalOnly, ((e, t) => {
    const {
        wallet: n,
        avatar: o,
        lastUpdate: a
    } = e.body;
    if (!n || !o || !a) return t.status(400).json({
        error: "Wallet, avatar, and lastUpdate are required"
    });
    const s = readJsonFile(USERS_FILE);
    if (!s[n]) return t.status(404).json({
        error: "Wallet address not found"
    });
    const r = new Date(s[n].lastAvatarUpdate || s[n].lastUpdate);
    if ((new Date - r) / 1e3 < 86400) return t.status(429).json({
        error: "You can only change your avatar once every 24 hours."
    });
    s[n].avatar = o, s[n].lastAvatarUpdate = a, writeJsonFile(USERS_FILE, s), t.json({
        success: !0,
        message: "Avatar updated successfully"
    })
})), app.put("/api/users/update-bio", internalOnly, ((e, t) => {
    const {
        wallet: n,
        bio: o,
        lastUpdate: a
    } = e.body;
    if (!n || !o || !a) return t.status(400).json({
        error: "Wallet, bio, and lastUpdate are required"
    });
    const s = readJsonFile(USERS_FILE);
    if (!s[n]) return t.status(404).json({
        error: "Wallet address not found"
    });
    const r = new Date(s[n].lastBioUpdate || s[n].lastUpdate);
    if ((new Date - r) / 1e3 < 86400) return t.status(429).json({
        error: "You can only change your bio once every 24 hours."
    });
    s[n].bio = o, s[n].lastBioUpdate = a, writeJsonFile(USERS_FILE, s), t.json({
        success: !0,
        message: "Bio updated successfully"
    })
})), // Your existing app.get route, properly closed with no trailing comma
app.get("/get_nickname", (e, t) => {
    const n = e.query.wallet,
        o = readJsonFile(USERS_FILE)[n];
    if (o) {
        t.json({
            success: true,
            nickname: o.nickname
        });
    } else {
        t.json({
            success: false,
            message: "User not found"
        });
    }
});  // <-- notice this ends with semicolon, NOT comma

// Now start a new statement properly
const os = require('os');
const networkInterfaces = os.networkInterfaces();

function getLocalIP() {
  for (const interfaceDetails of Object.values(networkInterfaces)) {
    for (const details of interfaceDetails) {
      if (details.family === 'IPv4' && !details.internal) {
        return details.address;
      }
    }
  }
  return 'localhost';
}

const localIP = getLocalIP();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Server running on https://localhost:${PORT}`);
  console.log(`✅ Server also accessible on https://${localIP}:${PORT}`);
});
