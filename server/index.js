import express from "express";
import session from "express-session";
import helmet from "helmet";
import cors from "cors";
import bcrypt from "bcrypt";
import argon2 from "argon2";
import rateLimit from "express-rate-limit";
import { Server as SocketServer } from "socket.io";
import http from "http";
import fs from "fs";
import path from "path";
import multer from "multer";
import { fileURLToPath } from "url";
import crypto from "crypto";
import { ethers } from "ethers";
import { initDb, persistDb, getDb } from "./db.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);
const server = http.createServer(app);
const CLIENT_URL = process.env.CLIENT_URL || "http://localhost:5173";
const DEV_CLIENT_URL = "http://localhost:5173";
const allowedOrigins = [CLIENT_URL, DEV_CLIENT_URL].filter(Boolean);
const CHAIN_MODE = process.env.CHAIN_MODE || "mock";
const GAME_INVITE_TTL_MS = 2 * 60 * 1000;
const BLACKJACK_FACTORY_ABI = [
  "function createMatch(bytes32 matchId,address token,uint256 wager,address[] calldata players,uint64 depositDeadline) external",
  "function getMatch(bytes32 matchId) external view returns (address token,uint256 wager,uint64 depositDeadline,address winner,bool finalized,bool settled,address[] memory players,uint256[] memory deposits)",
  "function finalizeMatch(bytes32 matchId,address winner) external",
];
const MINI_GAME_FACTORY_ABI = [
  "function createMatch(bytes32 matchId,address token,uint256 fixedWager,bool variableDeposit,address[] calldata players,uint64 depositDeadline) external",
  "function getMatch(bytes32 matchId) external view returns (address token,uint256 fixedWager,uint64 depositDeadline,bool variableDeposit,address winner,bool finalized,bool settled,address[] memory players,uint256[] memory deposits,uint256 totalPot)",
  "function finalizeMatch(bytes32 matchId,address winner) external",
  "function refundMatch(bytes32 matchId) external",
];
const ERC20_ABI = [
  "function decimals() view returns (uint8)",
];
const BLACKJACK_CHAIN_CONFIG = {
  base: {
    rpcUrl: process.env.BASE_RPC_URL || "",
    factoryAddress: process.env.BLACKJACK_FACTORY_ADDRESS_BASE || "",
    miniGameFactoryAddress: process.env.MINIGAME_FACTORY_ADDRESS_BASE || "",
    explorerTxUrl: "https://basescan.org/tx/",
    tokens: {
      USDC: process.env.BLACKJACK_TOKEN_USDC_BASE || "",
      USDT: process.env.BLACKJACK_TOKEN_USDT_BASE || "",
      DAI: process.env.BLACKJACK_TOKEN_DAI_BASE || "",
    },
  },
  arbitrum: {
    rpcUrl: process.env.ARBITRUM_RPC_URL || "",
    factoryAddress: process.env.BLACKJACK_FACTORY_ADDRESS_ARBITRUM || "",
    miniGameFactoryAddress: process.env.MINIGAME_FACTORY_ADDRESS_ARBITRUM || "",
    explorerTxUrl: "https://arbiscan.io/tx/",
    tokens: {
      USDC: process.env.BLACKJACK_TOKEN_USDC_ARBITRUM || "",
      USDT: process.env.BLACKJACK_TOKEN_USDT_ARBITRUM || "",
      DAI: process.env.BLACKJACK_TOKEN_DAI_ARBITRUM || "",
    },
  },
  polygon: {
    rpcUrl: process.env.POLYGON_RPC_URL || "",
    factoryAddress: process.env.BLACKJACK_FACTORY_ADDRESS_POLYGON || "",
    miniGameFactoryAddress: process.env.MINIGAME_FACTORY_ADDRESS_POLYGON || "",
    explorerTxUrl: "https://polygonscan.com/tx/",
    tokens: {
      USDC: process.env.BLACKJACK_TOKEN_USDC_POLYGON || "",
      USDT: process.env.BLACKJACK_TOKEN_USDT_POLYGON || "",
      DAI: process.env.BLACKJACK_TOKEN_DAI_POLYGON || "",
    },
  },
};
const io = new SocketServer(server, {
  cors: {
    origin: allowedOrigins,
    credentials: true,
  },
});

const isProd = process.env.NODE_ENV === "production";

const sessionMiddleware = session({
  name: "xpchat.sid",
  secret: process.env.SESSION_SECRET || "dev-secret-change-me",
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: isProd ? "none" : "lax",
    secure: isProd,
    maxAge: 1000 * 60 * 60 * 24 * 7,
  },
});

app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: isProd
      ? {
          directives: {
            defaultSrc: ["'self'"],
            connectSrc: [
              "'self'",
              CLIENT_URL,
              CLIENT_URL.replace(/^http/, "ws"),
            ],
            imgSrc: ["'self'", "data:", "blob:"],
            mediaSrc: ["'self'", "data:", "blob:"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            scriptSrc: ["'self'"],
          },
        }
      : {
          directives: {
            defaultSrc: ["'self'"],
            connectSrc: ["'self'", "http://localhost:3001", "ws://localhost:3001", "http://localhost:5173", "ws://localhost:5173"],
            imgSrc: ["'self'", "data:", "blob:"],
            mediaSrc: ["'self'", "data:", "blob:"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
          },
        },
  })
);
app.use(
  cors({
    origin: (origin, cb) => {
      if (!origin) return cb(null, true);
      if (allowedOrigins.includes(origin)) return cb(null, true);
      return cb(new Error("Not allowed by CORS"));
    },
    credentials: true,
  })
);
app.use(express.json({ limit: "1mb" }));

function sanitizeObject(value) {
  if (value == null) return value;
  if (typeof value === "string") {
    const trimmed = value.replace(/\u0000/g, "").trim();
    return trimmed.length > 4000 ? trimmed.slice(0, 4000) : trimmed;
  }
  if (Array.isArray(value)) return value.map(sanitizeObject);
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = sanitizeObject(v);
    }
    return out;
  }
  return value;
}

app.use((req, _res, next) => {
  if (req.body && typeof req.body === "object") {
    req.body = sanitizeObject(req.body);
  }
  next();
});
app.use(sessionMiddleware);

const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 300,
  standardHeaders: "draft-7",
  legacyHeaders: false,
});
const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 15,
  standardHeaders: "draft-7",
  legacyHeaders: false,
});
app.use("/api/", apiLimiter);

function ensureCsrf(req) {
  if (!req.session.csrfToken) {
    req.session.csrfToken = crypto.randomBytes(32).toString("hex");
  }
  return req.session.csrfToken;
}

app.use((req, res, next) => {
  if (req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") {
    return next();
  }
  if (req.path === "/api/login" || req.path === "/api/register") {
    return next();
  }
  const token = req.get("x-csrf-token");
  if (!req.session?.userId || !token || token !== req.session.csrfToken) {
    return res.status(403).json({ error: "CSRF token missing or invalid" });
  }
  next();
});

const uploadDir = path.join(__dirname, "public", "uploads");
fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase();
      const safeExt = ext && ext.length <= 8 ? ext : "";
      const name = `${Date.now()}-${Math.random().toString(36).slice(2)}${safeExt}`;
      cb(null, name);
    },
  }),
  limits: { fileSize: 4 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (!file.mimetype?.startsWith("image/")) {
      return cb(new Error("Images only"));
    }
    cb(null, true);
  },
});

const storyUpload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase();
      const safeExt = ext && ext.length <= 8 ? ext : "";
      const name = `${Date.now()}-${Math.random().toString(36).slice(2)}${safeExt}`;
      cb(null, name);
    },
  }),
  limits: { fileSize: 20 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (file.mimetype?.startsWith("image/") || file.mimetype?.startsWith("video/")) {
      return cb(null, true);
    }
    return cb(new Error("Images or videos only"));
  },
});

const audioUpload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase();
      const safeExt = ext && ext.length <= 8 ? ext : "";
      const name = `${Date.now()}-${Math.random().toString(36).slice(2)}${safeExt}`;
      cb(null, name);
    },
  }),
  limits: { fileSize: 6 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (!file.mimetype?.startsWith("audio/")) {
      return cb(new Error("Audio only"));
    }
    cb(null, true);
  },
});

const ringtoneUpload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase();
      const safeExt = ext && ext.length <= 8 ? ext : "";
      const name = `${Date.now()}-${Math.random().toString(36).slice(2)}${safeExt}`;
      cb(null, name);
    },
  }),
  limits: { fileSize: 2 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (file.mimetype === "audio/mpeg" || file.mimetype === "audio/mp3") {
      return cb(null, true);
    }
    return cb(new Error("MP3 only"));
  },
});

const onlineUsers = new Map();
const activeCalls = new Map();
const pendingCalls = new Map();
const pendingUsers = new Set();
const callSessions = new Map();
const groupCalls = new Map();
const MAX_GROUP_MEMBERS = 10;
const MAX_USERNAME_ALIASES = 5;
const USERNAME_COOLDOWN_DAYS = 7;
const USERNAME_PRIMARY_COOLDOWN_MS = 60 * 60 * 1000;
const USERNAME_TRANSFER_COOLDOWN_MS = 5 * 60 * 1000;
const USERNAME_REMOVE_COOLDOWN_MS = 5 * 60 * 1000;
const USERNAME_CLAIM_COOLDOWN_MS = 5 * 60 * 1000;
const USERNAME_REGEX = /^[A-Za-z0-9_]+$/;
const USERNAME_MIN = 3;
const USERNAME_MAX = 14;
const USERNAME_COOLDOWN_ACTIONS = [
  "add_alias",
  "remove_alias",
  "transfer_out",
  "transfer_in",
  "change_primary",
];

function normalizePair(a, b) {
  return a < b ? [a, b] : [b, a];
}

function callKey(a, b) {
  return [a, b].sort((x, y) => x - y).join(":");
}

function formatCallDuration(totalSeconds) {
  const seconds = Math.max(0, Math.floor(totalSeconds));
  if (seconds < 60) return `${seconds} seconds`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) {
    return remainder
      ? `${minutes} minutes ${remainder} seconds`
      : `${minutes} minutes`;
  }
  const hours = Math.floor(minutes / 60);
  const leftoverMinutes = minutes % 60;
  return leftoverMinutes
    ? `${hours} hours ${leftoverMinutes} minutes`
    : `${hours} hours`;
}

const PASSWORD_PEPPER = process.env.PASSWORD_PEPPER || "";

async function hashPassword(raw) {
  return argon2.hash(`${raw}${PASSWORD_PEPPER}`, {
    type: argon2.argon2id,
    memoryCost: 2 ** 16,
    timeCost: 3,
    parallelism: 1,
  });
}

async function verifyPassword(raw, hash) {
  if (!hash) return false;
  if (hash.startsWith("$argon2")) {
    return argon2.verify(hash, `${raw}${PASSWORD_PEPPER}`);
  }
  return bcrypt.compare(raw, hash);
}

function serializeAudioStates(audioStates) {
  if (!audioStates) return {};
  const out = {};
  if (audioStates instanceof Map) {
    audioStates.forEach((value, key) => {
      out[key] = {
        muted: !!value?.muted,
        deafened: !!value?.deafened,
      };
    });
    return out;
  }
  Object.entries(audioStates).forEach(([key, value]) => {
    out[key] = {
      muted: !!value?.muted,
      deafened: !!value?.deafened,
    };
  });
  return out;
}

function emitCallActive(userA, userB, startedAt, audioStates = null) {
  io.to(`user:${userA}`).emit("call:active", {
    otherId: userB,
    startedAt,
    audioStates: serializeAudioStates(audioStates),
  });
  io.to(`user:${userB}`).emit("call:active", {
    otherId: userA,
    startedAt,
    audioStates: serializeAudioStates(audioStates),
  });
}

function getOne(sql, params = []) {
  const db = getDb();
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const row = stmt.step() ? stmt.getAsObject() : null;
  stmt.free();
  return row;
}

function getAll(sql, params = []) {
  const db = getDb();
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();
  return rows;
}

function run(sql, params = []) {
  const db = getDb();
  db.run(sql, params);
  const row = getOne("SELECT last_insert_rowid() AS id");
  persistDb();
  return { lastInsertRowid: row?.id };
}

function buildProfilePayload(userId) {
  const user = getOne(
    "SELECT id, username, display_name, custom_status, bio, email, email_verified, avatar, status, song_title, song_artist, song_album, song_cover_url, song_audio_url, song_source, song_source_url, song_updated_at FROM users WHERE id = ?",
    [userId]
  );
  if (!user) return null;
  const aliasMap = getAliasesForUsers([userId]);
  return { ...user, aliases: aliasMap.get(userId) || [] };
}

function parseJsonSafe(value, fallback) {
  if (!value) return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function areUsersFriends(userId, otherId) {
  const row = getOne(
    `
      SELECT id FROM friendships
      WHERE (user1_id = ? AND user2_id = ?)
         OR (user1_id = ? AND user2_id = ?)
    `,
    [userId, otherId, otherId, userId]
  );
  return !!row;
}

function normalizeBlackjackMatch(row) {
  if (!row) return null;
  return {
    ...row,
    players: parseJsonSafe(row.players_json, []),
    state: parseJsonSafe(row.state_json, null),
  };
}

function getBlackjackMatch(matchId) {
  const row = getOne("SELECT * FROM blackjack_matches WHERE id = ?", [matchId]);
  return normalizeBlackjackMatch(row);
}

function saveBlackjackMatch(match) {
  run(
    `
      UPDATE blackjack_matches
      SET status = ?, token_address = ?, escrow_factory_address = ?, escrow_match_id = ?, escrow_address = ?, invite_deadline = ?, deposit_deadline = ?, players_json = ?, state_json = ?,
          winner_id = ?, settlement_tx = ?, claim_address = ?, updated_at = ?
      WHERE id = ?
    `,
    [
      match.status,
      match.token_address || null,
      match.escrow_factory_address || null,
      match.escrow_match_id || null,
      match.escrow_address || null,
      match.invite_deadline || null,
      match.deposit_deadline || null,
      JSON.stringify(match.players || []),
      match.state ? JSON.stringify(match.state) : null,
      match.winner_id || null,
      match.settlement_tx || null,
      match.claim_address || null,
      nowIso(),
      match.id,
    ]
  );
}

function getMiniGameMatch(matchId) {
  const row = getOne("SELECT * FROM mini_game_matches WHERE id = ?", [matchId]);
  if (!row) return null;
  return {
    ...row,
    players: parseJsonSafe(row.players_json, []),
    state: parseJsonSafe(row.state_json, null),
  };
}

function saveMiniGameMatch(match) {
  run(
    `
      UPDATE mini_game_matches
      SET status = ?, token_address = ?, escrow_factory_address = ?, escrow_match_id = ?, escrow_address = ?, invite_deadline = ?, deposit_deadline = ?,
          players_json = ?, state_json = ?, winner_id = ?, settlement_tx = ?, claim_address = ?, updated_at = ?
      WHERE id = ?
    `,
    [
      match.status,
      match.token_address || null,
      match.escrow_factory_address || null,
      match.escrow_match_id || null,
      match.escrow_address || null,
      match.invite_deadline || null,
      match.deposit_deadline || null,
      JSON.stringify(match.players || []),
      match.state ? JSON.stringify(match.state) : null,
      match.winner_id || null,
      match.settlement_tx || null,
      match.claim_address || null,
      nowIso(),
      match.id,
    ]
  );
}

function shouldHideBigBankAmounts(match) {
  return (
    match?.game_type === "bigbank" &&
    match?.status !== "ended" &&
    match?.status !== "settled" &&
    match?.status !== "refunded"
  );
}

function sanitizeMiniGameMatch(match, viewerUserId = null) {
  if (!match) return null;
  const hideAmounts = shouldHideBigBankAmounts(match);
  const players = (match.players || []).map((player) => {
    if (!hideAmounts) return player;
    if (viewerUserId && player.user_id === viewerUserId) return player;
    return {
      ...player,
      deposit_amount: 0,
      deposited_amount: 0,
    };
  });
  return {
    id: match.id,
    game_type: match.game_type,
    inviter_id: match.inviter_id,
    status: match.status,
    chain: match.chain,
    token: match.token,
    token_address: match.token_address,
    wager_amount: match.wager_amount,
    escrow_factory_address: match.escrow_factory_address,
    escrow_match_id: match.escrow_match_id,
    escrow_address: match.escrow_address,
    invite_deadline: match.invite_deadline,
    deposit_deadline: match.deposit_deadline,
    players,
    state: match.state || null,
    winner_id: match.winner_id,
    settlement_tx: match.settlement_tx,
    claim_address: match.claim_address,
    created_at: match.created_at,
    updated_at: match.updated_at,
  };
}

function emitMiniGameUpdate(match) {
  (match.players || []).forEach((p) => {
    const payload = sanitizeMiniGameMatch(match, p.user_id);
    if (!payload) return;
    io.to(`user:${p.user_id}`).emit("minigame:state", payload);
  });
}

function getMiniGameMatchKey(matchId) {
  return ethers.id(`xp-minigame:${matchId}`);
}

function isExpiredIso(value) {
  if (!value) return false;
  const ts = new Date(value).getTime();
  return Number.isFinite(ts) && ts <= Date.now();
}

function getMiniGameFactoryContract(chain, withSigner = false) {
  const config = getBlackjackChainConfig(chain);
  if (!config?.miniGameFactoryAddress) {
    throw new Error(`MINIGAME_FACTORY_ADDRESS not configured for ${chain}`);
  }
  const runner = withSigner ? getBlackjackSigner(chain) : getBlackjackProvider(chain);
  return new ethers.Contract(config.miniGameFactoryAddress, MINI_GAME_FACTORY_ABI, runner);
}

async function createOnchainMiniGameMatch(match) {
  const players = (match.players || []).map((p) => p.wallet_address).filter(Boolean);
  if (players.length !== (match.players || []).length) {
    throw new Error("All players must register wallets before creating the escrow match");
  }
  const tokenAddress = match.token_address || resolveBlackjackToken(match.chain, match.token)?.address;
  if (!tokenAddress) {
    throw new Error(`Unsupported token ${match.token} on ${match.chain}`);
  }
  const decimals = await getTokenDecimals(match.chain, tokenAddress);
  const fixedWager =
    match.game_type === "coinflip"
      ? ethers.parseUnits(String(match.wager_amount), decimals)
      : 0n;
  const factory = getMiniGameFactoryContract(match.chain, true);
  const depositDeadline = Math.floor(new Date(match.deposit_deadline).getTime() / 1000);
  const escrowMatchId = match.escrow_match_id || getMiniGameMatchKey(match.id);
  const tx = await factory.createMatch(
    escrowMatchId,
    tokenAddress,
    fixedWager,
    match.game_type === "bigbank",
    players,
    depositDeadline
  );
  await tx.wait();
  const chainConfig = getBlackjackChainConfig(match.chain);
  match.token_address = tokenAddress;
  match.escrow_factory_address = chainConfig?.miniGameFactoryAddress || null;
  match.escrow_match_id = escrowMatchId;
  match.escrow_address = chainConfig?.miniGameFactoryAddress || null;
  match.settlement_tx = tx.hash;
  return match;
}

async function syncMiniGameOnchainState(match) {
  if (!match?.escrow_match_id || !match?.escrow_factory_address) return match;
  const factory = getMiniGameFactoryContract(match.chain, false);
  const result = await factory.getMatch(match.escrow_match_id);
  const players = result[7] || [];
  const deposits = result[8] || [];
  const decimals = await getTokenDecimals(match.chain, match.token_address);
  match.players = (match.players || []).map((player) => {
    const idx = players.findIndex(
      (wallet) => wallet && player.wallet_address && wallet.toLowerCase() === player.wallet_address.toLowerCase()
    );
    if (idx === -1) return player;
    const depositValue = deposits[idx] || 0n;
    return {
      ...player,
      deposited_amount: Number(ethers.formatUnits(depositValue, decimals)),
      status:
        match.status === "deposit" && depositValue > 0n
          ? "deposited"
          : player.status,
    };
  });
  if (result[5] && !match.winner_id) {
    const winnerWallet = String(result[4] || "").toLowerCase();
    const winner = (match.players || []).find(
      (player) => String(player.wallet_address || "").toLowerCase() === winnerWallet
    );
    if (winner) match.winner_id = winner.user_id;
  }
  if (result[6]) {
    match.status = "settled";
  }
  return match;
}

async function finalizeMiniGameOnchainMatch(match) {
  if (!match?.winner_id) throw new Error("Cannot finalize without a winner");
  const winner = (match.players || []).find((p) => p.user_id === match.winner_id);
  if (!winner?.wallet_address) throw new Error("Winner wallet is missing");
  const factory = getMiniGameFactoryContract(match.chain, true);
  const tx = await factory.finalizeMatch(match.escrow_match_id, winner.wallet_address);
  await tx.wait();
  match.settlement_tx = tx.hash;
  return match;
}

async function refundMiniGameOnchainMatch(match) {
  const factory = getMiniGameFactoryContract(match.chain, true);
  const tx = await factory.refundMatch(match.escrow_match_id);
  await tx.wait();
  match.settlement_tx = tx.hash;
  return match;
}

function resolveMiniGameWinner(match) {
  if (!match?.players?.length) return match;
  if (match.game_type === "coinflip") {
    if (!match.state?.result) {
      const side = Math.random() < 0.5 ? "heads" : "tails";
      const ordered = [...match.players];
      match.state = {
        ...(match.state || {}),
        coinSide: side,
        assignments: {
          [ordered[0].user_id]: "heads",
          [ordered[1].user_id]: "tails",
        },
        result: side,
      };
    }
    match.winner_id =
      (match.players || []).find((p) => match.state?.assignments?.[p.user_id] === match.state?.result)?.user_id || null;
  } else {
    const sorted = [...match.players].sort((a, b) => Number(b.deposited_amount || 0) - Number(a.deposited_amount || 0));
    const topAmount = Number(sorted[0]?.deposited_amount || 0);
    const runnerUp = Number(sorted[1]?.deposited_amount || 0);
    if (topAmount > 0 && topAmount === runnerUp) {
      match.winner_id = null;
      match.state = {
        ...(match.state || {}),
        result: "tie",
      };
    } else {
      match.winner_id = sorted[0]?.user_id || null;
      match.state = {
        ...(match.state || {}),
        result: "highest-deposit",
      };
    }
  }
  return match;
}

function beginMiniGameReveal(match) {
  match.status = "revealing";
  match.state = {
    ...(match.state || {}),
    revealEndsAt: new Date(Date.now() + 10 * 1000).toISOString(),
  };
  return match;
}

async function finalizeMiniGameAfterReveal(match) {
  if (!match || match.game_type !== "bigbank" || match.status !== "revealing") return match;
  if (!isExpiredIso(match.state?.revealEndsAt)) return match;
  match.status = "ended";
  resolveMiniGameWinner(match);
  if (!match.winner_id && match.game_type === "bigbank") {
    if (CHAIN_MODE === "evm") {
      await refundMiniGameOnchainMatch(match);
    }
    match.status = "refunded";
    match.state = { ...(match.state || {}), result: "tie" };
  } else {
    if (CHAIN_MODE === "evm") {
      await finalizeMiniGameOnchainMatch(match);
    }
    maybeNotifyMiniGameWinner(match);
  }
  return match;
}

function maybeNotifyMiniGameWinner(match) {
  if (!match?.winner_id) return;
  const winner = getOne("SELECT username FROM users WHERE id = ?", [match.winner_id]);
  const pot = (match.players || []).reduce(
    (sum, player) => sum + Number(player.deposited_amount || player.deposit_amount || 0),
    0
  );
  const payload = JSON.stringify({
    matchId: match.id,
    gameType: match.game_type,
    chain: match.chain,
    token: match.token,
    pot,
    winnerUsername: winner?.username || "winner",
  });
  const existing = getOne(
    "SELECT id FROM notifications WHERE user_id = ? AND type = 'minigame_claim' AND ref_id = ?",
    [match.winner_id, match.id]
  );
  if (!existing) {
    run(
      "INSERT INTO notifications (user_id, type, message, from_user_id, context, ref_id, payload) VALUES (?, 'minigame_claim', ?, ?, 'minigame', ?, ?)",
      [
        match.winner_id,
        `@${winner?.username || "winner"} has won ${pot.toFixed(2)} ${match.token}.`,
        match.inviter_id,
        match.id,
        payload,
      ]
    );
    io.to(`user:${match.winner_id}`).emit("notify:new");
  }
}

function cleanSongText(value, maxLen) {
  const trimmed = String(value || "").trim();
  if (!trimmed) return null;
  return trimmed.slice(0, maxLen);
}

function cleanSongUrl(value, maxLen = 500) {
  const trimmed = String(value || "").trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("/")) return trimmed.slice(0, maxLen);
  if (!/^https?:\/\//i.test(trimmed)) return null;
  return trimmed.slice(0, maxLen);
}

function parseSpotifyTitle(rawTitle) {
  if (!rawTitle) return { title: "", artist: "" };
  if (rawTitle.includes(" - ")) {
    const [title, artist] = rawTitle.split(" - ");
    return { title: title?.trim() || "", artist: artist?.trim() || "" };
  }
  if (rawTitle.includes(" • ")) {
    const [title, artist] = rawTitle.split(" • ");
    return { title: title?.trim() || "", artist: artist?.trim() || "" };
  }
  return { title: rawTitle.trim(), artist: "" };
}

async function fetchSpotifyOembed(sourceUrl) {
  if (!sourceUrl) return null;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch(
      `https://open.spotify.com/oembed?url=${encodeURIComponent(sourceUrl)}`,
      { signal: controller.signal }
    );
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function syncSpotifyConnectionSong(userId, spotifyUrl) {
  const sourceUrl = cleanSongUrl(spotifyUrl || "");
  if (!sourceUrl) {
    run(
      "UPDATE users SET song_title = NULL, song_artist = NULL, song_album = NULL, song_cover_url = NULL, song_audio_url = NULL, song_source = NULL, song_source_url = NULL, song_updated_at = ? WHERE id = ?",
      [new Date().toISOString(), userId]
    );
    return;
  }
  let title = "";
  let artist = "";
  let coverUrl = null;
  const meta = await fetchSpotifyOembed(sourceUrl);
  if (meta?.title) {
    const parsed = parseSpotifyTitle(meta.title);
    title = cleanSongText(parsed.title, 120);
    artist = cleanSongText(parsed.artist, 120);
  }
  if (!artist && meta?.author_name) {
    artist = cleanSongText(meta.author_name, 120);
  }
  if (meta?.thumbnail_url) {
    coverUrl = cleanSongUrl(meta.thumbnail_url);
  }
  run(
    "UPDATE users SET song_title = ?, song_artist = ?, song_album = NULL, song_cover_url = ?, song_audio_url = NULL, song_source = ?, song_source_url = ?, song_updated_at = ? WHERE id = ?",
    [
      title,
      artist,
      coverUrl,
      "spotify",
      sourceUrl,
      new Date().toISOString(),
      userId,
    ]
  );
}

const BJ_SUITS = ["H", "D", "C", "S"];
const BJ_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"];

function createDeck() {
  const deck = [];
  BJ_SUITS.forEach((suit) => {
    BJ_RANKS.forEach((rank) => deck.push(`${rank}${suit}`));
  });
  // Add a second pass of lower cards so rounds breathe a bit more while
  // keeping both players on the exact same randomized shoe.
  BJ_SUITS.forEach((suit) => {
    ["2", "3", "4", "5", "6", "7"].forEach((rank) => deck.push(`${rank}${suit}`));
  });
  return deck;
}

function shuffleDeck(deck) {
  const arr = [...deck];
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const rand = crypto.randomBytes(4).readUInt32BE(0);
    const j = rand % (i + 1);
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function cardValue(rank) {
  if (rank === "A") return 11;
  if (["K", "Q", "J"].includes(rank)) return 10;
  return Number(rank);
}

function handValue(cards) {
  let total = 0;
  let aces = 0;
  cards.forEach((card) => {
    const rank = card.slice(0, -1);
    total += cardValue(rank);
    if (rank === "A") aces += 1;
  });
  while (total > 21 && aces > 0) {
    total -= 10;
    aces -= 1;
  }
  return total;
}

function isBlackjack(cards) {
  return cards.length === 2 && handValue(cards) === 21;
}

function canSplit(hand) {
  if (!hand || hand.cards.length !== 2) return false;
  const rankA = hand.cards[0].slice(0, -1);
  const rankB = hand.cards[1].slice(0, -1);
  return rankA === rankB;
}

function dealCard(state) {
  if (!state.deck || state.deck.length === 0) {
    state.deck = shuffleDeck(createDeck());
  }
  return state.deck.pop();
}

function initBlackjackState(playerIds, wagerAmount) {
  const deck = shuffleDeck(createDeck());
  const state = {
    deck,
    dealer: { cards: [], status: "hidden" },
    players: {},
    order: playerIds,
    currentPlayerId: playerIds[0],
    phase: "player",
    results: {},
    message: "",
  };
  playerIds.forEach((id) => {
    state.players[id] = {
      hands: [{ cards: [], bet: wagerAmount, status: "playing" }],
      activeHand: 0,
    };
  });
  for (let i = 0; i < 2; i += 1) {
    playerIds.forEach((id) => {
      state.players[id].hands[0].cards.push(dealCard(state));
    });
    state.dealer.cards.push(dealCard(state));
  }
  playerIds.forEach((id) => {
    const hand = state.players[id].hands[0];
    if (isBlackjack(hand.cards)) {
      hand.status = "blackjack";
    }
  });
  advanceToNextHand(state);
  return state;
}

function advanceToNextHand(state) {
  for (let i = 0; i < state.order.length; i += 1) {
    const playerId = state.order[i];
    const player = state.players[playerId];
    for (let h = 0; h < player.hands.length; h += 1) {
      const hand = player.hands[h];
      if (hand.status === "playing") {
        player.activeHand = h;
        state.currentPlayerId = playerId;
        state.phase = "player";
        return;
      }
    }
  }
  state.currentPlayerId = null;
  state.phase = "dealer";
}

function finalizeDealer(state) {
  state.dealer.status = "reveal";
  while (handValue(state.dealer.cards) < 17) {
    state.dealer.cards.push(dealCard(state));
  }
}

function settleRound(state) {
  finalizeDealer(state);
  const dealerValue = handValue(state.dealer.cards);
  const dealerBust = dealerValue > 21;
  const results = {};
  state.order.forEach((playerId) => {
    const player = state.players[playerId];
    results[playerId] = player.hands.map((hand) => {
      if (hand.status === "bust") return { result: "lose", value: handValue(hand.cards) };
      const value = handValue(hand.cards);
      if (hand.status === "blackjack" && !dealerBust && dealerValue !== 21) {
        return { result: "win", value };
      }
      if (dealerBust) return { result: "win", value };
      if (value > dealerValue) return { result: "win", value };
      if (value < dealerValue) return { result: "lose", value };
      return { result: "push", value };
    });
  });
  state.results = results;
  state.phase = "settled";
}

function applyPlayerAction(state, playerId, action) {
  const player = state.players[playerId];
  if (!player) return { ok: false, error: "Player not found" };
  const hand = player.hands[player.activeHand];
  if (!hand || hand.status !== "playing") return { ok: false, error: "Invalid hand" };
  if (action === "hit") {
    hand.cards.push(dealCard(state));
    if (handValue(hand.cards) > 21) {
      hand.status = "bust";
    }
  } else if (action === "stand") {
    hand.status = "stand";
  } else if (action === "double") {
    if (hand.cards.length !== 2) return { ok: false, error: "Double not allowed" };
    hand.bet *= 2;
    hand.cards.push(dealCard(state));
    if (handValue(hand.cards) > 21) {
      hand.status = "bust";
    } else {
      hand.status = "stand";
    }
  } else if (action === "split") {
    if (!canSplit(hand)) return { ok: false, error: "Split not allowed" };
    const [first, second] = hand.cards;
    hand.cards = [first];
    const newHand = { cards: [second], bet: hand.bet, status: "playing" };
    hand.cards.push(dealCard(state));
    newHand.cards.push(dealCard(state));
    player.hands.splice(player.activeHand + 1, 0, newHand);
  } else {
    return { ok: false, error: "Unknown action" };
  }
  advanceToNextHand(state);
  if (state.phase === "dealer") {
    settleRound(state);
  }
  return { ok: true };
}

function calculateWinnerFromState(state) {
  if (!state?.results) return null;
  const scores = state.order.map((playerId) => {
    const list = state.results[playerId] || [];
    const net = list.reduce((sum, r) => sum + (r.result === "win" ? 1 : r.result === "lose" ? -1 : 0), 0);
    return { playerId, net };
  });
  scores.sort((a, b) => b.net - a.net);
  if (scores.length === 0) return null;
  if (scores.length > 1 && scores[0].net === scores[1].net) return null;
  return scores[0].playerId;
}

function sanitizeBlackjackState(state) {
  if (!state) return null;
  const { deck, ...rest } = state;
  return { ...rest, deckCount: Array.isArray(deck) ? deck.length : 0 };
}

function sanitizeBlackjackMatch(match) {
  if (!match) return null;
  return {
    id: match.id,
    inviter_id: match.inviter_id,
    status: match.status,
    chain: match.chain,
    token: match.token,
    token_address: match.token_address,
    wager_amount: match.wager_amount,
    escrow_factory_address: match.escrow_factory_address,
    escrow_match_id: match.escrow_match_id,
    escrow_address: match.escrow_address,
    invite_deadline: match.invite_deadline,
    deposit_deadline: match.deposit_deadline,
    players: match.players || [],
    state: sanitizeBlackjackState(match.state),
    winner_id: match.winner_id,
    settlement_tx: match.settlement_tx,
    claim_address: match.claim_address,
    created_at: match.created_at,
    updated_at: match.updated_at,
  };
}

function emitBlackjackUpdate(match) {
  const payload = sanitizeBlackjackMatch(match);
  if (!payload) return;
  (match.players || []).forEach((p) => {
    io.to(`user:${p.user_id}`).emit("blackjack:state", payload);
  });
}

function createMockEscrowAddress() {
  return `0x${crypto.randomBytes(20).toString("hex")}`;
}

function getBlackjackMatchKey(matchId) {
  return ethers.id(`xp-blackjack:${matchId}`);
}

async function createOnchainBlackjackMatch(match) {
  const players = (match.players || []).map((p) => p.wallet_address).filter(Boolean);
  if (players.length !== (match.players || []).length) {
    throw new Error("All players must register wallets before creating the escrow match");
  }
  const chainConfig = getBlackjackChainConfig(match.chain);
  const tokenAddress = match.token_address || resolveBlackjackToken(match.chain, match.token)?.address;
  if (!tokenAddress) {
    throw new Error(`Unsupported token ${match.token} on ${match.chain}`);
  }
  const decimals = await getTokenDecimals(match.chain, tokenAddress);
  const wager = ethers.parseUnits(String(match.wager_amount), decimals);
  const factory = getBlackjackFactoryContract(match.chain, true);
  const depositDeadline = Math.floor(new Date(match.deposit_deadline).getTime() / 1000);
  const escrowMatchId = match.escrow_match_id || getBlackjackMatchKey(match.id);
  const tx = await factory.createMatch(
    escrowMatchId,
    tokenAddress,
    wager,
    players,
    depositDeadline
  );
  await tx.wait();
  match.token_address = tokenAddress;
  match.escrow_factory_address = chainConfig?.factoryAddress || null;
  match.escrow_match_id = escrowMatchId;
  match.escrow_address = chainConfig?.factoryAddress || null;
  match.settlement_tx = tx.hash;
  return match;
}

async function syncBlackjackOnchainState(match) {
  if (!match?.escrow_match_id || !match?.escrow_factory_address) return match;
  const factory = getBlackjackFactoryContract(match.chain, false);
  const result = await factory.getMatch(match.escrow_match_id);
  const players = result[6] || [];
  const deposits = result[7] || [];
  match.players = (match.players || []).map((player) => {
    const idx = players.findIndex(
      (wallet) => wallet && player.wallet_address && wallet.toLowerCase() === player.wallet_address.toLowerCase()
    );
    if (idx === -1) return player;
    const deposited = deposits[idx] ? Number(deposits[idx] > 0n) : 0;
    return {
      ...player,
      deposited_amount: deposited ? match.wager_amount : 0,
      status:
        match.status === "deposit" && deposited
          ? "deposited"
          : player.status,
    };
  });
  const allDeposited = (match.players || []).every((player) => player.deposited_amount >= match.wager_amount);
  if (match.status === "deposit" && allDeposited) {
    match.status = "active";
    match.state = initBlackjackState(
      match.players.map((p) => p.user_id),
      match.wager_amount
    );
  }
  if (result[4] && !match.winner_id) {
    const winnerWallet = String(result[3] || "").toLowerCase();
    const winner = (match.players || []).find(
      (player) => String(player.wallet_address || "").toLowerCase() === winnerWallet
    );
    if (winner) {
      match.winner_id = winner.user_id;
    }
  }
  if (result[5]) {
    match.status = "settled";
  }
  return match;
}

async function finalizeBlackjackOnchainMatch(match) {
  if (!match?.winner_id) {
    throw new Error("Cannot finalize without a winner");
  }
  const winner = (match.players || []).find((p) => p.user_id === match.winner_id);
  if (!winner?.wallet_address) {
    throw new Error("Winner wallet is missing");
  }
  const factory = getBlackjackFactoryContract(match.chain, true);
  const tx = await factory.finalizeMatch(match.escrow_match_id, winner.wallet_address);
  await tx.wait();
  match.settlement_tx = tx.hash;
  return match;
}

function getBlackjackChainConfig(chain) {
  return BLACKJACK_CHAIN_CONFIG[String(chain || "").toLowerCase()] || null;
}

function resolveBlackjackToken(chain, token) {
  const config = getBlackjackChainConfig(chain);
  const symbol = String(token || "").toUpperCase();
  if (!config) return null;
  const address = config.tokens?.[symbol] || "";
  if (!address) return null;
  return { symbol, address };
}

function getBlackjackProvider(chain) {
  const config = getBlackjackChainConfig(chain);
  if (!config?.rpcUrl) {
    throw new Error(`RPC not configured for ${chain}`);
  }
  return new ethers.JsonRpcProvider(config.rpcUrl);
}

function getBlackjackSigner(chain) {
  const pk = process.env.BLACKJACK_OPERATOR_PRIVATE_KEY || "";
  if (!pk) {
    throw new Error("BLACKJACK_OPERATOR_PRIVATE_KEY is not configured");
  }
  return new ethers.Wallet(pk, getBlackjackProvider(chain));
}

function getBlackjackFactoryContract(chain, withSigner = false) {
  const config = getBlackjackChainConfig(chain);
  if (!config?.factoryAddress) {
    throw new Error(`BLACKJACK_FACTORY_ADDRESS not configured for ${chain}`);
  }
  const runner = withSigner ? getBlackjackSigner(chain) : getBlackjackProvider(chain);
  return new ethers.Contract(config.factoryAddress, BLACKJACK_FACTORY_ABI, runner);
}

async function getTokenDecimals(chain, tokenAddress) {
  const token = new ethers.Contract(tokenAddress, ERC20_ABI, getBlackjackProvider(chain));
  return Number(await token.decimals());
}

function formatEvmError(err, fallback) {
  return err?.shortMessage || err?.reason || err?.message || fallback;
}


function emitProfileUpdate(userId) {
  const payload = buildProfilePayload(userId);
  if (payload) {
    io.emit("profile:update", payload);
  }
  return payload;
}

function emitSystemDmMessage(senderId, recipientId, body, type = "system") {
  const info = run(
    "INSERT INTO messages (sender_id, recipient_id, body, type) VALUES (?, ?, ?, ?)",
    [senderId, recipientId, body.slice(0, 2000), type]
  );
  const message = {
    id: info.lastInsertRowid,
    sender_id: senderId,
    recipient_id: recipientId,
    body: body.slice(0, 2000),
    type,
    image_url: null,
    audio_url: null,
    created_at: new Date().toISOString(),
    edited_at: null,
    deleted_at: null,
  };
  io.to(`user:${senderId}`).emit("dm:new", message);
  io.to(`user:${recipientId}`).emit("dm:new", message);
}

function emitSystemDmMessageTo(senderId, recipientId, body, type = "system") {
  const info = run(
    "INSERT INTO messages (sender_id, recipient_id, body, type) VALUES (?, ?, ?, ?)",
    [senderId, recipientId, body.slice(0, 2000), type]
  );
  const message = {
    id: info.lastInsertRowid,
    sender_id: senderId,
    recipient_id: recipientId,
    body: body.slice(0, 2000),
    type,
    image_url: null,
    audio_url: null,
    created_at: new Date().toISOString(),
    edited_at: null,
    deleted_at: null,
  };
  io.to(`user:${recipientId}`).emit("dm:new", message);
}

function emitSystemGroupMessage(groupId, senderId, body, type = "call") {
  const info = run(
    "INSERT INTO group_messages (group_id, sender_id, body, type, image_url, is_system) VALUES (?, ?, ?, ?, NULL, 1)",
    [groupId, senderId, body.slice(0, 2000), type]
  );
  const message = {
    id: info.lastInsertRowid,
    group_id: groupId,
    sender_id: senderId,
    body: body.slice(0, 2000),
    type,
    image_url: null,
    audio_url: null,
    is_system: 1,
    created_at: new Date().toISOString(),
    edited_at: null,
    deleted_at: null,
  };
  io.to(`group:${groupId}`).emit("group:new", message);
}

function getStoryStatusMap(viewerId, userIds) {
  if (!userIds.length) return new Map();
  const placeholders = userIds.map(() => "?").join(",");
  const rows = getAll(
    `
    SELECT s.user_id,
           COUNT(*) AS story_count,
           SUM(CASE WHEN sv.id IS NULL THEN 1 ELSE 0 END) AS unviewed_count
    FROM stories s
    LEFT JOIN story_views sv
      ON sv.story_id = s.id AND sv.viewer_id = ?
    WHERE s.user_id IN (${placeholders})
      AND datetime(s.expires_at) > datetime('now')
    GROUP BY s.user_id
    `,
    [viewerId, ...userIds]
  );
  const map = new Map();
  rows.forEach((row) => {
    map.set(row.user_id, {
      count: row.story_count || 0,
      unviewed: row.unviewed_count || 0,
    });
  });
  return map;
}

function normalizeUsername(value) {
  return (value || "").trim();
}

function isValidUsername(value) {
  const name = normalizeUsername(value);
  return (
    name.length >= USERNAME_MIN &&
    name.length <= USERNAME_MAX &&
    USERNAME_REGEX.test(name)
  );
}

function findUserByUsernameOrAlias(username) {
  if (!username) return null;
  const direct = getOne(
    "SELECT * FROM users WHERE username = ? COLLATE NOCASE",
    [username]
  );
  if (direct) return direct;
  const alias = getOne(
    `
    SELECT u.*
    FROM username_aliases ua
    JOIN users u ON u.id = ua.user_id
    WHERE ua.username = ? COLLATE NOCASE
    `,
    [username]
  );
  return alias || null;
}

function isUsernameAvailable(username) {
  const name = normalizeUsername(username);
  if (!isValidUsername(name)) return false;
  const existing = getOne(
    "SELECT id FROM users WHERE username = ? COLLATE NOCASE",
    [name]
  );
  if (existing) return false;
  const alias = getOne(
    "SELECT id FROM username_aliases WHERE username = ? COLLATE NOCASE",
    [name]
  );
  if (alias) return false;
  const pending = getOne(
    "SELECT id FROM username_transfers WHERE username = ? COLLATE NOCASE AND status = 'pending' AND datetime(expires_at) > datetime('now')",
    [name]
  );
  return !pending;
}

function getAliasesForUsers(userIds) {
  if (!userIds.length) return new Map();
  const placeholders = userIds.map(() => "?").join(",");
  const rows = getAll(
    `SELECT user_id, username FROM username_aliases WHERE user_id IN (${placeholders}) ORDER BY created_at ASC`,
    userIds
  );
  const map = new Map();
  rows.forEach((row) => {
    const list = map.get(row.user_id) || [];
    list.push(row.username);
    map.set(row.user_id, list);
  });
  return map;
}

function getCooldownUntil(userId, actions = USERNAME_COOLDOWN_ACTIONS) {
  let sql = "SELECT created_at FROM username_actions WHERE user_id = ?";
  const params = [userId];
  if (actions && actions.length) {
    const placeholders = actions.map(() => "?").join(",");
    sql += ` AND action IN (${placeholders})`;
    params.push(...actions);
  }
  sql += " ORDER BY datetime(created_at) DESC LIMIT 1";
  const row = getOne(sql, params);
  if (!row?.created_at) return null;
  const last = new Date(row.created_at.replace(" ", "T"));
  if (Number.isNaN(last.getTime())) return null;
  const until = new Date(last.getTime() + USERNAME_CLAIM_COOLDOWN_MS);
  if (until.getTime() <= Date.now()) return null;
  return until.toISOString();
}

function getCooldownUntilFor(userId, actions, cooldownMs) {
  const row = getOne(
    `SELECT created_at FROM username_actions WHERE user_id = ? AND action IN (${actions
      .map(() => "?")
      .join(",")}) ORDER BY datetime(created_at) DESC LIMIT 1`,
    [userId, ...actions]
  );
  if (!row?.created_at) return null;
  const last = new Date(row.created_at.replace(" ", "T"));
  if (Number.isNaN(last.getTime())) return null;
  const until = new Date(last.getTime() + cooldownMs);
  if (until.getTime() <= Date.now()) return null;
  return until.toISOString();
}

function recordUsernameAction(userId, action) {
  run("INSERT INTO username_actions (user_id, action) VALUES (?, ?)", [
    userId,
    action,
  ]);
}

function expireUsernameTransfers() {
  run(
    "UPDATE username_transfers SET status = 'expired' WHERE status = 'pending' AND datetime(expires_at) <= datetime('now')"
  );
}

function getLastMessageBetween(userId, otherId) {
  return getOne(
    `
      SELECT id, sender_id, recipient_id, body, type, image_url, audio_url, story_id, story_owner_id, story_media_url, story_media_type, forwarded_from_id, forwarded_from_username, forwarded_from_display, created_at, edited_at
      FROM messages
      WHERE (sender_id = ? AND recipient_id = ?)
         OR (sender_id = ? AND recipient_id = ?)
        AND deleted_at IS NULL
      ORDER BY datetime(created_at) DESC
      LIMIT 1
    `,
    [userId, otherId, otherId, userId]
  );
}

function getDmUnreadCount(userId, otherId) {
  const readRow = getOne(
    "SELECT last_read_at FROM dm_reads WHERE user_id = ? AND other_id = ?",
    [userId, otherId]
  );
  if (!readRow) {
    const row = getOne(
      "SELECT COUNT(*) AS count FROM messages WHERE sender_id = ? AND recipient_id = ? AND deleted_at IS NULL",
      [otherId, userId]
    );
    return row?.count || 0;
  }
  const row = getOne(
    "SELECT COUNT(*) AS count FROM messages WHERE sender_id = ? AND recipient_id = ? AND deleted_at IS NULL AND datetime(created_at) > datetime(?)",
    [otherId, userId, readRow.last_read_at]
  );
  return row?.count || 0;
}

function getDmReadAt(userId, otherId) {
  const readRow = getOne(
    "SELECT last_read_at FROM dm_reads WHERE user_id = ? AND other_id = ?",
    [userId, otherId]
  );
  return readRow?.last_read_at || null;
}

function getGroupLastMessage(groupId) {
  return getOne(
    `
      SELECT id, sender_id, body, type, image_url, audio_url, forwarded_from_id, forwarded_from_username, forwarded_from_display, created_at, edited_at, is_system
      FROM group_messages
      WHERE group_id = ? AND deleted_at IS NULL
      ORDER BY datetime(created_at) DESC
      LIMIT 1
    `,
    [groupId]
  );
}

function getGroupUnreadCount(userId, groupId) {
  const readRow = getOne(
    "SELECT last_read_at FROM group_reads WHERE user_id = ? AND group_id = ?",
    [userId, groupId]
  );
  if (!readRow) {
    const row = getOne(
      "SELECT COUNT(*) AS count FROM group_messages WHERE group_id = ? AND sender_id <> ? AND deleted_at IS NULL",
      [groupId, userId]
    );
    return row?.count || 0;
  }
  const row = getOne(
    "SELECT COUNT(*) AS count FROM group_messages WHERE group_id = ? AND sender_id <> ? AND deleted_at IS NULL AND datetime(created_at) > datetime(?)",
    [groupId, userId, readRow.last_read_at]
  );
  return row?.count || 0;
}

function getReactions(messageType, messageIds, viewerId) {
  if (!messageIds.length) return {};
  const placeholders = messageIds.map(() => "?").join(",");
  const rows = getAll(
    `
      SELECT message_id, emoji,
        COUNT(*) AS count,
        SUM(CASE WHEN user_id = ? THEN 1 ELSE 0 END) AS me
      FROM message_reactions
      WHERE message_type = ? AND message_id IN (${placeholders})
      GROUP BY message_id, emoji
    `,
    [viewerId, messageType, ...messageIds]
  );
  const map = {};
  rows.forEach((row) => {
    const list = map[row.message_id] || [];
    list.push({
      emoji: row.emoji,
      count: row.count,
      byMe: Number(row.me) > 0,
    });
    map[row.message_id] = list;
  });
  return map;
}

function upsertDmRead(userId, otherId) {
  const existing = getOne(
    "SELECT id FROM dm_reads WHERE user_id = ? AND other_id = ?",
    [userId, otherId]
  );
  if (existing) {
    run("UPDATE dm_reads SET last_read_at = CURRENT_TIMESTAMP WHERE id = ?", [
      existing.id,
    ]);
  } else {
    run(
      "INSERT INTO dm_reads (user_id, other_id, last_read_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
      [userId, otherId]
    );
  }
}

function upsertGroupRead(userId, groupId) {
  const existing = getOne(
    "SELECT id FROM group_reads WHERE user_id = ? AND group_id = ?",
    [userId, groupId]
  );
  if (existing) {
    run("UPDATE group_reads SET last_read_at = CURRENT_TIMESTAMP WHERE id = ?", [
      existing.id,
    ]);
  } else {
    run(
      "INSERT INTO group_reads (user_id, group_id, last_read_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
      [userId, groupId]
    );
  }
}

function extractMentions(body) {
  const mentions = new Set();
  const regex = /@([A-Za-z0-9_]+)/g;
  let match;
  while ((match = regex.exec(body)) !== null) {
    mentions.add(match[1]);
  }
  return Array.from(mentions);
}

function withinTwoMinutes(createdAt) {
  if (!createdAt) return false;
  let parsed;
  if (typeof createdAt === "string" && !createdAt.includes("T")) {
    parsed = new Date(createdAt.replace(" ", "T") + "Z");
  } else {
    parsed = new Date(createdAt);
  }
  if (Number.isNaN(parsed.getTime())) {
    return false;
  }
  return Date.now() - parsed.getTime() <= 2 * 60 * 1000;
}

function isFriends(userId, otherId) {
  const [u1, u2] = normalizePair(userId, otherId);
  const row = getOne(
    "SELECT id FROM friendships WHERE user1_id = ? AND user2_id = ?",
    [u1, u2]
  );
  return Boolean(row);
}

function canViewConnection(visibility, ownerId, viewerId) {
  if (ownerId === viewerId) return true;
  if (visibility === "public") return true;
  if (visibility === "friends") {
    return isFriends(ownerId, viewerId);
  }
  return false;
}

function requireAuth(req, res, next) {
  if (!req.session?.userId) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

app.post("/api/register", authLimiter, async (req, res) => {
  const { username, password, email, displayName } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: "Username and password required" });
  }
  const cleanUsername = normalizeUsername(username);
  if (!isValidUsername(cleanUsername)) {
    return res.status(400).json({ error: "Invalid username" });
  }
  if (!isUsernameAvailable(cleanUsername)) {
    return res.status(409).json({ error: "Username already taken" });
  }
  const hash = await hashPassword(password);
  const info = run(
    "INSERT INTO users (username, display_name, password_hash, email, email_verified, avatar, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
    [cleanUsername, displayName || null, hash, email || null, 0, null, "online"]
  );
  req.session.userId = info.lastInsertRowid;
  const csrfToken = ensureCsrf(req);
  res.json({ ok: true, userId: info.lastInsertRowid, csrfToken });
});

app.post("/api/login", authLimiter, async (req, res) => {
  const { username, password } = req.body || {};
  const user = getOne("SELECT * FROM users WHERE username = ?", [username]);
  if (!user) {
    return res.status(401).json({ error: "Invalid credentials" });
  }
  const ok = await verifyPassword(password, user.password_hash);
  if (!ok) {
    return res.status(401).json({ error: "Invalid credentials" });
  }
  if (!user.password_hash?.startsWith("$argon2")) {
    const upgraded = await hashPassword(password);
    run("UPDATE users SET password_hash = ? WHERE id = ?", [upgraded, user.id]);
  }
  req.session.userId = user.id;
  const allowedStatuses = new Set(["online", "idle", "busy", "away", "invisible"]);
  const nextStatus = allowedStatuses.has(user.status) ? user.status : "online";
  run("UPDATE users SET status = ? WHERE id = ?", [nextStatus, user.id]);
  io.emit("presence:update", { userId: user.id, status: nextStatus });
  const csrfToken = ensureCsrf(req);
  res.json({ ok: true, csrfToken });
});

app.post("/api/logout", (req, res) => {
  const userId = req.session?.userId;
  req.session?.destroy(() => {
    if (userId) {
      onlineUsers.set(userId, "offline");
      run("UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", [userId]);
      io.emit("presence:update", { userId, status: "offline" });
    }
    res.json({ ok: true });
  });
});

app.post("/api/uploads", requireAuth, upload.single("image"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No image uploaded" });
  }
  res.json({ url: `/uploads/${req.file.filename}` });
});

app.post("/api/uploads/audio", requireAuth, audioUpload.single("audio"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No audio uploaded" });
  }
  res.json({ url: `/uploads/${req.file.filename}` });
});

app.post("/api/uploads/ringtone", requireAuth, ringtoneUpload.single("ringtone"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No ringtone uploaded" });
  }
  const url = `/uploads/${req.file.filename}`;
  run("UPDATE users SET ringtone_url = ? WHERE id = ?", [url, req.session.userId]);
  res.json({ url });
});

app.post("/api/stories", requireAuth, storyUpload.single("story"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No story uploaded" });
  }
  const mediaUrl = `/uploads/${req.file.filename}`;
  const mediaType = req.file.mimetype?.startsWith("video/") ? "video" : "image";
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  const info = run(
    "INSERT INTO stories (user_id, media_url, media_type, expires_at) VALUES (?, ?, ?, ?)",
    [req.session.userId, mediaUrl, mediaType, expiresAt]
  );
  run(
    "INSERT OR IGNORE INTO story_views (story_id, viewer_id) VALUES (?, ?)",
    [info.lastInsertRowid, req.session.userId]
  );
  res.json({
    id: info.lastInsertRowid,
    media_url: mediaUrl,
    media_type: mediaType,
    expires_at: expiresAt,
  });
});

app.get("/api/stories", requireAuth, (req, res) => {
  const friendsRows = getAll(
    `
    SELECT u.id, u.username, u.display_name, u.avatar, u.status, u.last_seen
    FROM friendships f
    JOIN users u
      ON (u.id = f.user1_id AND f.user2_id = ?)
      OR (u.id = f.user2_id AND f.user1_id = ?)
    ORDER BY u.username COLLATE NOCASE
    `,
    [req.session.userId, req.session.userId]
  );
  const userIds = [req.session.userId, ...friendsRows.map((row) => row.id)];
  if (userIds.length === 0) {
    return res.json({ users: [] });
  }
  const placeholders = userIds.map(() => "?").join(",");
  const stories = getAll(
    `
    SELECT s.id, s.user_id, s.media_url, s.media_type, s.created_at, s.expires_at,
           CASE WHEN sv.id IS NULL THEN 0 ELSE 1 END AS viewed
    FROM stories s
    LEFT JOIN story_views sv
      ON sv.story_id = s.id AND sv.viewer_id = ?
    WHERE s.user_id IN (${placeholders})
      AND datetime(s.expires_at) > datetime('now')
    ORDER BY datetime(s.created_at) ASC
    `,
    [req.session.userId, ...userIds]
  );
  const userRows = getAll(
    `SELECT id, username, display_name, avatar, status, last_seen FROM users WHERE id IN (${placeholders})`,
    userIds
  );
  const userMap = new Map();
  userRows.forEach((row) => {
    userMap.set(row.id, {
      ...row,
      status: onlineUsers.get(row.id) || row.status,
    });
  });
  const storiesByUser = {};
  stories.forEach((story) => {
    const list = storiesByUser[story.user_id] || [];
    list.push({
      id: story.id,
      media_url: story.media_url,
      media_type: story.media_type,
      created_at: story.created_at,
      expires_at: story.expires_at,
      viewed: Boolean(story.viewed),
    });
    storiesByUser[story.user_id] = list;
  });
  const users = userIds
    .map((id) => {
      const storiesList = storiesByUser[id] || [];
      if (storiesList.length === 0) return null;
      const unviewed = storiesList.some((s) => !s.viewed);
      return {
        user: userMap.get(id),
        stories: storiesList,
        has_unviewed: unviewed,
      };
    })
    .filter(Boolean);
  res.json({ users });
});

app.post("/api/stories/:id/view", requireAuth, (req, res) => {
  const storyId = Number(req.params.id);
  if (!storyId) {
    return res.status(400).json({ error: "Invalid story" });
  }
  run(
    "INSERT OR IGNORE INTO story_views (story_id, viewer_id) VALUES (?, ?)",
    [storyId, req.session.userId]
  );
  res.json({ ok: true });
});

app.get("/api/stories/:id/viewers", requireAuth, (req, res) => {
  const storyId = Number(req.params.id);
  const story = getOne("SELECT id, user_id FROM stories WHERE id = ?", [storyId]);
  if (!story) {
    return res.status(404).json({ error: "Story not found" });
  }
  if (story.user_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  const rows = getAll(
    `
      SELECT u.id, u.username, u.display_name, u.avatar, sv.viewed_at
      FROM story_views sv
      JOIN users u ON u.id = sv.viewer_id
      WHERE sv.story_id = ?
      ORDER BY datetime(sv.viewed_at) DESC
    `,
    [storyId]
  );
  res.json({ viewers: rows });
});

app.get("/api/stories/item/:id", requireAuth, (req, res) => {
  const storyId = Number(req.params.id);
  if (!storyId) {
    return res.status(400).json({ error: "Invalid story" });
  }
  const row = getOne(
    `
      SELECT s.id, s.user_id, s.media_url, s.media_type, s.created_at, s.expires_at,
             u.username, u.display_name, u.avatar, u.status, u.last_seen
      FROM stories s
      JOIN users u ON u.id = s.user_id
      WHERE s.id = ?
        AND datetime(s.expires_at) > datetime('now')
    `,
    [storyId]
  );
  if (!row) {
    return res.status(404).json({ error: "Story not found" });
  }
  res.json({
    story: {
      id: row.id,
      user_id: row.user_id,
      media_url: row.media_url,
      media_type: row.media_type,
      created_at: row.created_at,
      expires_at: row.expires_at,
      user: {
        id: row.user_id,
        username: row.username,
        display_name: row.display_name,
        avatar: row.avatar,
        status: onlineUsers.get(row.user_id) || row.status,
        last_seen: row.last_seen,
      },
    },
  });
});

app.delete("/api/stories/:id", requireAuth, (req, res) => {
  const storyId = Number(req.params.id);
  if (!storyId) {
    return res.status(400).json({ error: "Invalid story" });
  }
  const story = getOne(
    "SELECT id, user_id FROM stories WHERE id = ?",
    [storyId]
  );
  if (!story || story.user_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  run("DELETE FROM story_views WHERE story_id = ?", [storyId]);
  run("DELETE FROM stories WHERE id = ?", [storyId]);
  res.json({ ok: true });
});

app.get("/api/me", requireAuth, (req, res) => {
  const user = getOne(
    "SELECT id, username, display_name, custom_status, bio, email, email_verified, avatar, status, last_seen, ringtone_url, song_title, song_artist, song_album, song_cover_url, song_audio_url, song_source, song_source_url, song_updated_at FROM users WHERE id = ?",
    [req.session.userId]
  );
  const aliasMap = getAliasesForUsers(user ? [user.id] : []);
  const csrfToken = ensureCsrf(req);
  res.json({
    user: user ? { ...user, aliases: aliasMap.get(user.id) || [] } : null,
    csrfToken,
  });
});

app.get("/api/users", requireAuth, (req, res) => {
  const users = getAll(
    "SELECT id, username, display_name, custom_status, bio, avatar, status, last_seen, song_title, song_artist, song_album, song_cover_url, song_audio_url, song_source, song_source_url, song_updated_at FROM users ORDER BY username COLLATE NOCASE"
  );
  const aliasMap = getAliasesForUsers(users.map((row) => row.id));
  res.json({
    users: users.map((row) => ({
      ...row,
      aliases: aliasMap.get(row.id) || [],
    })),
  });
});

app.get("/api/connections", requireAuth, (req, res) => {
  const rows = getAll(
    "SELECT id, service, handle, url, visibility FROM connections WHERE user_id = ? ORDER BY datetime(created_at) DESC",
    [req.session.userId]
  );
  res.json({ connections: rows });
});

app.get("/api/connections/:userId", requireAuth, (req, res) => {
  const userId = Number(req.params.userId);
  const rows = getAll(
    "SELECT id, service, handle, url, visibility FROM connections WHERE user_id = ? ORDER BY datetime(created_at) DESC",
    [userId]
  );
  const filtered = rows.filter((row) =>
    canViewConnection(row.visibility, userId, req.session.userId)
  );
  res.json({ connections: filtered });
});

app.post("/api/connections", requireAuth, async (req, res) => {
  const { service, handle, url, visibility } = req.body || {};
  const cleanService = (service || "").trim().toLowerCase();
  const cleanHandle = (handle || "").trim();
  const cleanUrl = (url || "").trim();
  const allowedVisibility = ["public", "friends", "hidden"];
  const cleanVisibility = allowedVisibility.includes(visibility)
    ? visibility
    : "public";
  if (!cleanService || !cleanHandle) {
    return res.status(400).json({ error: "Service and handle required" });
  }
  if (!/^[a-zA-Z0-9._-]+$/.test(cleanHandle)) {
    return res.status(400).json({ error: "Handle can only use letters, numbers, ., -, _" });
  }
  run(
    "INSERT INTO connections (user_id, service, handle, url, visibility) VALUES (?, ?, ?, ?, ?)",
    [req.session.userId, cleanService, cleanHandle, cleanUrl || null, cleanVisibility]
  );
  if (cleanService === "spotify") {
    await syncSpotifyConnectionSong(req.session.userId, cleanUrl || null);
  }
  res.json({ ok: true });
});

app.patch("/api/connections/:id", requireAuth, async (req, res) => {
  const id = Number(req.params.id);
  const { handle, url, visibility } = req.body || {};
  const row = getOne(
    "SELECT id, service, url FROM connections WHERE id = ? AND user_id = ?",
    [id, req.session.userId]
  );
  if (!row) {
    return res.status(404).json({ error: "Connection not found" });
  }
  const allowedVisibility = ["public", "friends", "hidden"];
  const cleanVisibility = allowedVisibility.includes(visibility)
    ? visibility
    : null;
  if (handle !== undefined) {
    run("UPDATE connections SET handle = ? WHERE id = ?", [
      (handle || "").trim(),
      id,
    ]);
  }
  if (url !== undefined) {
    run("UPDATE connections SET url = ? WHERE id = ?", [
      (url || "").trim() || null,
      id,
    ]);
  }
  if (cleanVisibility) {
    run("UPDATE connections SET visibility = ? WHERE id = ?", [
      cleanVisibility,
      id,
    ]);
  }
  if (row.service === "spotify") {
    const current = getOne("SELECT url FROM connections WHERE id = ?", [id]);
    await syncSpotifyConnectionSong(req.session.userId, current?.url || null);
  }
  res.json({ ok: true });
});

app.delete("/api/connections/:id", requireAuth, async (req, res) => {
  const id = Number(req.params.id);
  const row = getOne("SELECT service FROM connections WHERE id = ? AND user_id = ?", [
    id,
    req.session.userId,
  ]);
  run("DELETE FROM connections WHERE id = ? AND user_id = ?", [
    id,
    req.session.userId,
  ]);
  if (row?.service === "spotify") {
    const remaining = getOne(
      "SELECT url FROM connections WHERE user_id = ? AND service = 'spotify' ORDER BY datetime(created_at) DESC LIMIT 1",
      [req.session.userId]
    );
    await syncSpotifyConnectionSong(req.session.userId, remaining?.url || null);
  }
  res.json({ ok: true });
});

app.get("/api/messages/:userId", requireAuth, (req, res) => {
  const otherId = Number(req.params.userId);
  const rows = getAll(
    `
      SELECT id, sender_id, recipient_id, body, type, image_url, audio_url, story_id, story_owner_id, story_media_url, story_media_type, forwarded_from_id, forwarded_from_username, forwarded_from_display, created_at, edited_at, deleted_at
      FROM messages
      WHERE ((sender_id = ? AND recipient_id = ?)
         OR (sender_id = ? AND recipient_id = ?))
        AND deleted_at IS NULL
      ORDER BY datetime(created_at) ASC
      LIMIT 500
    `,
    [req.session.userId, otherId, otherId, req.session.userId]
  );
  const ids = rows.map((row) => row.id);
  const reactions = getReactions("dm", ids, req.session.userId);
  const messages = rows.map((row) => ({
    ...row,
    reactions: reactions[row.id] || [],
  }));
  res.json({ messages });
});

app.post("/api/messages/:userId/read", requireAuth, (req, res) => {
  const otherId = Number(req.params.userId);
  upsertDmRead(req.session.userId, otherId);
  io.to(`user:${otherId}`).emit("dm:read", {
    readerId: req.session.userId,
    otherId,
    lastReadAt: new Date().toISOString(),
  });
  res.json({ ok: true });
});

app.get("/api/notes/:userId", requireAuth, (req, res) => {
  const targetId = Number(req.params.userId);
  const row = getOne(
    "SELECT note FROM private_notes WHERE owner_id = ? AND target_id = ?",
    [req.session.userId, targetId]
  );
  res.json({ note: row?.note || "" });
});

app.patch("/api/notes/:userId", requireAuth, (req, res) => {
  const targetId = Number(req.params.userId);
  const note = (req.body?.note || "").slice(0, 1000);
  const existing = getOne(
    "SELECT id FROM private_notes WHERE owner_id = ? AND target_id = ?",
    [req.session.userId, targetId]
  );
  if (existing) {
    run(
      "UPDATE private_notes SET note = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
      [note, existing.id]
    );
  } else {
    run(
      "INSERT INTO private_notes (owner_id, target_id, note) VALUES (?, ?, ?)",
      [req.session.userId, targetId, note]
    );
  }
  res.json({ ok: true, note });
});

app.delete("/api/messages/:userId", requireAuth, (req, res) => {
  const otherId = Number(req.params.userId);
  run(
    `
      DELETE FROM messages
      WHERE (sender_id = ? AND recipient_id = ?)
         OR (sender_id = ? AND recipient_id = ?)
    `,
    [req.session.userId, otherId, otherId, req.session.userId]
  );
  run(
    "DELETE FROM message_reactions WHERE message_type = 'dm' AND message_id IN (SELECT id FROM messages WHERE (sender_id = ? AND recipient_id = ?) OR (sender_id = ? AND recipient_id = ?))",
    [req.session.userId, otherId, otherId, req.session.userId]
  );
  io.to(`user:${req.session.userId}`).emit("dm:deleted", { userId: otherId });
  io.to(`user:${otherId}`).emit("dm:deleted", { userId: req.session.userId });
  res.json({ ok: true });
});

app.delete("/api/messages/item/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const message = getOne("SELECT * FROM messages WHERE id = ?", [id]);
  if (!message) {
    return res.status(404).json({ error: "Message not found" });
  }
  if (
    message.sender_id !== req.session.userId &&
    message.recipient_id !== req.session.userId
  ) {
    return res.status(403).json({ error: "Not allowed" });
  }
  run(
    "UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
    [id]
  );
  run(
    "DELETE FROM message_reactions WHERE message_type = 'dm' AND message_id = ?",
    [id]
  );
  io.to(`user:${message.sender_id}`).emit("message:deleted", {
    messageType: "dm",
    messageId: id,
    otherId: message.recipient_id,
  });
  io.to(`user:${message.recipient_id}`).emit("message:deleted", {
    messageType: "dm",
    messageId: id,
    otherId: message.sender_id,
  });
  res.json({ ok: true });
});

app.patch("/api/messages/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const { body } = req.body || {};
  if (!body) {
    return res.status(400).json({ error: "Body required" });
  }
  const message = getOne(
    "SELECT * FROM messages WHERE id = ?",
    [id]
  );
  if (!message || message.sender_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  run("UPDATE messages SET body = ?, edited_at = CURRENT_TIMESTAMP WHERE id = ?", [
    body.slice(0, 2000),
    id,
  ]);
  const updated = {
    id,
    sender_id: message.sender_id,
    recipient_id: message.recipient_id,
    body: body.slice(0, 2000),
    type: message.type || "text",
    image_url: message.image_url || null,
    audio_url: message.audio_url || null,
    forwarded_from_id: message.forwarded_from_id || null,
    forwarded_from_username: message.forwarded_from_username || null,
    forwarded_from_display: message.forwarded_from_display || null,
    created_at: message.created_at,
    edited_at: new Date().toISOString(),
  };
  io.to(`user:${message.sender_id}`).emit("dm:edit", updated);
  io.to(`user:${message.recipient_id}`).emit("dm:edit", updated);
  res.json({ message: updated });
});

app.patch("/api/settings", requireAuth, async (req, res) => {
  const {
    username,
    displayName,
    customStatus,
    avatar,
    bio,
    email,
    password,
    newPassword,
    status,
    song,
  } = req.body || {};
  const user = getOne("SELECT * FROM users WHERE id = ?", [req.session.userId]);
  if (!user) {
    return res.status(404).json({ error: "User not found" });
  }
  if (username && username !== user.username) {
    const cleanUsername = normalizeUsername(username);
    if (!isValidUsername(cleanUsername)) {
      return res.status(400).json({ error: "Invalid username" });
    }
    const cooldownUntil = getCooldownUntilFor(
      user.id,
      ["change_primary"],
      USERNAME_PRIMARY_COOLDOWN_MS
    );
    if (cooldownUntil) {
      return res
        .status(429)
        .json({ error: "Username cooldown active", cooldown_until: cooldownUntil });
    }
    const ok = await verifyPassword(password || "", user.password_hash);
    if (!ok) {
      return res.status(401).json({ error: "Password required to change username" });
    }
    if (!isUsernameAvailable(cleanUsername)) {
      return res.status(409).json({ error: "Username already taken" });
    }
    const aliasCountRow = getOne(
      "SELECT COUNT(*) AS count FROM username_aliases WHERE user_id = ?",
      [user.id]
    );
    if ((aliasCountRow?.count || 0) >= MAX_USERNAME_ALIASES) {
      return res
        .status(400)
        .json({ error: `Max ${MAX_USERNAME_ALIASES} aliases` });
    }
    run("INSERT INTO username_history (user_id, old_username) VALUES (?, ?)", [
      user.id,
      user.username,
    ]);
    run("UPDATE users SET username = ? WHERE id = ?", [cleanUsername, user.id]);
    run(
      "INSERT OR IGNORE INTO username_aliases (user_id, username) VALUES (?, ?)",
      [user.id, user.username]
    );
    recordUsernameAction(user.id, "change_primary");
  }
  if (avatar !== undefined) {
    run("UPDATE users SET avatar = ? WHERE id = ?", [avatar, user.id]);
  }
  if (displayName !== undefined) {
    run("UPDATE users SET display_name = ? WHERE id = ?", [displayName, user.id]);
  }
  if (customStatus !== undefined) {
    const trimmed = String(customStatus || "").trim();
    if (trimmed.length > 13) {
      return res
        .status(400)
        .json({ error: "Custom status must be 13 characters or fewer" });
    }
    run("UPDATE users SET custom_status = ? WHERE id = ?", [
      trimmed || null,
      user.id,
    ]);
  }
  if (bio !== undefined) {
    const cleaned = String(bio || "");
    if (cleaned.length > 190) {
      return res
        .status(400)
        .json({ error: "Bio must be 190 characters or fewer" });
    }
    const trimmed = cleaned.trim();
    run("UPDATE users SET bio = ? WHERE id = ?", [trimmed || null, user.id]);
  }
  if (email !== undefined) {
    const trimmed = (email || "").trim();
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (trimmed && !emailRegex.test(trimmed)) {
      return res.status(400).json({ error: "Invalid email format" });
    }
    run("UPDATE users SET email = ?, email_verified = 0 WHERE id = ?", [
      trimmed || null,
      user.id,
    ]);
  }
  if (req.body?.ringtone === null) {
    run("UPDATE users SET ringtone_url = NULL WHERE id = ?", [user.id]);
  }
  if (song === null) {
    run(
      "UPDATE users SET song_title = NULL, song_artist = NULL, song_album = NULL, song_cover_url = NULL, song_audio_url = NULL, song_source = NULL, song_source_url = NULL, song_updated_at = ? WHERE id = ?",
      [new Date().toISOString(), user.id]
    );
  } else if (song) {
    const sourceUrl = cleanSongUrl(song.sourceUrl || song.source_url);
    const source = cleanSongText(song.source || song.source_type || "", 32);
    let title = cleanSongText(song.title, 120);
    let artist = cleanSongText(song.artist, 120);
    let album = cleanSongText(song.album, 120);
    let coverUrl = cleanSongUrl(song.coverUrl || song.cover_url);
    let audioUrl = cleanSongUrl(song.audioUrl || song.audio_url);
    if (source === "spotify" && sourceUrl) {
      const meta = await fetchSpotifyOembed(sourceUrl);
      if (meta?.title) {
        const parsed = parseSpotifyTitle(meta.title);
        if (!title) title = cleanSongText(parsed.title, 120);
        if (!artist) artist = cleanSongText(parsed.artist, 120);
      }
      if (!artist && meta?.author_name) {
        artist = cleanSongText(meta.author_name, 120);
      }
      if (!coverUrl && meta?.thumbnail_url) {
        coverUrl = cleanSongUrl(meta.thumbnail_url);
      }
    }
    run(
      "UPDATE users SET song_title = ?, song_artist = ?, song_album = ?, song_cover_url = ?, song_audio_url = ?, song_source = ?, song_source_url = ?, song_updated_at = ? WHERE id = ?",
      [
        title,
        artist,
        album,
        coverUrl,
        audioUrl,
        source,
        sourceUrl,
        new Date().toISOString(),
        user.id,
      ]
    );
  }
  if (status) {
    const allowed = ["online", "away", "busy", "invisible"];
    const normalized = allowed.includes(status) ? status : "online";
    run("UPDATE users SET status = ? WHERE id = ?", [normalized, user.id]);
    onlineUsers.set(user.id, normalized);
    io.emit("presence:update", { userId: user.id, status: normalized });
  }
  if (newPassword) {
    const ok = await verifyPassword(password || "", user.password_hash);
    if (!ok) {
      return res.status(401).json({ error: "Password required to change password" });
    }
    const hash = await hashPassword(newPassword);
    run("UPDATE users SET password_hash = ? WHERE id = ?", [hash, user.id]);
  }
  const payload = emitProfileUpdate(user.id);
  res.json({ ok: true, user: payload });
});

app.get("/api/usernames", requireAuth, (req, res) => {
  expireUsernameTransfers();
  const user = getOne("SELECT id, username FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  if (!user) {
    return res.status(404).json({ error: "User not found" });
  }
  const aliases = getAll(
    "SELECT username, created_at FROM username_aliases WHERE user_id = ? ORDER BY datetime(created_at) ASC",
    [user.id]
  );
  const cooldownUntil = getCooldownUntilFor(
    user.id,
    ["add_alias"],
    USERNAME_CLAIM_COOLDOWN_MS
  );
  const incoming = getAll(
    `
      SELECT ut.id, ut.username, ut.from_user_id, ut.created_at, ut.expires_at,
             u.username AS from_username, u.display_name AS from_display_name, u.avatar AS from_avatar
      FROM username_transfers ut
      JOIN users u ON u.id = ut.from_user_id
      WHERE ut.to_user_id = ? AND ut.status = 'pending' AND datetime(ut.expires_at) > datetime('now')
      ORDER BY datetime(ut.created_at) DESC
    `,
    [user.id]
  );
  const outgoing = getAll(
    `
      SELECT ut.id, ut.username, ut.to_user_id, ut.created_at, ut.expires_at,
             u.username AS to_username, u.display_name AS to_display_name, u.avatar AS to_avatar
      FROM username_transfers ut
      JOIN users u ON u.id = ut.to_user_id
      WHERE ut.from_user_id = ? AND ut.status = 'pending' AND datetime(ut.expires_at) > datetime('now')
      ORDER BY datetime(ut.created_at) DESC
    `,
    [user.id]
  );
  res.json({
    primary: user.username,
    aliases,
    cooldown_until: cooldownUntil,
    limit: MAX_USERNAME_ALIASES,
    incoming,
    outgoing,
  });
});

app.post("/api/usernames/check", requireAuth, (req, res) => {
  const { username } = req.body || {};
  const clean = normalizeUsername(username);
  if (!isValidUsername(clean)) {
    return res.status(400).json({ error: "Invalid username" });
  }
  const available = isUsernameAvailable(clean);
  res.json({ available });
});

app.post("/api/usernames/claim", requireAuth, async (req, res) => {
  const { username, password } = req.body || {};
  const clean = normalizeUsername(username);
  const rawPassword = password ?? "";
  if (!isValidUsername(clean)) {
    return res.status(400).json({ error: "Invalid username" });
  }
  if (!isUsernameAvailable(clean)) {
    return res.status(409).json({ error: "Username already taken" });
  }
  const cooldownUntil = getCooldownUntilFor(
    req.session.userId,
    ["add_alias"],
    USERNAME_CLAIM_COOLDOWN_MS
  );
  if (cooldownUntil) {
    return res
      .status(429)
      .json({ error: "Username cooldown active", cooldown_until: cooldownUntil });
  }
  const user = getOne("SELECT id, password_hash FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  if (!rawPassword) {
    return res.status(401).json({ error: "Password required" });
  }
  const ok = await verifyPassword(rawPassword, user?.password_hash || "");
  if (!ok) {
    return res.status(401).json({ error: "Incorrect password" });
  }
  const countRow = getOne(
    "SELECT COUNT(*) AS count FROM username_aliases WHERE user_id = ?",
    [user.id]
  );
  if ((countRow?.count || 0) >= MAX_USERNAME_ALIASES) {
    return res.status(400).json({ error: `Max ${MAX_USERNAME_ALIASES} aliases` });
  }
  run("INSERT INTO username_aliases (user_id, username) VALUES (?, ?)", [
    user.id,
    clean,
  ]);
  recordUsernameAction(user.id, "add_alias");
  emitProfileUpdate(user.id);
  res.json({ ok: true, alias: clean });
});

app.post("/api/usernames/set-primary", requireAuth, async (req, res) => {
  const { username } = req.body || {};
  const clean = normalizeUsername(username);
  if (!clean) {
    return res.status(400).json({ error: "Username required" });
  }
  const cooldownUntil = getCooldownUntilFor(
    req.session.userId,
    ["change_primary"],
    USERNAME_PRIMARY_COOLDOWN_MS
  );
  if (cooldownUntil) {
    return res
      .status(429)
      .json({ error: "Username cooldown active", cooldown_until: cooldownUntil });
  }
  const user = getOne("SELECT id, username FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  if (!user) {
    return res.status(404).json({ error: "User not found" });
  }
  if (user.username.toLowerCase() === clean.toLowerCase()) {
    return res.json({ ok: true });
  }
  const alias = getOne(
    "SELECT id FROM username_aliases WHERE user_id = ? AND username = ? COLLATE NOCASE",
    [user.id, clean]
  );
  if (!alias) {
    return res.status(404).json({ error: "Alias not found" });
  }
  run("DELETE FROM username_aliases WHERE id = ?", [alias.id]);
  run("INSERT INTO username_history (user_id, old_username) VALUES (?, ?)", [
    user.id,
    user.username,
  ]);
  run("UPDATE users SET username = ? WHERE id = ?", [clean, user.id]);
  run(
    "INSERT OR IGNORE INTO username_aliases (user_id, username) VALUES (?, ?)",
    [user.id, user.username]
  );
  const payload = emitProfileUpdate(user.id);
  res.json({ ok: true, username: clean, user: payload });
});

app.post("/api/usernames/remove", requireAuth, async (req, res) => {
  const { username, password } = req.body || {};
  const clean = normalizeUsername(username);
  if (!clean) {
    return res.status(400).json({ error: "Username required" });
  }
  const cooldownUntil = getCooldownUntilFor(
    req.session.userId,
    ["remove_alias"],
    USERNAME_REMOVE_COOLDOWN_MS
  );
  if (cooldownUntil) {
    return res
      .status(429)
      .json({ error: "Username cooldown active", cooldown_until: cooldownUntil });
  }
  const rawPassword = password ?? "";
  if (!rawPassword) {
    return res.status(401).json({ error: "Password required" });
  }
  const user = getOne("SELECT password_hash FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  const ok = await verifyPassword(rawPassword, user?.password_hash || "");
  if (!ok) {
    return res.status(401).json({ error: "Incorrect password" });
  }
  const alias = getOne(
    "SELECT id FROM username_aliases WHERE user_id = ? AND username = ? COLLATE NOCASE",
    [req.session.userId, clean]
  );
  if (!alias) {
    return res.status(404).json({ error: "Alias not found" });
  }
  run("DELETE FROM username_aliases WHERE id = ?", [alias.id]);
  recordUsernameAction(req.session.userId, "remove_alias");
  emitProfileUpdate(req.session.userId);
  res.json({ ok: true });
});

app.post("/api/usernames/transfer", requireAuth, async (req, res) => {
  const { username, password, recipient } = req.body || {};
  const clean = normalizeUsername(username);
  const recipientName = normalizeUsername(recipient);
  if (!clean || !recipientName) {
    return res.status(400).json({ error: "Username and recipient required" });
  }
  const cooldownUntil = getCooldownUntilFor(
    req.session.userId,
    ["transfer_out"],
    USERNAME_TRANSFER_COOLDOWN_MS
  );
  if (cooldownUntil) {
    return res
      .status(429)
      .json({ error: "Username cooldown active", cooldown_until: cooldownUntil });
  }
  const user = getOne("SELECT id, username, password_hash FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  const ok = await verifyPassword(password || "", user?.password_hash || "");
  if (!ok) {
    return res.status(401).json({ error: "Password required" });
  }
  if (user.username.toLowerCase() === clean.toLowerCase()) {
    return res.status(400).json({ error: "Set another primary first" });
  }
  const alias = getOne(
    "SELECT id FROM username_aliases WHERE user_id = ? AND username = ? COLLATE NOCASE",
    [user.id, clean]
  );
  if (!alias) {
    return res.status(404).json({ error: "Alias not found" });
  }
  const recipientUser = findUserByUsernameOrAlias(recipientName);
  if (!recipientUser) {
    return res.status(404).json({ error: "Recipient not found" });
  }
  if (recipientUser.id === user.id) {
    return res.status(400).json({ error: "Cannot transfer to yourself" });
  }
  const pending = getOne(
    "SELECT id FROM username_transfers WHERE username = ? COLLATE NOCASE AND status = 'pending' AND datetime(expires_at) > datetime('now')",
    [clean]
  );
  if (pending) {
    return res.status(409).json({ error: "Transfer already pending" });
  }
  const countRow = getOne(
    "SELECT COUNT(*) AS count FROM username_aliases WHERE user_id = ?",
    [recipientUser.id]
  );
  if ((countRow?.count || 0) >= MAX_USERNAME_ALIASES) {
    return res.status(400).json({ error: "Recipient has max aliases" });
  }
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  run(
    "INSERT INTO username_transfers (username, from_user_id, to_user_id, expires_at) VALUES (?, ?, ?, ?)",
    [clean, user.id, recipientUser.id, expiresAt]
  );
  recordUsernameAction(user.id, "transfer_out");
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id) VALUES (?, 'username_transfer', ?, ?)",
    [
      recipientUser.id,
      `@${user.username || "user"} wants to transfer @${clean} to you`,
      user.id,
    ]
  );
  io.to(`user:${recipientUser.id}`).emit("notify:new");
  res.json({ ok: true });
});

app.post("/api/usernames/transfers/:id/accept", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  expireUsernameTransfers();
  const transfer = getOne("SELECT * FROM username_transfers WHERE id = ?", [id]);
  if (!transfer || transfer.status !== "pending") {
    return res.status(404).json({ error: "Transfer not found" });
  }
  if (transfer.to_user_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  if (new Date(transfer.expires_at).getTime() <= Date.now()) {
    run("UPDATE username_transfers SET status = 'expired' WHERE id = ?", [id]);
    return res.status(410).json({ error: "Transfer expired" });
  }
  const cooldownUntil = getCooldownUntilFor(
    req.session.userId,
    ["transfer_in"],
    USERNAME_TRANSFER_COOLDOWN_MS
  );
  if (cooldownUntil) {
    return res
      .status(429)
      .json({ error: "Username cooldown active", cooldown_until: cooldownUntil });
  }
  const countRow = getOne(
    "SELECT COUNT(*) AS count FROM username_aliases WHERE user_id = ?",
    [req.session.userId]
  );
  if ((countRow?.count || 0) >= MAX_USERNAME_ALIASES) {
    return res.status(400).json({ error: "Max aliases reached" });
  }
  const alias = getOne(
    "SELECT id FROM username_aliases WHERE user_id = ? AND username = ? COLLATE NOCASE",
    [transfer.from_user_id, transfer.username]
  );
  if (!alias) {
    return res.status(409).json({ error: "Username no longer available" });
  }
  run("DELETE FROM username_aliases WHERE id = ?", [alias.id]);
  run(
    "INSERT INTO username_aliases (user_id, username) VALUES (?, ?)",
    [transfer.to_user_id, transfer.username]
  );
  run("UPDATE username_transfers SET status = 'accepted' WHERE id = ?", [id]);
  recordUsernameAction(req.session.userId, "transfer_in");
  emitProfileUpdate(transfer.from_user_id);
  emitProfileUpdate(transfer.to_user_id);
  const recipient = getOne("SELECT username FROM users WHERE id = ?", [
    transfer.to_user_id,
  ]);
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id) VALUES (?, 'username_transfer', ?, ?)",
    [
      transfer.from_user_id,
      `@${recipient?.username || "user"} accepted @${transfer.username}`,
      transfer.to_user_id,
    ]
  );
  io.to(`user:${transfer.from_user_id}`).emit("notify:new");
  res.json({ ok: true });
});

app.post("/api/usernames/transfers/:id/deny", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  expireUsernameTransfers();
  const transfer = getOne("SELECT * FROM username_transfers WHERE id = ?", [id]);
  if (!transfer || transfer.status !== "pending") {
    return res.status(404).json({ error: "Transfer not found" });
  }
  if (transfer.to_user_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  run("UPDATE username_transfers SET status = 'declined' WHERE id = ?", [id]);
  const recipient = getOne("SELECT username FROM users WHERE id = ?", [
    transfer.to_user_id,
  ]);
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id) VALUES (?, 'username_transfer', ?, ?)",
    [
      transfer.from_user_id,
      `@${recipient?.username || "user"} declined @${transfer.username}`,
      transfer.to_user_id,
    ]
  );
  io.to(`user:${transfer.from_user_id}`).emit("notify:new");
  res.json({ ok: true });
});

app.get("/api/friends", requireAuth, (req, res) => {
  const rows = getAll(
    `
    SELECT u.id, u.username, u.display_name, u.custom_status, u.bio, u.avatar, u.status, u.last_seen
    FROM friendships f
    JOIN users u
      ON (u.id = f.user1_id AND f.user2_id = ?)
      OR (u.id = f.user2_id AND f.user1_id = ?)
    ORDER BY u.username COLLATE NOCASE
    `,
    [req.session.userId, req.session.userId]
  );
  const storyMap = getStoryStatusMap(
    req.session.userId,
    rows.map((row) => row.id)
  );
  const aliasMap = getAliasesForUsers(rows.map((row) => row.id));
  const friends = rows.map((user) => ({
    ...user,
    status: onlineUsers.get(user.id) || user.status,
    has_story: storyMap.get(user.id)?.count > 0,
    has_unviewed_story: storyMap.get(user.id)?.unviewed > 0,
    aliases: aliasMap.get(user.id) || [],
  }));
  res.json({ friends });
});

app.get("/api/chats", requireAuth, (req, res) => {
  const friendsRows = getAll(
    `
    SELECT u.id, u.username, u.display_name, u.custom_status, u.bio, u.avatar, u.status, u.last_seen
    FROM friendships f
    JOIN users u
      ON (u.id = f.user1_id AND f.user2_id = ?)
      OR (u.id = f.user2_id AND f.user1_id = ?)
    ORDER BY u.username COLLATE NOCASE
    `,
    [req.session.userId, req.session.userId]
  );
  const storyMap = getStoryStatusMap(
    req.session.userId,
    friendsRows.map((row) => row.id)
  );
  const aliasMap = getAliasesForUsers(friendsRows.map((row) => row.id));
  const dms = friendsRows.map((user) => {
    const last = getLastMessageBetween(req.session.userId, user.id);
    return {
      ...user,
      status: onlineUsers.get(user.id) || user.status,
      last_read_at: getDmReadAt(user.id, req.session.userId),
      last_message: last || null,
      unread_count: getDmUnreadCount(req.session.userId, user.id),
      has_story: storyMap.get(user.id)?.count > 0,
      has_unviewed_story: storyMap.get(user.id)?.unviewed > 0,
      aliases: aliasMap.get(user.id) || [],
    };
  });

  const groupRows = getAll(
    `
    SELECT g.id, g.name, g.owner_id, g.avatar
    FROM groups g
    JOIN group_members gm ON gm.group_id = g.id
    WHERE gm.user_id = ?
    ORDER BY datetime(g.created_at) DESC
    `,
    [req.session.userId]
  );
  if (groupRows.length === 0) {
    return res.json({ dms, groups: [] });
  }
  const ids = groupRows.map((row) => row.id);
  const placeholders = ids.map(() => "?").join(",");
  const memberRows = getAll(
    `
    SELECT gm.group_id, u.id, u.username, u.display_name, u.custom_status, u.bio, u.avatar, u.status, u.last_seen
    FROM group_members gm
    JOIN users u ON u.id = gm.user_id
    WHERE gm.group_id IN (${placeholders})
    ORDER BY u.username COLLATE NOCASE
    `,
    ids
  );
  const groupAliasMap = getAliasesForUsers(memberRows.map((row) => row.id));
  const membersByGroup = {};
  memberRows.forEach((row) => {
    const list = membersByGroup[row.group_id] || [];
    list.push({
      id: row.id,
      username: row.username,
      display_name: row.display_name,
      bio: row.bio,
      avatar: row.avatar,
      status: onlineUsers.get(row.id) || row.status,
      aliases: groupAliasMap.get(row.id) || [],
    });
    membersByGroup[row.group_id] = list;
  });
  const groups = groupRows.map((row) => {
    const last = getGroupLastMessage(row.id);
    return {
      ...row,
      members: membersByGroup[row.id] || [],
      last_message: last || null,
      unread_count: getGroupUnreadCount(req.session.userId, row.id),
    };
  });
  res.json({ dms, groups });
});

app.post("/api/friends/request", requireAuth, (req, res) => {
  const { username } = req.body || {};
  if (!username) {
    return res.status(400).json({ error: "Username required" });
  }
  const target = findUserByUsernameOrAlias(username);
  if (!target) {
    return res.status(404).json({ error: "User not found" });
  }
  if (target.id === req.session.userId) {
    return res.status(400).json({ error: "Cannot add yourself" });
  }
  if (isFriends(req.session.userId, target.id)) {
    return res.status(409).json({ error: "Already friends" });
  }
  const existing = getOne(
    "SELECT id FROM friend_requests WHERE requester_id = ? AND recipient_id = ? AND status = 'pending'",
    [req.session.userId, target.id]
  );
  if (existing) {
    return res.status(409).json({ error: "Request already sent" });
  }
  const reverse = getOne(
    "SELECT id FROM friend_requests WHERE requester_id = ? AND recipient_id = ? AND status = 'pending'",
    [target.id, req.session.userId]
  );
  if (reverse) {
    return res.status(409).json({ error: "They already requested you" });
  }
  run(
    "INSERT INTO friend_requests (requester_id, recipient_id, status) VALUES (?, ?, 'pending')",
    [req.session.userId, target.id]
  );
  const requester = getOne(
    "SELECT username FROM users WHERE id = ?",
    [req.session.userId]
  );
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id) VALUES (?, 'friend_request', ?, ?)",
    [target.id, `@${requester?.username || "user"} sent a friend request`, req.session.userId]
  );
  io.to(`user:${target.id}`).emit("notify:new");
  io.to(`user:${req.session.userId}`).emit("friends:update");
  io.to(`user:${target.id}`).emit("friends:update");
  res.json({ ok: true });
});

app.get("/api/friends/requests", requireAuth, (req, res) => {
  const rows = getAll(
    `
    SELECT fr.id, fr.requester_id, u.username, u.display_name, u.avatar, fr.created_at
    FROM friend_requests fr
    JOIN users u ON u.id = fr.requester_id
    WHERE fr.recipient_id = ? AND fr.status = 'pending'
    ORDER BY datetime(fr.created_at) DESC
    `,
    [req.session.userId]
  );
  res.json({ requests: rows });
});

app.get("/api/friends/requests/outgoing", requireAuth, (req, res) => {
  const rows = getAll(
    `
    SELECT fr.id, fr.recipient_id, fr.created_at, u.username
    FROM friend_requests fr
    JOIN users u ON u.id = fr.recipient_id
    WHERE fr.requester_id = ? AND fr.status = 'pending'
    ORDER BY datetime(fr.created_at) DESC
    `,
    [req.session.userId]
  );
  res.json({ requests: rows });
});

app.delete("/api/friends/requests/outgoing/:userId", requireAuth, (req, res) => {
  const recipientId = Number(req.params.userId);
  run(
    "UPDATE friend_requests SET status = 'denied' WHERE requester_id = ? AND recipient_id = ? AND status = 'pending'",
    [req.session.userId, recipientId]
  );
  io.to(`user:${req.session.userId}`).emit("notify:new");
  io.to(`user:${req.session.userId}`).emit("friends:update");
  io.to(`user:${recipientId}`).emit("friends:update");
  res.json({ ok: true });
});

app.get("/api/friends/requests/outgoing", requireAuth, (req, res) => {
  const rows = getAll(
    `
    SELECT fr.id, fr.recipient_id, fr.created_at, u.username
    FROM friend_requests fr
    JOIN users u ON u.id = fr.recipient_id
    WHERE fr.requester_id = ? AND fr.status = 'pending'
    ORDER BY datetime(fr.created_at) DESC
    `,
    [req.session.userId]
  );
  res.json({ requests: rows });
});

app.delete("/api/friends/requests/outgoing/:userId", requireAuth, (req, res) => {
  const recipientId = Number(req.params.userId);
  run(
    "UPDATE friend_requests SET status = 'denied' WHERE requester_id = ? AND recipient_id = ? AND status = 'pending'",
    [req.session.userId, recipientId]
  );
  io.to(`user:${req.session.userId}`).emit("notify:new");
  io.to(`user:${req.session.userId}`).emit("friends:update");
  io.to(`user:${recipientId}`).emit("friends:update");
  res.json({ ok: true });
});

app.post("/api/friends/requests/:id/accept", requireAuth, (req, res) => {
  const requestId = Number(req.params.id);
  const request = getOne(
    "SELECT * FROM friend_requests WHERE id = ? AND recipient_id = ? AND status = 'pending'",
    [requestId, req.session.userId]
  );
  if (!request) {
    return res.status(404).json({ error: "Request not found" });
  }
  run("UPDATE friend_requests SET status = 'accepted' WHERE id = ?", [requestId]);
  const [u1, u2] = normalizePair(request.requester_id, request.recipient_id);
  const existingFriend = getOne(
    "SELECT id FROM friendships WHERE user1_id = ? AND user2_id = ?",
    [u1, u2]
  );
  if (!existingFriend) {
    run("INSERT INTO friendships (user1_id, user2_id) VALUES (?, ?)", [u1, u2]);
  }
  const accepter = getOne(
    "SELECT username, display_name FROM users WHERE id = ?",
    [request.recipient_id]
  );
  const accepterName =
    (accepter?.display_name || "").trim() || accepter?.username || "user";
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id) VALUES (?, 'friend_accept', ?, ?)",
    [
      request.requester_id,
      `@${accepterName} accepted your friend request`,
      request.recipient_id,
    ]
  );
  io.to(`user:${request.requester_id}`).emit("notify:new");
  io.to(`user:${request.recipient_id}`).emit("notify:new");
  io.to(`user:${request.requester_id}`).emit("friends:update");
  io.to(`user:${request.recipient_id}`).emit("friends:update");
  res.json({ ok: true });
});

app.post("/api/friends/requests/:id/deny", requireAuth, (req, res) => {
  const requestId = Number(req.params.id);
  const request = getOne(
    "SELECT * FROM friend_requests WHERE id = ? AND recipient_id = ? AND status = 'pending'",
    [requestId, req.session.userId]
  );
  if (!request) {
    return res.status(404).json({ error: "Request not found" });
  }
  run("UPDATE friend_requests SET status = 'denied' WHERE id = ?", [requestId]);
  io.to(`user:${request.requester_id}`).emit("notify:new");
  io.to(`user:${request.recipient_id}`).emit("notify:new");
  io.to(`user:${request.requester_id}`).emit("friends:update");
  io.to(`user:${request.recipient_id}`).emit("friends:update");
  res.json({ ok: true });
});

app.delete("/api/friends/:id", requireAuth, (req, res) => {
  const otherId = Number(req.params.id);
  const [u1, u2] = normalizePair(req.session.userId, otherId);
  const existing = getOne(
    "SELECT id FROM friendships WHERE user1_id = ? AND user2_id = ?",
    [u1, u2]
  );
  if (!existing) {
    return res.status(409).json({ error: "Not friends yet" });
  }
  run("DELETE FROM friendships WHERE user1_id = ? AND user2_id = ?", [u1, u2]);
  run(
    "DELETE FROM friend_requests WHERE (requester_id = ? AND recipient_id = ?) OR (requester_id = ? AND recipient_id = ?)",
    [req.session.userId, otherId, otherId, req.session.userId]
  );
  io.to(`user:${req.session.userId}`).emit("notify:new");
  io.to(`user:${otherId}`).emit("notify:new");
  io.to(`user:${req.session.userId}`).emit("friends:update");
  io.to(`user:${otherId}`).emit("friends:update");
  res.json({ ok: true });
});

app.get("/api/notifications", requireAuth, (req, res) => {
  const rows = getAll(
    `
    SELECT n.id, n.type, n.message, n.created_at, n.read_at,
           n.from_user_id, n.group_id, n.context, n.ref_id, n.payload,
           u.username AS from_username, u.display_name AS from_display_name,
           g.name AS group_name
    FROM notifications n
    LEFT JOIN users u ON u.id = n.from_user_id
    LEFT JOIN groups g ON g.id = n.group_id
    WHERE n.user_id = ? AND n.read_at IS NULL
    ORDER BY datetime(n.created_at) DESC
    LIMIT 50
    `,
    [req.session.userId]
  );
  res.json({ notifications: rows });
});

app.post("/api/notifications/:id/read", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  run(
    "UPDATE notifications SET read_at = CURRENT_TIMESTAMP WHERE id = ? AND user_id = ?",
    [id, req.session.userId]
  );
  res.json({ ok: true });
});

app.post("/api/notifications/clear", requireAuth, (req, res) => {
  run("DELETE FROM notifications WHERE user_id = ?", [req.session.userId]);
  res.json({ ok: true });
});

app.post("/api/minigames/invite", requireAuth, (req, res) => {
  const { toId, wagerAmount, token, chain, gameType } = req.body || {};
  const opponentId = Number(toId);
  const wager = Number(wagerAmount);
  const cleanType = String(gameType || "").toLowerCase();
  if (!["coinflip", "bigbank"].includes(cleanType)) {
    return res.status(400).json({ error: "Unsupported game type" });
  }
  if (!opponentId || opponentId === req.session.userId) {
    return res.status(400).json({ error: "Invalid opponent" });
  }
  if (!areUsersFriends(req.session.userId, opponentId)) {
    return res.status(403).json({ error: "Not friends" });
  }
  if (!wager || Number.isNaN(wager) || wager <= 0) {
    return res.status(400).json({ error: "Invalid wager" });
  }
  const cleanToken = String(token || "USDC").toUpperCase();
  const cleanChain = String(chain || "base").toLowerCase();
  const tokenConfig = resolveBlackjackToken(cleanChain, cleanToken);
  if (CHAIN_MODE === "evm" && !tokenConfig) {
    return res.status(400).json({ error: `Unsupported token ${cleanToken} on ${cleanChain}` });
  }
  const players = [
    { user_id: req.session.userId, status: "accepted", deposit_amount: cleanType === "coinflip" ? wager : null },
    { user_id: opponentId, status: "invited", deposit_amount: cleanType === "coinflip" ? wager : null },
  ];
  const inviteDeadline = new Date(Date.now() + GAME_INVITE_TTL_MS).toISOString();
  const info = run(
    `
      INSERT INTO mini_game_matches
        (game_type, inviter_id, status, chain, token, wager_amount, invite_deadline, players_json, state_json)
      VALUES (?, ?, 'invited', ?, ?, ?, ?, ?, ?)
    `,
    [
      cleanType,
      req.session.userId,
      cleanChain,
      cleanToken,
      wager,
      inviteDeadline,
      JSON.stringify(players),
      JSON.stringify({ countdownEndsAt: null, result: null, coinSide: null }),
    ]
  );
  const matchId = info.lastInsertRowid;
  const inviter = getOne("SELECT username FROM users WHERE id = ?", [req.session.userId]);
  const payload = JSON.stringify({ matchId, gameType: cleanType, wagerAmount: wager, token: cleanToken, expiresAt: inviteDeadline });
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id, context, ref_id, payload) VALUES (?, 'minigame_invite', ?, ?, 'minigame', ?, ?)",
    [
      opponentId,
      `@${inviter?.username || "user"} invited you to ${cleanType === "coinflip" ? "Coinflip" : "Big Bank Small Bank"}`,
      req.session.userId,
      matchId,
      payload,
    ]
  );
  io.to(`user:${opponentId}`).emit("notify:new");
  res.json({ ok: true, matchId });
});

app.post("/api/minigames/:id/accept", requireAuth, (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  if (!match) return res.status(404).json({ error: "Game not found" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  if (match.status === "invited" && isExpiredIso(match.invite_deadline)) {
    match.status = "expired";
    player.status = "expired";
    saveMiniGameMatch(match);
    emitMiniGameUpdate(match);
    return res.status(410).json({ error: "Invitation expired" });
  }
  if (match.status !== "invited" && match.status !== "deposit") {
    return res.status(400).json({ error: "Game not joinable" });
  }
  player.status = "accepted";
  if ((match.players || []).every((p) => p.status === "accepted")) {
    match.status = "deposit";
    match.deposit_deadline = new Date(Date.now() + 2 * 60 * 1000).toISOString();
    match.state = {
      ...(match.state || {}),
      countdownEndsAt: match.deposit_deadline,
      result: null,
      coinSide: null,
    };
  }
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true, match: sanitizeMiniGameMatch(match, req.session.userId) });
});

app.post("/api/minigames/:id/decline", requireAuth, (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  if (!match) return res.status(404).json({ error: "Game not found" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  match.status = "declined";
  player.status = "declined";
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true });
});

app.get("/api/minigames/:id", requireAuth, (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  if (!match) return res.status(404).json({ error: "Game not found" });
  const player = (match.players || []).some((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  Promise.resolve()
    .then(async () => {
      if (CHAIN_MODE === "evm" && (match.status === "deposit" || match.status === "revealing" || match.status === "ended" || match.status === "settled")) {
        await syncMiniGameOnchainState(match);
      }
      if (match.status === "revealing") {
        await finalizeMiniGameAfterReveal(match);
      }
      saveMiniGameMatch(match);
      emitMiniGameUpdate(match);
      res.json({ match: sanitizeMiniGameMatch(match, req.session.userId) });
    })
    .catch((err) => {
      res.status(500).json({ error: formatEvmError(err, "Unable to load game") });
    });
});

app.post("/api/minigames/:id/wallet", requireAuth, async (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  const { walletAddress } = req.body || {};
  if (!match) return res.status(404).json({ error: "Game not found" });
  if (match.status !== "deposit") return res.status(400).json({ error: "Wallet registration is closed" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  let normalized;
  try {
    normalized = ethers.getAddress(String(walletAddress || "").trim());
  } catch {
    return res.status(400).json({ error: "Invalid wallet address" });
  }
  player.wallet_address = normalized;
  const readyForEscrow =
    CHAIN_MODE === "evm" &&
    (match.players || []).every((entry) => !!entry.wallet_address) &&
    !match.escrow_match_id;
  if (readyForEscrow) {
    try {
      await createOnchainMiniGameMatch(match);
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to create game escrow") });
    }
  }
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true, match: sanitizeMiniGameMatch(match, req.session.userId) });
});

app.post("/api/minigames/:id/deposit/mock", requireAuth, async (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  const { amount } = req.body || {};
  if (!match) return res.status(404).json({ error: "Game not found" });
  if (match.status !== "deposit") return res.status(400).json({ error: "Deposits are closed" });
  if (CHAIN_MODE !== "mock") return res.status(403).json({ error: "Mock deposits disabled" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  const depositAmount = match.game_type === "coinflip" ? Number(match.wager_amount) : Number(amount);
  if (!depositAmount || Number.isNaN(depositAmount) || depositAmount <= 0) {
    return res.status(400).json({ error: "Invalid deposit amount" });
  }
  player.deposit_amount = depositAmount;
  player.deposited_amount = depositAmount;
  player.status = "deposited";
  const allDeposited = (match.players || []).every((p) => p.status === "deposited");
  if (allDeposited) {
    if (match.game_type === "bigbank") {
      beginMiniGameReveal(match);
    } else {
      match.status = "ended";
      resolveMiniGameWinner(match);
      maybeNotifyMiniGameWinner(match);
    }
  }
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true, match: sanitizeMiniGameMatch(match, req.session.userId) });
});

app.post("/api/minigames/:id/sync", requireAuth, async (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  if (!match) return res.status(404).json({ error: "Game not found" });
  const player = (match.players || []).some((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  if (CHAIN_MODE === "evm") {
    try {
      await syncMiniGameOnchainState(match);
      const allDeposited = (match.players || []).every((p) => Number(p.deposited_amount || 0) > 0);
      if (match.status === "deposit" && allDeposited) {
        if (match.game_type === "bigbank") {
          beginMiniGameReveal(match);
        } else {
          match.status = "ended";
          resolveMiniGameWinner(match);
          await finalizeMiniGameOnchainMatch(match);
          maybeNotifyMiniGameWinner(match);
        }
      }
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to sync game escrow") });
    }
  }
  if (match.status === "revealing") {
    try {
      await finalizeMiniGameAfterReveal(match);
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to finalize reveal") });
    }
  }
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true, match: sanitizeMiniGameMatch(match, req.session.userId) });
});

app.post("/api/minigames/:id/claim", requireAuth, async (req, res) => {
  const match = getMiniGameMatch(Number(req.params.id));
  const { walletAddress, txHash } = req.body || {};
  if (!match) return res.status(404).json({ error: "Game not found" });
  if (match.status !== "ended" && match.status !== "settled") {
    return res.status(400).json({ error: "Game not finalized" });
  }
  if (match.winner_id !== req.session.userId) {
    return res.status(403).json({ error: "Only winner can claim" });
  }
  const addr = String(walletAddress || "").trim();
  if (addr) {
    try {
      match.claim_address = ethers.getAddress(addr);
    } catch {
      return res.status(400).json({ error: "Invalid wallet address" });
    }
  }
  if (CHAIN_MODE === "mock") {
    match.settlement_tx = `mock-${Date.now()}`;
  } else if (txHash) {
    match.settlement_tx = String(txHash);
  } else {
    try {
      await syncMiniGameOnchainState(match);
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to verify claim") });
    }
  }
  match.status = "settled";
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true, match: sanitizeMiniGameMatch(match, req.session.userId) });
});

app.post("/api/minigames/:id/deposit", requireAuth, (req, res) => {
  if (CHAIN_MODE === "evm") {
    return res.status(403).json({ error: "Use the on-chain deposit flow" });
  }
  const match = getMiniGameMatch(Number(req.params.id));
  const { amount } = req.body || {};
  if (!match) return res.status(404).json({ error: "Game not found" });
  if (match.status !== "deposit") return res.status(400).json({ error: "Deposits are closed" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  const depositAmount = match.game_type === "coinflip" ? Number(match.wager_amount) : Number(amount);
  if (!depositAmount || Number.isNaN(depositAmount) || depositAmount <= 0) {
    return res.status(400).json({ error: "Invalid deposit amount" });
  }
  player.deposit_amount = depositAmount;
  player.deposited_amount = depositAmount;
  player.status = "deposited";
  const allDeposited = (match.players || []).every((p) => p.status === "deposited");
  if (allDeposited) {
    if (match.game_type === "bigbank") {
      beginMiniGameReveal(match);
    } else {
      match.status = "ended";
      resolveMiniGameWinner(match);
      maybeNotifyMiniGameWinner(match);
    }
  }
  saveMiniGameMatch(match);
  emitMiniGameUpdate(match);
  res.json({ ok: true, match: sanitizeMiniGameMatch(match, req.session.userId) });
});

app.post("/api/blackjack/invite", requireAuth, (req, res) => {
  const { toId, wagerAmount, token, chain } = req.body || {};
  const opponentId = Number(toId);
  const wager = Number(wagerAmount);
  if (!opponentId || opponentId === req.session.userId) {
    return res.status(400).json({ error: "Invalid opponent" });
  }
  if (!areUsersFriends(req.session.userId, opponentId)) {
    return res.status(403).json({ error: "Not friends" });
  }
  if (!wager || Number.isNaN(wager) || wager <= 0) {
    return res.status(400).json({ error: "Invalid wager" });
  }
  const cleanToken = String(token || "USDC").toUpperCase();
  const cleanChain = String(chain || "base").toLowerCase();
  const tokenConfig = resolveBlackjackToken(cleanChain, cleanToken);
  if (CHAIN_MODE === "evm" && !tokenConfig) {
    return res.status(400).json({ error: `Unsupported token ${cleanToken} on ${cleanChain}` });
  }
  const players = [
    {
      user_id: req.session.userId,
      status: "accepted",
      deposited_amount: 0,
      deposit_tx: null,
      wallet_address: null,
    },
    {
      user_id: opponentId,
      status: "invited",
      deposited_amount: 0,
      deposit_tx: null,
      wallet_address: null,
    },
  ];
  const inviteDeadline = new Date(Date.now() + GAME_INVITE_TTL_MS).toISOString();
  const info = run(
    `
      INSERT INTO blackjack_matches
        (inviter_id, status, chain, token, token_address, wager_amount, invite_deadline, players_json)
      VALUES (?, 'invited', ?, ?, ?, ?, ?, ?)
    `,
    [
      req.session.userId,
      cleanChain,
      cleanToken,
      tokenConfig?.address || null,
      wager,
      inviteDeadline,
      JSON.stringify(players),
    ]
  );
  const matchId = info.lastInsertRowid;
  const inviter = getOne("SELECT username FROM users WHERE id = ?", [req.session.userId]);
  const inviterName = inviter?.username || "user";
  const payload = JSON.stringify({
    matchId,
    chain: cleanChain,
    token: cleanToken,
    wagerAmount: wager,
    expiresAt: inviteDeadline,
  });
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id, context, ref_id, payload) VALUES (?, 'blackjack_invite', ?, ?, 'blackjack', ?, ?)",
    [
      opponentId,
      `@${inviterName} invited you to Blackjack`,
      req.session.userId,
      matchId,
      payload,
    ]
  );
  io.to(`user:${opponentId}`).emit("notify:new");
  res.json({ ok: true, matchId });
});

app.post("/api/blackjack/:id/accept", requireAuth, (req, res) => {
  const matchId = Number(req.params.id);
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  if (match.status === "invited" && isExpiredIso(match.invite_deadline)) {
    match.status = "expired";
    player.status = "expired";
    saveBlackjackMatch(match);
    emitBlackjackUpdate(match);
    return res.status(410).json({ error: "Invitation expired" });
  }
  if (match.status !== "invited" && match.status !== "deposit") {
    return res.status(400).json({ error: "Match not joinable" });
  }
  player.status = "accepted";
  const allAccepted = (match.players || []).every((p) => p.status === "accepted");
  if (allAccepted && match.status === "invited") {
    match.status = "deposit";
    match.deposit_deadline = new Date(Date.now() + 2 * 60 * 1000).toISOString();
    if (CHAIN_MODE === "mock") {
      match.escrow_address = createMockEscrowAddress();
    }
  }
  saveBlackjackMatch(match);
  emitBlackjackUpdate(match);
  res.json({ ok: true, match: sanitizeBlackjackMatch(match) });
});

app.post("/api/blackjack/:id/decline", requireAuth, (req, res) => {
  const matchId = Number(req.params.id);
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  match.status = "declined";
  player.status = "declined";
  saveBlackjackMatch(match);
  const inviterId = match.inviter_id;
  const decliner = getOne("SELECT username FROM users WHERE id = ?", [req.session.userId]);
  run(
    "INSERT INTO notifications (user_id, type, message, from_user_id, context, ref_id) VALUES (?, 'blackjack_declined', ?, ?, 'blackjack', ?)",
    [
      inviterId,
      `@${decliner?.username || "user"} declined your Blackjack invite`,
      req.session.userId,
      matchId,
    ]
  );
  io.to(`user:${inviterId}`).emit("notify:new");
  emitBlackjackUpdate(match);
  res.json({ ok: true });
});

app.get("/api/blackjack/:id", requireAuth, (req, res) => {
  const matchId = Number(req.params.id);
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  const participant = (match.players || []).some((p) => p.user_id === req.session.userId);
  if (!participant) return res.status(403).json({ error: "Not a participant" });
  Promise.resolve()
    .then(async () => {
      if (CHAIN_MODE === "evm" && (match.status === "deposit" || match.status === "ended" || match.status === "settled")) {
        await syncBlackjackOnchainState(match);
        saveBlackjackMatch(match);
      }
      res.json({ match: sanitizeBlackjackMatch(match) });
    })
    .catch((err) => {
      res.status(500).json({ error: formatEvmError(err, "Unable to load blackjack match") });
    });
});

app.post("/api/blackjack/:id/wallet", requireAuth, async (req, res) => {
  const matchId = Number(req.params.id);
  const { walletAddress } = req.body || {};
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  if (match.status !== "deposit") return res.status(400).json({ error: "Wallet registration is closed" });
  let normalized;
  try {
    normalized = ethers.getAddress(String(walletAddress || "").trim());
  } catch {
    return res.status(400).json({ error: "Invalid wallet address" });
  }
  player.wallet_address = normalized;
  const readyForEscrow =
    CHAIN_MODE === "evm" &&
    (match.players || []).every((entry) => !!entry.wallet_address) &&
    !match.escrow_match_id;
  if (readyForEscrow) {
    try {
      await createOnchainBlackjackMatch(match);
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to create escrow match") });
    }
  }
  saveBlackjackMatch(match);
  emitBlackjackUpdate(match);
  res.json({ ok: true, match: sanitizeBlackjackMatch(match) });
});

app.post("/api/blackjack/:id/deposit/mock", requireAuth, (req, res) => {
  if (CHAIN_MODE !== "mock") {
    return res.status(403).json({ error: "Mock deposits disabled" });
  }
  const matchId = Number(req.params.id);
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  if (match.status !== "deposit") {
    return res.status(400).json({ error: "Deposits not open" });
  }
  const player = (match.players || []).find((p) => p.user_id === req.session.userId);
  if (!player) return res.status(403).json({ error: "Not a participant" });
  player.status = "deposited";
  player.deposit_tx = `mock-${Date.now()}`;
  player.deposited_amount = match.wager_amount;
  const allDeposited = (match.players || []).every((p) => p.status === "deposited");
  if (allDeposited) {
    match.status = "active";
    match.state = initBlackjackState(
      match.players.map((p) => p.user_id),
      match.wager_amount
    );
  }
  saveBlackjackMatch(match);
  emitBlackjackUpdate(match);
  res.json({ ok: true, match: sanitizeBlackjackMatch(match) });
});

app.post("/api/blackjack/:id/sync", requireAuth, async (req, res) => {
  const matchId = Number(req.params.id);
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  const participant = (match.players || []).some((p) => p.user_id === req.session.userId);
  if (!participant) return res.status(403).json({ error: "Not a participant" });
  if (CHAIN_MODE === "evm") {
    try {
      await syncBlackjackOnchainState(match);
      saveBlackjackMatch(match);
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to sync deposits") });
    }
  }
  emitBlackjackUpdate(match);
  res.json({ ok: true, match: sanitizeBlackjackMatch(match) });
});

app.post("/api/blackjack/:id/action", requireAuth, async (req, res) => {
  const matchId = Number(req.params.id);
  const { action } = req.body || {};
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  if (match.status !== "active") return res.status(400).json({ error: "Match not active" });
  if (!match.state) return res.status(400).json({ error: "Match state missing" });
  if (match.state.currentPlayerId !== req.session.userId) {
    return res.status(403).json({ error: "Not your turn" });
  }
  const result = applyPlayerAction(match.state, req.session.userId, String(action || "").toLowerCase());
  if (!result.ok) return res.status(400).json({ error: result.error });
  if (match.state.phase === "settled") {
    match.status = "ended";
    match.winner_id = calculateWinnerFromState(match.state);
    if (match.winner_id && CHAIN_MODE === "evm") {
      try {
        await finalizeBlackjackOnchainMatch(match);
      } catch (err) {
        return res.status(500).json({ error: formatEvmError(err, "Unable to finalize the escrow match") });
      }
    }
    if (match.winner_id) {
      const winner = getOne("SELECT username FROM users WHERE id = ?", [match.winner_id]);
      const payload = JSON.stringify({
        matchId: match.id,
        chain: match.chain,
        token: match.token,
        pot: match.wager_amount * (match.players?.length || 2),
        winnerUsername: winner?.username || "winner",
      });
      run(
        "INSERT INTO notifications (user_id, type, message, from_user_id, context, ref_id, payload) VALUES (?, 'blackjack_claim', ?, ?, 'blackjack', ?, ?)",
        [
          match.winner_id,
          `@${winner?.username || "winner"} has won ${(match.wager_amount * (match.players?.length || 2)).toFixed(2)} ${match.token}.`,
          match.inviter_id,
          match.id,
          payload,
        ]
      );
      io.to(`user:${match.winner_id}`).emit("notify:new");
    }
  }
  saveBlackjackMatch(match);
  emitBlackjackUpdate(match);
  res.json({ ok: true, match: sanitizeBlackjackMatch(match) });
});

app.post("/api/blackjack/:id/claim", requireAuth, async (req, res) => {
  const matchId = Number(req.params.id);
  const { walletAddress, txHash } = req.body || {};
  const match = getBlackjackMatch(matchId);
  if (!match) return res.status(404).json({ error: "Match not found" });
  if (match.status !== "ended" && match.status !== "settled") {
    return res.status(400).json({ error: "Match not finalized" });
  }
  if (match.winner_id !== req.session.userId) {
    return res.status(403).json({ error: "Only winner can claim" });
  }
  const addr = String(walletAddress || "").trim();
  if (addr) {
    try {
      match.claim_address = ethers.getAddress(addr);
    } catch {
      return res.status(400).json({ error: "Invalid wallet address" });
    }
  }
  if (CHAIN_MODE === "mock") {
    match.settlement_tx = `mock-${Date.now()}`;
  } else if (txHash) {
    match.settlement_tx = String(txHash);
  } else {
    try {
      await syncBlackjackOnchainState(match);
    } catch (err) {
      return res.status(500).json({ error: formatEvmError(err, "Unable to verify claim") });
    }
  }
  match.status = "settled";
  saveBlackjackMatch(match);
  emitBlackjackUpdate(match);
  res.json({ ok: true, match: sanitizeBlackjackMatch(match) });
});

app.post("/api/groups", requireAuth, (req, res) => {
  const { name, memberIds } = req.body || {};
  let cleanName = (name || "").trim();
  const members = Array.isArray(memberIds) ? memberIds : [];
  if (!cleanName || members.length === 0) {
    if (members.length === 0) {
      return res.status(400).json({ error: "Members required" });
    }
  }
  const uniqueMembers = [...new Set(members.map(Number))].filter(
    (id) => id && id !== req.session.userId
  );
  if (uniqueMembers.length + 1 > MAX_GROUP_MEMBERS) {
    return res
      .status(400)
      .json({ error: `Group limit is ${MAX_GROUP_MEMBERS} members` });
  }
  for (const id of uniqueMembers) {
    if (!isFriends(req.session.userId, id)) {
      return res.status(403).json({ error: "All members must be friends" });
    }
  }
  if (!cleanName) {
    const names = getAll(
      `SELECT username FROM users WHERE id IN (${uniqueMembers
        .map(() => "?")
        .join(",")})`,
      uniqueMembers
    )
      .map((row) => row.username)
      .filter(Boolean);
    cleanName = names.join(", ");
  }
  const info = run("INSERT INTO groups (name, owner_id) VALUES (?, ?)", [
    cleanName,
    req.session.userId,
  ]);
  const inviterRow = getOne("SELECT username FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  const inviterName = inviterRow?.username || "user";
  const groupId = info.lastInsertRowid;
  run(
    "INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'owner')",
    [groupId, req.session.userId]
  );
  uniqueMembers.forEach((id) => {
    run(
      "INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'member')",
      [groupId, id]
    );
    run(
      "INSERT INTO notifications (user_id, type, message, from_user_id, group_id, context) VALUES (?, 'group_invite', ?, ?, ?, 'group')",
      [
        id,
        `@${inviterName} has added you to ${cleanName}`,
        req.session.userId,
        groupId,
      ]
    );
    io.to(`user:${id}`).emit("notify:new");
  });
  res.json({ ok: true, groupId });
});

app.get("/api/groups", requireAuth, (req, res) => {
  const rows = getAll(
    `
    SELECT g.id, g.name, g.owner_id
    FROM groups g
    JOIN group_members gm ON gm.group_id = g.id
    WHERE gm.user_id = ?
    ORDER BY datetime(g.created_at) DESC
    `,
    [req.session.userId]
  );
  if (rows.length === 0) {
    return res.json({ groups: [] });
  }
  const ids = rows.map((row) => row.id);
  const placeholders = ids.map(() => "?").join(",");
  const memberRows = getAll(
    `
    SELECT gm.group_id, u.id, u.username, u.display_name, u.bio, u.avatar, u.status
    FROM group_members gm
    JOIN users u ON u.id = gm.user_id
    WHERE gm.group_id IN (${placeholders})
    ORDER BY u.username COLLATE NOCASE
    `,
    ids
  );
  const aliasMap = getAliasesForUsers(memberRows.map((row) => row.id));
  const membersByGroup = {};
  memberRows.forEach((row) => {
    const list = membersByGroup[row.group_id] || [];
    list.push({
      id: row.id,
      username: row.username,
      display_name: row.display_name,
      bio: row.bio,
      avatar: row.avatar,
      status: onlineUsers.get(row.id) || row.status,
      aliases: aliasMap.get(row.id) || [],
    });
    membersByGroup[row.group_id] = list;
  });
  const groups = rows.map((row) => ({
    ...row,
    members: membersByGroup[row.id] || [],
  }));
  res.json({ groups });
});

app.get("/api/groups/:id/members", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  const rows = getAll(
    `
    SELECT u.id, u.username, u.display_name, u.custom_status, u.bio, u.avatar, u.last_seen, gm.role
    FROM group_members gm
    JOIN users u ON u.id = gm.user_id
    WHERE gm.group_id = ?
    ORDER BY u.username COLLATE NOCASE
    `,
    [groupId]
  );
  const aliasMap = getAliasesForUsers(rows.map((row) => row.id));
  const members = rows.map((row) => ({
    ...row,
    status: onlineUsers.get(row.id) || "offline",
    aliases: aliasMap.get(row.id) || [],
  }));
  res.json({ members });
});

app.post("/api/groups/:id/members", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const { memberIds } = req.body || {};
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  const members = Array.isArray(memberIds) ? memberIds : [];
  const uniqueMembers = [...new Set(members.map(Number))].filter(
    (id) => id && id !== req.session.userId
  );
  if (uniqueMembers.length === 0) {
    return res.status(400).json({ error: "Members required" });
  }
  const existingRows = getAll(
    `SELECT user_id FROM group_members WHERE group_id = ? AND user_id IN (${uniqueMembers
      .map(() => "?")
      .join(",")})`,
    [groupId, ...uniqueMembers]
  );
  const existingSet = new Set(existingRows.map((row) => row.user_id));
  const toAdd = uniqueMembers.filter((id) => !existingSet.has(id));
  if (toAdd.length === 0) {
    return res.json({ ok: true });
  }
  const countRow = getOne(
    "SELECT COUNT(*) AS count FROM group_members WHERE group_id = ?",
    [groupId]
  );
  const currentCount = countRow?.count || 0;
  if (currentCount + toAdd.length > MAX_GROUP_MEMBERS) {
    return res
      .status(400)
      .json({ error: `Group limit is ${MAX_GROUP_MEMBERS} members` });
  }
  for (const id of toAdd) {
    if (!isFriends(req.session.userId, id)) {
      return res.status(403).json({ error: "All members must be friends" });
    }
  }
  const group = getOne("SELECT name FROM groups WHERE id = ?", [groupId]);
  toAdd.forEach((id) => {
    run(
      "INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'member')",
      [groupId, id]
    );
    run(
      "INSERT INTO notifications (user_id, type, message, from_user_id, group_id, context) VALUES (?, 'group_invite', ?, ?, ?, 'group')",
      [
        id,
        `@${inviter} has added you to ${group?.name || "Group"}`,
        req.session.userId,
        groupId,
      ]
    );
    io.to(`user:${id}`).emit("notify:new");
  });
  res.json({ ok: true });
});

app.post("/api/groups/:id/remove", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const targetId = Number(req.body?.userId);
  if (!targetId) {
    return res.status(400).json({ error: "User required" });
  }
  const group = getOne("SELECT id, owner_id FROM groups WHERE id = ?", [groupId]);
  if (!group) {
    return res.status(404).json({ error: "Group not found" });
  }
  let groupDeleted = false;
  if (group.owner_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  if (targetId === group.owner_id) {
    return res.status(400).json({ error: "Owner cannot be removed" });
  }
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, targetId]
  );
  if (!member) {
    return res.status(404).json({ error: "User not in group" });
  }
  run("DELETE FROM group_members WHERE group_id = ? AND user_id = ?", [
    groupId,
    targetId,
  ]);
  const actor = getOne("SELECT username FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  const target = getOne("SELECT username FROM users WHERE id = ?", [targetId]);
  const body = `@${actor?.username || "user"} removed @${target?.username || "user"} from the group`;
  const info = run(
    "INSERT INTO group_messages (group_id, sender_id, body, type, image_url, is_system) VALUES (?, ?, ?, 'text', NULL, 1)",
    [groupId, req.session.userId, body]
  );
  io.to(`group:${groupId}`).emit("group:new", {
    id: info.lastInsertRowid,
    group_id: groupId,
    sender_id: req.session.userId,
    body,
    is_system: 1,
    created_at: new Date().toISOString(),
    edited_at: null,
    deleted_at: null,
  });
  io.to(`group:${groupId}`).emit("group:members:update", { groupId });
  io.to(`user:${targetId}`).emit("group:removed", { groupId });
  res.json({ ok: true });
});

app.post("/api/groups/:id/owner", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const targetId = Number(req.body?.userId);
  if (!targetId) {
    return res.status(400).json({ error: "User required" });
  }
  const group = getOne("SELECT id, owner_id FROM groups WHERE id = ?", [groupId]);
  if (!group) {
    return res.status(404).json({ error: "Group not found" });
  }
  if (group.owner_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, targetId]
  );
  if (!member) {
    return res.status(404).json({ error: "User not in group" });
  }
  run("UPDATE groups SET owner_id = ? WHERE id = ?", [targetId, groupId]);
  run(
    "UPDATE group_members SET role = CASE WHEN user_id = ? THEN 'owner' ELSE 'member' END WHERE group_id = ?",
    [targetId, groupId]
  );
  io.to(`group:${groupId}`).emit("group:members:update", {
    groupId,
    ownerId: targetId,
  });
  res.json({ ok: true });
});

app.get("/api/groups/:id/messages", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  const rows = getAll(
    `
    SELECT id, group_id, sender_id, body, type, image_url, audio_url, forwarded_from_id, forwarded_from_username, forwarded_from_display, is_system, created_at, edited_at, deleted_at
    FROM group_messages
    WHERE group_id = ? AND deleted_at IS NULL
    ORDER BY datetime(created_at) ASC
    LIMIT 500
    `,
    [groupId]
  );
  const ids = rows.map((row) => row.id);
  const reactions = getReactions("group", ids, req.session.userId);
  const messages = rows.map((row) => ({
    ...row,
    reactions: reactions[row.id] || [],
  }));
  res.json({ messages });
});

app.patch("/api/groups/:groupId/messages/:id", requireAuth, (req, res) => {
  const groupId = Number(req.params.groupId);
  const id = Number(req.params.id);
  const { body } = req.body || {};
  if (!body) {
    return res.status(400).json({ error: "Body required" });
  }
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  const message = getOne(
    "SELECT * FROM group_messages WHERE id = ? AND group_id = ?",
    [id, groupId]
  );
  if (!message || message.sender_id !== req.session.userId || message.is_system) {
    return res.status(403).json({ error: "Not allowed" });
  }
  run(
    "UPDATE group_messages SET body = ?, edited_at = CURRENT_TIMESTAMP WHERE id = ?",
    [body.slice(0, 2000), id]
  );
  const updated = {
    id,
    group_id: groupId,
    sender_id: message.sender_id,
    body: body.slice(0, 2000),
    created_at: message.created_at,
    edited_at: new Date().toISOString(),
    is_system: 0,
    type: message.type || "text",
    image_url: message.image_url || null,
    audio_url: message.audio_url || null,
    forwarded_from_id: message.forwarded_from_id || null,
    forwarded_from_username: message.forwarded_from_username || null,
    forwarded_from_display: message.forwarded_from_display || null,
  };
  io.to(`group:${groupId}`).emit("group:edit", updated);
  res.json({ message: updated });
});

app.post("/api/groups/:id/read", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  upsertGroupRead(req.session.userId, groupId);
  res.json({ ok: true });
});

app.post("/api/groups/:id/leave", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const group = getOne("SELECT id, owner_id FROM groups WHERE id = ?", [groupId]);
  if (!group) {
    return res.status(404).json({ error: "Group not found" });
  }
  run("DELETE FROM group_members WHERE group_id = ? AND user_id = ?", [
    groupId,
    req.session.userId,
  ]);
  if (group.owner_id === req.session.userId) {
    const nextOwner = getOne(
      "SELECT user_id FROM group_members WHERE group_id = ? ORDER BY datetime(joined_at) ASC LIMIT 1",
      [groupId]
    );
    if (nextOwner?.user_id) {
      run("UPDATE groups SET owner_id = ? WHERE id = ?", [nextOwner.user_id, groupId]);
      run(
        "UPDATE group_members SET role = CASE WHEN user_id = ? THEN 'owner' ELSE 'member' END WHERE group_id = ?",
        [nextOwner.user_id, groupId]
      );
      io.to(`group:${groupId}`).emit("group:members:update", {
        groupId,
        ownerId: nextOwner.user_id,
      });
    } else {
      run("DELETE FROM groups WHERE id = ?", [groupId]);
      run("DELETE FROM group_messages WHERE group_id = ?", [groupId]);
      groupDeleted = true;
    }
  }
  if (groupDeleted) {
    return res.json({ ok: true });
  }
  const userRow = getOne("SELECT username FROM users WHERE id = ?", [
    req.session.userId,
  ]);
  const body = `@${userRow?.username || "user"} left the group`;
  const info = run(
    "INSERT INTO group_messages (group_id, sender_id, body, type, image_url, is_system) VALUES (?, ?, ?, 'text', NULL, 1)",
    [groupId, req.session.userId, body]
  );
  io.to(`group:${groupId}`).emit("group:new", {
    id: info.lastInsertRowid,
    group_id: groupId,
    sender_id: req.session.userId,
    body,
    is_system: 1,
    created_at: new Date().toISOString(),
    edited_at: null,
    deleted_at: null,
  });
  res.json({ ok: true });
});

app.delete("/api/groups/:groupId/messages/:id", requireAuth, (req, res) => {
  const groupId = Number(req.params.groupId);
  const id = Number(req.params.id);
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  const message = getOne(
    "SELECT * FROM group_messages WHERE id = ? AND group_id = ?",
    [id, groupId]
  );
  if (!message) {
    return res.status(404).json({ error: "Message not found" });
  }
  if (message.sender_id !== req.session.userId) {
    return res.status(403).json({ error: "Not allowed" });
  }
  run(
    "UPDATE group_messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
    [id]
  );
  run(
    "DELETE FROM message_reactions WHERE message_type = 'group' AND message_id = ?",
    [id]
  );
  io.to(`group:${groupId}`).emit("message:deleted", {
    messageType: "group",
    messageId: id,
    groupId,
  });
  res.json({ ok: true });
});

app.patch("/api/groups/:id", requireAuth, (req, res) => {
  const groupId = Number(req.params.id);
  const body = req.body || {};
  const name = typeof body.name === "string" ? body.name : null;
  const avatar = typeof body.avatar === "string" ? body.avatar : null;
  const member = getOne(
    "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
    [groupId, req.session.userId]
  );
  if (!member) {
    return res.status(403).json({ error: "Not a member" });
  }
  const group = getOne("SELECT name, avatar FROM groups WHERE id = ?", [groupId]);
  if (!group) {
    return res.status(404).json({ error: "Group not found" });
  }
  const cleanName = name !== null ? name.trim() : "";
  const nextName = cleanName ? cleanName : group.name;
  const hasAvatar = Object.prototype.hasOwnProperty.call(body, "avatar");
  const cleanAvatar = hasAvatar ? (avatar || "").trim() : null;
  const nextAvatar = hasAvatar ? (cleanAvatar || null) : group.avatar;
  run("UPDATE groups SET name = ?, avatar = ? WHERE id = ?", [nextName, nextAvatar, groupId]);
  if (cleanName && cleanName !== group.name) {
    const userRow = getOne("SELECT username, display_name FROM users WHERE id = ?", [
      req.session.userId,
    ]);
    const actorName = userRow?.username || "user";
    const body = `@${actorName} changed the group name to "${cleanName}"`;
    const info = run(
      "INSERT INTO group_messages (group_id, sender_id, body, type, image_url, is_system) VALUES (?, ?, ?, 'text', NULL, 1)",
      [groupId, req.session.userId, body]
    );
    io.to(`group:${groupId}`).emit("group:new", {
      id: info.lastInsertRowid,
      group_id: groupId,
      sender_id: req.session.userId,
      body,
      is_system: 1,
      created_at: new Date().toISOString(),
      edited_at: null,
      deleted_at: null,
    });
  }
  res.json({ ok: true });
});

io.use((socket, next) => {
  sessionMiddleware(socket.request, {}, next);
});

io.on("connection", (socket) => {
  const session = socket.request.session;
  const userId = session?.userId;
  if (!userId) {
    socket.disconnect();
    return;
  }
  const allowedStatuses = new Set(["online", "idle", "busy", "away", "invisible"]);
  const row = getOne("SELECT status FROM users WHERE id = ?", [userId]);
  const nextStatus = allowedStatuses.has(row?.status) ? row.status : "online";
  onlineUsers.set(userId, nextStatus);
  run("UPDATE users SET status = ? WHERE id = ?", [nextStatus, userId]);
  io.emit("presence:update", { userId, status: nextStatus });

  const activeOther = activeCalls.get(userId);
  if (activeOther) {
    const session = callSessions.get(callKey(userId, activeOther));
    socket.emit("call:active", {
      otherId: activeOther,
      startedAt: session?.startedAt || Date.now(),
    });
  }
  pendingCalls.forEach((pending) => {
    if (pending.calleeId === userId) {
      socket.emit("call:request", { fromId: pending.callerId });
    } else if (pending.callerId === userId) {
      socket.emit("call:calling", { otherId: pending.calleeId });
    }
  });

  socket.on("dm:send", (payload) => {
    const { toId, body, type, imageUrl, audioUrl, forwardedFrom, story } = payload || {};
    if (!toId) {
      return;
    }
    const cleanType =
      type === "image"
        ? "image"
        : type === "audio"
        ? "audio"
        : type === "story_reply"
        ? "story_reply"
        : type === "story_share"
        ? "story_share"
        : "text";
    const cleanBody = (body || "").trim();
    if (cleanType === "image" && !imageUrl) {
      return;
    }
    if (cleanType === "audio" && !audioUrl) {
      return;
    }
    if ((cleanType === "story_reply" || cleanType === "story_share") && !story?.id) {
      return;
    }
    if (cleanType === "text" && !cleanBody) {
      return;
    }
    if (cleanType === "story_reply" && !cleanBody) {
      return;
    }
    const storyId = story?.id ? Number(story.id) : null;
    const storyOwnerId = story?.ownerId ? Number(story.ownerId) : null;
    const storyMediaUrl = story?.mediaUrl || null;
    const storyMediaType = story?.mediaType || null;
    const fId = forwardedFrom?.id || null;
    const fUsername = forwardedFrom?.username || null;
    const fDisplay = forwardedFrom?.displayName || null;
    const info = run(
      "INSERT INTO messages (sender_id, recipient_id, body, type, image_url, audio_url, story_id, story_owner_id, story_media_url, story_media_type, forwarded_from_id, forwarded_from_username, forwarded_from_display) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [userId, toId, cleanBody.slice(0, 2000), cleanType, imageUrl || null, audioUrl || null, storyId, storyOwnerId, storyMediaUrl, storyMediaType, fId, fUsername, fDisplay]
    );
    const message = {
      id: info.lastInsertRowid,
      sender_id: userId,
      recipient_id: toId,
      body: cleanBody.slice(0, 2000),
      type: cleanType,
      image_url: imageUrl || null,
      audio_url: audioUrl || null,
      story_id: storyId,
      story_owner_id: storyOwnerId,
      story_media_url: storyMediaUrl,
      story_media_type: storyMediaType,
      forwarded_from_id: fId,
      forwarded_from_username: fUsername,
      forwarded_from_display: fDisplay,
      created_at: new Date().toISOString(),
      edited_at: null,
      deleted_at: null,
    };
    socket.emit("dm:new", message);
    io.to(`user:${toId}`).emit("dm:new", message);

    const mentions = extractMentions(cleanBody);
    if (mentions.length > 0) {
      const recipient = getOne(
        "SELECT id, username FROM users WHERE id = ?",
        [toId]
      );
      const sender = getOne(
        "SELECT username FROM users WHERE id = ?",
        [userId]
      );
      const aliasRows = getAll(
        "SELECT username FROM username_aliases WHERE user_id = ?",
        [toId]
      );
      const aliasSet = new Set(aliasRows.map((row) => row.username));
      const matched =
        recipient &&
        mentions.some(
          (m) =>
            m.toLowerCase() === recipient.username?.toLowerCase() ||
            Array.from(aliasSet).some(
              (alias) => alias.toLowerCase() === m.toLowerCase()
            )
        );
      if (matched) {
        run(
          "INSERT INTO notifications (user_id, type, message, from_user_id, context) VALUES (?, 'mention', ?, ?, 'dm')",
          [
            recipient.id,
            `@${sender?.username || "user"} has mentioned you`,
            userId,
          ]
        );
        io.to(`user:${recipient.id}`).emit("notify:new");
      }
    }
  });

  socket.on("dm:typing", (payload) => {
    const { toId, isTyping } = payload || {};
    if (!toId) return;
    io.to(`user:${toId}`).emit("dm:typing", {
      fromId: userId,
      isTyping: !!isTyping,
    });
  });

  socket.on("dm:react", (payload) => {
    const { messageId, emoji } = payload || {};
    if (!messageId || !emoji) return;
    const message = getOne("SELECT * FROM messages WHERE id = ?", [messageId]);
    if (
      !message ||
      (message.sender_id !== userId && message.recipient_id !== userId)
    ) {
      return;
    }
    const existing = getOne(
      "SELECT id FROM message_reactions WHERE message_type = 'dm' AND message_id = ? AND user_id = ? AND emoji = ?",
      [messageId, userId, emoji]
    );
    if (existing) {
      run("DELETE FROM message_reactions WHERE id = ?", [existing.id]);
    } else {
      run(
        "INSERT INTO message_reactions (message_type, message_id, user_id, emoji) VALUES ('dm', ?, ?, ?)",
        [messageId, userId, emoji]
      );
    }
    const reactions = getReactions("dm", [messageId], userId)[messageId] || [];
    const otherId =
      message.sender_id === userId ? message.recipient_id : message.sender_id;
    io.to(`user:${userId}`).emit("message:reactions", {
      messageType: "dm",
      messageId,
      otherId,
      reactions,
    });
    io.to(`user:${otherId}`).emit("message:reactions", {
      messageType: "dm",
      messageId,
      otherId,
      reactions,
    });
  });

  socket.on("dm:delete", (payload) => {
    const { userId: otherId } = payload || {};
    if (!otherId) return;
    run(
      `
        DELETE FROM messages
        WHERE (sender_id = ? AND recipient_id = ?)
           OR (sender_id = ? AND recipient_id = ?)
      `,
      [userId, otherId, otherId, userId]
    );
    io.to(`user:${userId}`).emit("dm:deleted", { userId: otherId });
    io.to(`user:${otherId}`).emit("dm:deleted", { userId });
  });

  socket.on("groups:join", (payload) => {
    const { groupIds } = payload || {};
    if (!Array.isArray(groupIds)) return;
    groupIds.forEach((id) => socket.join(`group:${id}`));
  });

  socket.on("group:send", (payload) => {
    const { groupId, body, type, imageUrl, audioUrl, forwardedFrom } = payload || {};
    if (!groupId) return;
    const member = getOne(
      "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
      [groupId, userId]
    );
    if (!member) return;
    const cleanType = type === "image" ? "image" : type === "audio" ? "audio" : "text";
    const cleanBody = (body || "").trim();
    if (cleanType === "image" && !imageUrl) {
      return;
    }
    if (cleanType === "audio" && !audioUrl) {
      return;
    }
    if (cleanType === "text" && !cleanBody) {
      return;
    }
    const fId = forwardedFrom?.id || null;
    const fUsername = forwardedFrom?.username || null;
    const fDisplay = forwardedFrom?.displayName || null;
    const info = run(
      "INSERT INTO group_messages (group_id, sender_id, body, type, image_url, audio_url, forwarded_from_id, forwarded_from_username, forwarded_from_display, is_system) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
      [groupId, userId, cleanBody.slice(0, 2000), cleanType, imageUrl || null, audioUrl || null, fId, fUsername, fDisplay]
    );
    const message = {
      id: info.lastInsertRowid,
      group_id: groupId,
      sender_id: userId,
      body: cleanBody.slice(0, 2000),
      type: cleanType,
      image_url: imageUrl || null,
      audio_url: audioUrl || null,
      forwarded_from_id: fId,
      forwarded_from_username: fUsername,
      forwarded_from_display: fDisplay,
      is_system: 0,
      created_at: new Date().toISOString(),
      edited_at: null,
      deleted_at: null,
    };
    io.to(`group:${groupId}`).emit("group:new", message);

    const mentions = extractMentions(cleanBody);
    if (mentions.length > 0) {
      const sender = getOne(
        "SELECT username FROM users WHERE id = ?",
        [userId]
      );
      const group = getOne(
        "SELECT name FROM groups WHERE id = ?",
        [groupId]
      );
      const placeholders = mentions.map(() => "?").join(",");
      const mentionedUsers = getAll(
        `
        SELECT DISTINCT u.id, u.username
        FROM group_members gm
        JOIN users u ON u.id = gm.user_id
        LEFT JOIN username_aliases ua ON ua.user_id = u.id
        WHERE gm.group_id = ?
          AND (u.username IN (${placeholders}) OR ua.username IN (${placeholders}))
        `,
        [groupId, ...mentions, ...mentions]
      );
      mentionedUsers.forEach((user) => {
        if (user.id === userId) return;
        run(
          "INSERT INTO notifications (user_id, type, message, from_user_id, group_id, context) VALUES (?, 'mention', ?, ?, ?, 'group')",
          [
            user.id,
            `@${sender?.username || "user"} has mentioned you in ${group?.name || "a group"}`,
            userId,
            groupId,
          ]
        );
        io.to(`user:${user.id}`).emit("notify:new");
      });
    }
  });

  socket.on("group:react", (payload) => {
    const { groupId, messageId, emoji } = payload || {};
    if (!groupId || !messageId || !emoji) return;
    const member = getOne(
      "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
      [groupId, userId]
    );
    if (!member) return;
    const message = getOne(
      "SELECT id FROM group_messages WHERE id = ? AND group_id = ?",
      [messageId, groupId]
    );
    if (!message) return;
    const existing = getOne(
      "SELECT id FROM message_reactions WHERE message_type = 'group' AND message_id = ? AND user_id = ? AND emoji = ?",
      [messageId, userId, emoji]
    );
    if (existing) {
      run("DELETE FROM message_reactions WHERE id = ?", [existing.id]);
    } else {
      run(
        "INSERT INTO message_reactions (message_type, message_id, user_id, emoji) VALUES ('group', ?, ?, ?)",
        [messageId, userId, emoji]
      );
    }
    const reactions = getReactions("group", [messageId], userId)[messageId] || [];
    io.to(`group:${groupId}`).emit("message:reactions", {
      messageType: "group",
      messageId,
      groupId,
      reactions,
    });
  });

  function areFriends(otherId) {
    const row = getOne(
      `
        SELECT id FROM friendships
        WHERE (user1_id = ? AND user2_id = ?)
           OR (user1_id = ? AND user2_id = ?)
      `,
      [userId, otherId, otherId, userId]
    );
    return !!row;
  }

  function endCallSession(a, b) {
    const key = callKey(a, b);
    const session = callSessions.get(key);
    if (session) {
      if (session.timerId) {
        clearTimeout(session.timerId);
      }
      callSessions.delete(key);
      const durationSeconds = (Date.now() - session.startedAt) / 1000;
      const caller = getOne("SELECT username FROM users WHERE id = ?", [
        session.callerId,
      ]);
      emitSystemDmMessage(
        session.callerId,
        session.calleeId,
        `@${caller?.username || "user"} started a call that lasted ${formatCallDuration(
          durationSeconds
        )}`,
        "call"
      );
    }
    activeCalls.delete(a);
    activeCalls.delete(b);
    io.to(`user:${a}`).emit("call:end", { fromId: b });
    io.to(`user:${b}`).emit("call:end", { fromId: a });
    io.to(`user:${a}`).emit("call:ended", { otherId: b });
    io.to(`user:${b}`).emit("call:ended", { otherId: a });
  }

  socket.on("call:request", (payload) => {
    const { toId } = payload || {};
    if (!toId || !areFriends(toId)) return;
    if (
      pendingUsers.has(userId) ||
      pendingUsers.has(toId) ||
      activeCalls.has(userId) ||
      activeCalls.has(toId)
    ) {
      io.to(`user:${userId}`).emit("call:decline", { fromId: toId, reason: "busy" });
      return;
    }
    const key = callKey(userId, toId);
    if (pendingCalls.has(key)) return;
    pendingUsers.add(userId);
    pendingUsers.add(toId);
    const timerId = setTimeout(() => {
      const pending = pendingCalls.get(key);
      if (!pending) return;
      pendingCalls.delete(key);
      pendingUsers.delete(pending.callerId);
      pendingUsers.delete(pending.calleeId);
      const caller = getOne("SELECT username FROM users WHERE id = ?", [
        pending.callerId,
      ]);
      const callee = getOne("SELECT username FROM users WHERE id = ?", [
        pending.calleeId,
      ]);
      emitSystemDmMessageTo(
        pending.callerId,
        pending.calleeId,
        `Missed call from @${caller?.username || "user"}`,
        "call"
      );
      emitSystemDmMessage(
        pending.callerId,
        pending.calleeId,
        `No answer from @${callee?.username || "user"}`,
        "call"
      );
      io.to(`user:${pending.callerId}`).emit("call:decline", {
        fromId: pending.calleeId,
        reason: "timeout",
      });
    }, 15000);
    pendingCalls.set(key, { callerId: userId, calleeId: toId, timerId });
    const caller = getOne("SELECT username FROM users WHERE id = ?", [userId]);
    emitSystemDmMessage(
      userId,
      toId,
      `@${caller?.username || "user"} started a call`,
      "call"
    );
    io.to(`user:${toId}`).emit("call:request", { fromId: userId });
  });

  socket.on("call:accept", (payload) => {
    const { toId } = payload || {};
    if (!toId || !areFriends(toId)) return;
    const key = callKey(userId, toId);
    const pending = pendingCalls.get(key);
    if (!pending || pending.calleeId !== userId) return;
    clearTimeout(pending.timerId);
    pendingCalls.delete(key);
    pendingUsers.delete(pending.callerId);
    pendingUsers.delete(pending.calleeId);
    activeCalls.set(userId, toId);
    activeCalls.set(toId, userId);
    const startedAt = Date.now();
    callSessions.set(key, {
      callerId: pending.callerId,
      calleeId: pending.calleeId,
      startedAt,
      timerId: null,
      joined: true,
      participants: new Set([pending.callerId, pending.calleeId]),
      audioStates: new Map([
        [pending.callerId, { muted: false, deafened: false }],
        [pending.calleeId, { muted: false, deafened: false }],
      ]),
    });
    // Start message already sent on call request.
    emitCallActive(
      pending.callerId,
      pending.calleeId,
      startedAt,
      callSessions.get(key)?.audioStates
    );
    io.to(`user:${pending.callerId}`).emit("call:joined", { otherId: pending.calleeId });
    io.to(`user:${pending.calleeId}`).emit("call:joined", { otherId: pending.callerId });
    io.to(`user:${toId}`).emit("call:accept", { fromId: userId });
  });

  socket.on("call:decline", (payload) => {
    const { toId } = payload || {};
    if (!toId) return;
    const key = callKey(userId, toId);
    const pending = pendingCalls.get(key);
    if (pending) {
      clearTimeout(pending.timerId);
      pendingCalls.delete(key);
      pendingUsers.delete(pending.callerId);
      pendingUsers.delete(pending.calleeId);
      const decliner = getOne("SELECT username FROM users WHERE id = ?", [userId]);
      emitSystemDmMessage(
        pending.callerId,
        pending.calleeId,
        `@${decliner?.username || "user"} declined the call`,
        "call"
      );
    }
    io.to(`user:${toId}`).emit("call:decline", { fromId: userId });
  });

  socket.on("call:offer", (payload) => {
    const { toId, offer } = payload || {};
    if (!toId || !offer) return;
    io.to(`user:${toId}`).emit("call:offer", { fromId: userId, offer });
  });

  socket.on("call:answer", (payload) => {
    const { toId, answer } = payload || {};
    if (!toId || !answer) return;
    io.to(`user:${toId}`).emit("call:answer", { fromId: userId, answer });
  });

  socket.on("call:ice", (payload) => {
    const { toId, candidate } = payload || {};
    if (!toId || !candidate) return;
    io.to(`user:${toId}`).emit("call:ice", { fromId: userId, candidate });
  });

  socket.on("call:screen:start", (payload) => {
    const { toId } = payload || {};
    if (!toId) return;
    io.to(`user:${toId}`).emit("call:screen:start", { fromId: userId });
  });

  socket.on("call:screen:stop", (payload) => {
    const { toId } = payload || {};
    if (!toId) return;
    io.to(`user:${toId}`).emit("call:screen:stop", { fromId: userId });
  });

  socket.on("call:rejoin", (payload) => {
    const { toId } = payload || {};
    if (!toId) return;
    const key = callKey(userId, toId);
    const session = callSessions.get(key);
    if (!session) {
      io.to(`user:${userId}`).emit("call:rejoin-denied", { otherId: toId });
      return;
    }
    session.joined = true;
    if (session.participants) {
      session.participants.add(userId);
    }
    if (session.timerId) {
      clearTimeout(session.timerId);
      session.timerId = null;
    }
    io.to(`user:${toId}`).emit("call:rejoin", { fromId: userId });
    io.to(`user:${userId}`).emit("call:rejoin-ack", {
      otherId: toId,
      startedAt: session.startedAt,
      audioStates: serializeAudioStates(session.audioStates),
    });
    io.to(`user:${userId}`).emit("call:joined", { otherId: toId });
    io.to(`user:${toId}`).emit("call:joined", { otherId: userId });
  });

  socket.on("call:status", (payload) => {
    const { toId, muted, deafened } = payload || {};
    const otherId = toId || activeCalls.get(userId);
    if (!otherId) return;
    const key = callKey(userId, otherId);
    const session = callSessions.get(key);
    if (!session) return;
    if (!session.audioStates) {
      session.audioStates = new Map();
    }
    session.audioStates.set(userId, {
      muted: !!muted,
      deafened: !!deafened,
    });
    io.to(`user:${userId}`).emit("call:status", {
      userId,
      muted: !!muted,
      deafened: !!deafened,
    });
    io.to(`user:${otherId}`).emit("call:status", {
      userId,
      muted: !!muted,
      deafened: !!deafened,
    });
  });

  socket.on("call:leave", (payload) => {
    const { toId } = payload || {};
    const otherId = toId || activeCalls.get(userId);
    if (!otherId) return;
    const key = callKey(userId, otherId);
    const session = callSessions.get(key);
    if (session?.participants) {
      session.participants.delete(userId);
      if (session.participants.size === 0) {
        endCallSession(userId, otherId);
        return;
      }
    }
    io.to(`user:${otherId}`).emit("call:left", { otherId: userId });
  });

  socket.on("call:end", (payload) => {
    const { toId } = payload || {};
    const otherId = toId || activeCalls.get(userId);
    if (!otherId) return;
    const key = callKey(userId, otherId);
    const pending = pendingCalls.get(key);
    if (pending) {
      clearTimeout(pending.timerId);
      pendingCalls.delete(key);
      pendingUsers.delete(pending.callerId);
      pendingUsers.delete(pending.calleeId);
      io.to(`user:${otherId}`).emit("call:decline", { fromId: userId });
      return;
    }
    endCallSession(userId, otherId);
  });

  socket.on("group:call:start", (payload) => {
    const { groupId } = payload || {};
    if (!groupId) return;
    const member = getOne(
      "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
      [groupId, userId]
    );
    if (!member) return;
    const members = getAll(
      "SELECT user_id FROM group_members WHERE group_id = ?",
      [groupId]
    );
    if (members.length > 10) {
      io.to(`user:${userId}`).emit("group:call:limit", { groupId });
      return;
    }
    if (!groupCalls.has(groupId)) {
      groupCalls.set(groupId, {
        participants: new Set(),
        startedAt: Date.now(),
        initiatorId: userId,
        answered: false,
        ringTimerId: null,
        soloTimerId: null,
        missedNotified: false,
        audioStates: new Map([[userId, { muted: false, deafened: false }]]),
      });
      const caller = getOne("SELECT username FROM users WHERE id = ?", [userId]);
      emitSystemGroupMessage(
        groupId,
        userId,
        `@${caller?.username || "user"} started a call`,
        "call"
      );
      const callRef = groupCalls.get(groupId);
      callRef.ringTimerId = setTimeout(() => {
        const current = groupCalls.get(groupId);
        if (!current || current.answered) return;
        if (current.participants.size > 1) return;
        if (current.missedNotified) return;
        current.missedNotified = true;
        io.to(`group:${groupId}`).emit("group:call:timeout", { groupId });
      }, 15000);
    }
    io.to(`group:${groupId}`).emit("group:call:ring", {
      groupId,
      fromId: userId,
      startedAt: groupCalls.get(groupId).startedAt,
    });
  });

  socket.on("group:call:join", (payload) => {
    const { groupId } = payload || {};
    if (!groupId) return;
    const member = getOne(
      "SELECT id FROM group_members WHERE group_id = ? AND user_id = ?",
      [groupId, userId]
    );
    if (!member) return;
    const call = groupCalls.get(groupId);
    if (!call) return;
    if (call.participants.size >= 10 && !call.participants.has(userId)) {
      io.to(`user:${userId}`).emit("group:call:limit", { groupId });
      return;
    }
    call.participants.add(userId);
    if (userId !== call.initiatorId) {
      call.answered = true;
    }
    if (!call.audioStates) {
      call.audioStates = new Map();
    }
    if (!call.audioStates.has(userId)) {
      call.audioStates.set(userId, { muted: false, deafened: false });
    }
    if (call.soloTimerId) {
      clearTimeout(call.soloTimerId);
      call.soloTimerId = null;
    }
    if (call.participants.size === 1) {
      call.soloTimerId = setTimeout(() => {
        const current = groupCalls.get(groupId);
        if (!current) return;
        if (current.participants.size !== 1) return;
        const caller = getOne("SELECT username FROM users WHERE id = ?", [
          current.initiatorId,
        ]);
        if (current.answered) {
          emitSystemGroupMessage(
            groupId,
            current.initiatorId,
            `@${caller?.username || "user"} started a call that lasted ${formatCallDuration(
              (Date.now() - current.startedAt) / 1000
            )}`,
            "call"
          );
        } else if (!current.missedNotified) {
          emitSystemGroupMessage(
            groupId,
            current.initiatorId,
            `Missed call from @${caller?.username || "user"}`,
            "call"
          );
        }
        groupCalls.delete(groupId);
        io.to(`group:${groupId}`).emit("group:call:ended", { groupId });
      }, 3 * 60 * 1000);
    }
    const participants = Array.from(call.participants).filter((id) => id !== userId);
    io.to(`user:${userId}`).emit("group:call:participants", {
      groupId,
      participants,
      startedAt: call.startedAt,
      audioStates: serializeAudioStates(call.audioStates),
    });
    io.to(`group:${groupId}`).emit("group:call:join", {
      groupId,
      userId,
    });
    io.to(`group:${groupId}`).emit("group:call:active", {
      groupId,
      startedAt: call.startedAt,
      audioStates: serializeAudioStates(call.audioStates),
    });
  });

  socket.on("group:call:status", (payload) => {
    const { groupId, muted, deafened } = payload || {};
    if (!groupId) return;
    const call = groupCalls.get(groupId);
    if (!call || !call.participants?.has(userId)) return;
    if (!call.audioStates) {
      call.audioStates = new Map();
    }
    call.audioStates.set(userId, {
      muted: !!muted,
      deafened: !!deafened,
    });
    io.to(`group:${groupId}`).emit("group:call:status", {
      groupId,
      userId,
      muted: !!muted,
      deafened: !!deafened,
    });
  });

  socket.on("group:call:leave", (payload) => {
    const { groupId } = payload || {};
    if (!groupId) return;
    const call = groupCalls.get(groupId);
    if (!call) return;
    call.participants.delete(userId);
    io.to(`group:${groupId}`).emit("group:call:leave", { groupId, userId });
    if (call.participants.size === 1) {
      if (call.soloTimerId) clearTimeout(call.soloTimerId);
      call.soloTimerId = setTimeout(() => {
        const current = groupCalls.get(groupId);
        if (!current) return;
        if (current.participants.size !== 1) return;
        const caller = getOne("SELECT username FROM users WHERE id = ?", [
          current.initiatorId,
        ]);
        if (current.answered) {
          emitSystemGroupMessage(
            groupId,
            current.initiatorId,
            `@${caller?.username || "user"} started a call that lasted ${formatCallDuration(
              (Date.now() - current.startedAt) / 1000
            )}`,
            "call"
          );
        } else if (!current.missedNotified) {
          emitSystemGroupMessage(
            groupId,
            current.initiatorId,
            `Missed call from @${caller?.username || "user"}`,
            "call"
          );
        }
        groupCalls.delete(groupId);
        io.to(`group:${groupId}`).emit("group:call:ended", { groupId });
      }, 3 * 60 * 1000);
    } else if (call.soloTimerId) {
      clearTimeout(call.soloTimerId);
      call.soloTimerId = null;
    }
    if (call.participants.size === 0) {
      if (call.ringTimerId) {
        clearTimeout(call.ringTimerId);
      }
      if (call.soloTimerId) {
        clearTimeout(call.soloTimerId);
      }
      const caller = getOne("SELECT username FROM users WHERE id = ?", [
        call.initiatorId,
      ]);
      if (call.answered) {
        emitSystemGroupMessage(
          groupId,
          call.initiatorId,
          `@${caller?.username || "user"} started a call that lasted ${formatCallDuration(
            (Date.now() - call.startedAt) / 1000
          )}`,
          "call"
        );
      } else if (!call.missedNotified) {
        emitSystemGroupMessage(
          groupId,
          call.initiatorId,
          `Missed call from @${caller?.username || "user"}`,
          "call"
        );
      }
      groupCalls.delete(groupId);
      io.to(`group:${groupId}`).emit("group:call:ended", { groupId });
    }
  });

  socket.on("group:call:offer", (payload) => {
    const { groupId, toId, offer } = payload || {};
    if (!groupId || !toId || !offer) return;
    io.to(`user:${toId}`).emit("group:call:offer", {
      groupId,
      fromId: userId,
      offer,
    });
  });

  socket.on("group:call:answer", (payload) => {
    const { groupId, toId, answer } = payload || {};
    if (!groupId || !toId || !answer) return;
    io.to(`user:${toId}`).emit("group:call:answer", {
      groupId,
      fromId: userId,
      answer,
    });
  });

  socket.on("group:call:ice", (payload) => {
    const { groupId, toId, candidate } = payload || {};
    if (!groupId || !toId || !candidate) return;
    io.to(`user:${toId}`).emit("group:call:ice", {
      groupId,
      fromId: userId,
      candidate,
    });
  });

  socket.join(`user:${userId}`);

  socket.on("disconnect", () => {
    const lastSeen = new Date().toISOString();
    onlineUsers.set(userId, "offline");
    const otherId = activeCalls.get(userId);
    if (otherId) {
      endCallSession(userId, otherId);
    }
    const pendingEntries = Array.from(pendingCalls.entries()).filter(
      ([, pending]) =>
        pending.callerId === userId || pending.calleeId === userId
    );
    pendingEntries.forEach(([key, pending]) => {
      clearTimeout(pending.timerId);
      pendingCalls.delete(key);
      pendingUsers.delete(pending.callerId);
      pendingUsers.delete(pending.calleeId);
      const other =
        pending.callerId === userId ? pending.calleeId : pending.callerId;
      io.to(`user:${other}`).emit("call:decline", { fromId: userId });
    });
    groupCalls.forEach((call, groupId) => {
      if (call.participants.has(userId)) {
        call.participants.delete(userId);
        io.to(`group:${groupId}`).emit("group:call:leave", { groupId, userId });
        if (call.participants.size === 0) {
          if (call.ringTimerId) {
            clearTimeout(call.ringTimerId);
          }
          const caller = getOne("SELECT username FROM users WHERE id = ?", [
            call.initiatorId,
          ]);
          if (call.answered) {
            emitSystemGroupMessage(
              groupId,
              call.initiatorId,
              `@${caller?.username || "user"} started a call that lasted ${formatCallDuration(
                (Date.now() - call.startedAt) / 1000
              )}`,
              "call"
            );
          } else if (!call.missedNotified) {
            emitSystemGroupMessage(
              groupId,
              call.initiatorId,
              `Missed call from @${caller?.username || "user"}`,
              "call"
            );
          }
          groupCalls.delete(groupId);
          io.to(`group:${groupId}`).emit("group:call:ended", { groupId });
        }
      }
    });
    run("UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", [userId]);
    io.emit("presence:update", { userId, status: "offline", last_seen: lastSeen });
  });
});

app.use("/uploads", express.static(path.join(__dirname, "public", "uploads")));
app.use(express.static(path.join(__dirname, "public")));

const port = process.env.PORT || 3001;
async function main() {
  await initDb();
  server.listen(port, "0.0.0.0", () => {
    console.log(`Server listening on port ${port}`);
  });
}

main();
