import React, { useEffect, useMemo, useRef, useState } from "react";
import { io } from "socket.io-client";
import { ethers } from "ethers";
import addIcon from "./assets/add.svg";
import bellIcon from "./assets/bell.svg";
import pendingIcon from "./assets/pending.svg";
import removeIcon from "./assets/remove.svg";
import discordIcon from "./assets/connections/discord.png";
import githubIcon from "./assets/connections/github.png";
import instagramIcon from "./assets/connections/instagram.png";
import spotifyIcon from "./assets/connections/spotify.png";
import telegramIcon from "./assets/connections/telegram.png";
import tiktokIcon from "./assets/connections/tiktok.png";
import twitchIcon from "./assets/connections/twitch.png";
import websiteIcon from "./assets/connections/website.png";
import xIcon from "./assets/connections/x.png";
import youtubeIcon from "./assets/connections/youtube.png";

const API_URL = import.meta.env.VITE_API_URL || window.location.origin || "http://localhost:3001";
const CHAIN_MODE = import.meta.env.VITE_CHAIN_MODE || "mock";
const WALLETCONNECT_PROJECT_ID = import.meta.env.VITE_WALLETCONNECT_PROJECT_ID || "";
const GAME_LOOP_URL = `${API_URL}/bet.mp3`;
const COINFLIP_SFX_URL = `${API_URL}/coinflip.mp3`;
const BLACKJACK_FACTORY_ABI = [
  "function deposit(bytes32 matchId) external",
  "function claim(bytes32 matchId,address recipient) external",
  "function getMatch(bytes32 matchId) external view returns (address token,uint256 wager,uint64 depositDeadline,address winner,bool finalized,bool settled,address[] memory players,uint256[] memory deposits)",
];
const MINI_GAME_FACTORY_ABI = [
  "function deposit(bytes32 matchId,uint256 amount) external",
  "function claim(bytes32 matchId,address recipient) external",
  "function getMatch(bytes32 matchId) external view returns (address token,uint256 fixedWager,uint64 depositDeadline,bool variableDeposit,address winner,bool finalized,bool settled,address[] memory players,uint256[] memory deposits,uint256 totalPot)",
];
const ERC20_ABI = [
  "function approve(address spender,uint256 amount) external returns (bool)",
  "function allowance(address owner,address spender) external view returns (uint256)",
  "function decimals() view returns (uint8)",
];
const BLACKJACK_CHAIN_LABELS = {
  base: "Base",
  arbitrum: "Arbitrum",
  polygon: "Polygon",
};
const BLACKJACK_CHAIN_PARAMS = {
  base: {
    chainId: 8453,
    hexChainId: "0x2105",
    chainName: "Base",
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: ["https://mainnet.base.org"],
    blockExplorerUrls: ["https://basescan.org"],
  },
  arbitrum: {
    chainId: 42161,
    hexChainId: "0xa4b1",
    chainName: "Arbitrum One",
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: ["https://arb1.arbitrum.io/rpc"],
    blockExplorerUrls: ["https://arbiscan.io"],
  },
  polygon: {
    chainId: 137,
    hexChainId: "0x89",
    chainName: "Polygon",
    nativeCurrency: { name: "POL", symbol: "POL", decimals: 18 },
    rpcUrls: ["https://polygon-rpc.com"],
    blockExplorerUrls: ["https://polygonscan.com"],
  },
};
const STORY_IMAGE_MS = 3500;
const STORY_VIDEO_MS = 4500;
const MAX_CUSTOM_STATUS = 13;
const MAX_BIO = 190;
const USERNAME_MIN = 3;
const USERNAME_MAX = 14;

const STATUS_OPTIONS = [
  { value: "online", label: "Online", color: "#2a9d3f" },
  { value: "away", label: "Idle", color: "#d19a00" },
  { value: "busy", label: "Busy", color: "#c2272d" },
  { value: "invisible", label: "Offline", color: "#6a6f78" },
];

const BLACKJACK_TOKENS = ["USDC", "USDT", "DAI"];
const BLACKJACK_CHAINS = [
  { id: "base", label: "Base" },
  { id: "arbitrum", label: "Arbitrum" },
  { id: "polygon", label: "Polygon" },
];

function formatGameTokenLabel(token) {
  if (!token) return "";
  if (token === "USDC") return "USDC stablecoin";
  if (token === "USDT") return "USDT stablecoin";
  if (token === "DAI") return "DAI stablecoin";
  return token;
}

function formatGameChainLabel(chain) {
  return BLACKJACK_CHAIN_LABELS[chain] || chain;
}

const EMOJI_LIST = [":)", ":(", ":D", ";)", "<3", ":O", ":P", ":3", "^_^", ">_<", "(y)", "(n)"];
const REACTION_EMOJIS = ["<3", ":)", ":D", ":O", ";)", "T_T", "^_^", ":P", "(y)"];

const CONNECTION_SERVICES = [
  { id: "instagram", label: "Instagram", icon: instagramIcon },
  { id: "discord", label: "Discord", icon: discordIcon },
  { id: "x", label: "X", icon: xIcon },
  { id: "github", label: "GitHub", icon: githubIcon },
  { id: "youtube", label: "YouTube", icon: youtubeIcon },
  { id: "twitch", label: "Twitch", icon: twitchIcon },
  { id: "spotify", label: "Spotify", icon: spotifyIcon },
  { id: "telegram", label: "Telegram", icon: telegramIcon },
  { id: "tiktok", label: "TikTok", icon: tiktokIcon },
  { id: "website", label: "Site", icon: websiteIcon },
];

const STUN_CONFIG = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
};

const defaultAvatar = (name) => {
  const initial = (name || "U")[0].toUpperCase();
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">
      <rect width="100%" height="100%" fill="#1f5aa6" />
      <text x="50%" y="54%" font-size="28" fill="#ffffff" font-family="Tahoma" text-anchor="middle">${initial}</text>
    </svg>
  `;
  return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
};

const avatarUrl = (avatar, fallbackName) => {
  if (!avatar || avatar === "null" || avatar === "undefined") return defaultAvatar(fallbackName);
  if (avatar.startsWith("http") || avatar.startsWith("data:") || avatar.startsWith("blob:")) return avatar;
  return resolveMediaUrl(avatar);
};

const resolveUserById = (id, { user, friends, friendsAll, manualDmUsers, allUsers }) => {
  if (!id) return null;
  if (user?.id === id) return user;
  return (
    friendsAll?.find((u) => u.id === id) ||
    friends?.find((u) => u.id === id) ||
    manualDmUsers?.find((u) => u.id === id) ||
    allUsers?.find((u) => u.id === id) ||
    null
  );
};

const onAvatarError = (event, fallbackName) => {
  if (!event?.currentTarget) return;
  event.currentTarget.onerror = null;
  event.currentTarget.src = defaultAvatar(fallbackName);
};

const defaultGroupAvatar = () => {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">
      <rect width="100%" height="100%" fill="#1f5aa6" />
      <circle cx="24" cy="28" r="8" fill="#ffffff" />
      <circle cx="40" cy="28" r="8" fill="#ffffff" opacity="0.9" />
      <rect x="16" y="38" width="32" height="12" rx="6" fill="#ffffff" opacity="0.95" />
    </svg>
  `;
  return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
};

let csrfToken = "";
function setCsrfToken(token) {
  csrfToken = token || "";
}

async function apiFetch(path, options = {}) {
  const res = await fetch(`${API_URL}${path}`, {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
    },
    ...options,
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.error || "Request failed");
  }
  return res.json();
}

async function apiUpload(path, formData) {
  const res = await fetch(`${API_URL}${path}`, {
    method: "POST",
    credentials: "include",
    headers: {
      ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
    },
    body: formData,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || "Upload failed");
  }
  return data;
}

function resolveMediaUrl(url) {
  if (!url) return "";
  if (url.startsWith("http")) return url;
  if (url.startsWith("data:") || url.startsWith("blob:")) return url;
  return `${API_URL}${url}`;
}

let walletConnectInstance = null;

function listInjectedWallets() {
  if (typeof window === "undefined") return [];
  const providers = [];
  const seen = new Set();
  const pushProvider = (provider, id, label) => {
    if (!provider || seen.has(id)) return;
    seen.add(id);
    providers.push({ id, label, type: "injected", provider });
  };
  const root = window.ethereum;
  const providerList = Array.isArray(root?.providers) ? root.providers : root ? [root] : [];
  providerList.forEach((provider) => {
    if (provider?.isMetaMask) pushProvider(provider, "metamask", "MetaMask");
    if (provider?.isRabby) pushProvider(provider, "rabby", "Rabby");
    if (provider?.isCoinbaseWallet) pushProvider(provider, "coinbase", "Coinbase Wallet");
    if (provider?.isExodus) pushProvider(provider, "exodus", "Exodus");
    if (provider?.isPhantom) pushProvider(provider, "phantom-evm", "Phantom");
    if (provider?.isTrust || provider?.isTrustWallet) pushProvider(provider, "trust", "Trust Wallet");
  });
  if (window.phantom?.ethereum) {
    pushProvider(window.phantom.ethereum, "phantom-evm", "Phantom");
  }
  if (root && providers.length === 0) {
    pushProvider(root, "browser-wallet", "Browser Wallet");
  }
  return providers;
}

async function getWalletConnectProvider(chain) {
  if (!WALLETCONNECT_PROJECT_ID) {
    throw new Error("WalletConnect is not configured. Set VITE_WALLETCONNECT_PROJECT_ID.");
  }
  const target = BLACKJACK_CHAIN_PARAMS[chain];
  if (!target) {
    throw new Error(`Unsupported chain ${chain}`);
  }
  if (!walletConnectInstance) {
    const { default: EthereumProvider } = await import("@walletconnect/ethereum-provider");
    walletConnectInstance = await EthereumProvider.init({
      projectId: WALLETCONNECT_PROJECT_ID,
      showQrModal: true,
      optionalChains: Object.values(BLACKJACK_CHAIN_PARAMS).map((cfg) => cfg.chainId),
      methods: [
        "eth_sendTransaction",
        "eth_signTransaction",
        "personal_sign",
        "eth_sign",
        "eth_signTypedData",
        "eth_signTypedData_v4",
      ],
      rpcMap: Object.fromEntries(
        Object.values(BLACKJACK_CHAIN_PARAMS).map((cfg) => [cfg.chainId, cfg.rpcUrls[0]])
      ),
    });
  }
  await walletConnectInstance.connect({
    optionalChains: [target.chainId],
  });
  return walletConnectInstance;
}

async function ensureWalletChain(rawProvider, chain) {
  const target = BLACKJACK_CHAIN_PARAMS[chain];
  if (!target) {
    throw new Error(`Unsupported chain ${chain}`);
  }
  const provider = new ethers.BrowserProvider(rawProvider);
  const network = await provider.getNetwork();
  if (Number(network.chainId) === target.chainId) {
    return provider;
  }
  try {
    await rawProvider.request({
      method: "wallet_switchEthereumChain",
      params: [{ chainId: target.hexChainId }],
    });
  } catch (err) {
    if (err?.code === 4902) {
      await rawProvider.request({
        method: "wallet_addEthereumChain",
        params: [
          {
            chainId: target.hexChainId,
            chainName: target.chainName,
            nativeCurrency: target.nativeCurrency,
            rpcUrls: target.rpcUrls,
            blockExplorerUrls: target.blockExplorerUrls,
          },
        ],
      });
    } else {
      throw err;
    }
  }
  return new ethers.BrowserProvider(rawProvider);
}

async function getWalletContext(walletOption, chain, requestAccounts = false) {
  if (!walletOption) {
    throw new Error("Choose a wallet first.");
  }
  const rawProvider =
    walletOption.type === "walletconnect"
      ? await getWalletConnectProvider(chain)
      : walletOption.provider;
  if (!rawProvider) {
    throw new Error("Wallet provider is unavailable.");
  }
  const provider = await ensureWalletChain(rawProvider, chain);
  if (requestAccounts) {
    await provider.send("eth_requestAccounts", []);
  }
  const signer = await provider.getSigner();
  const address = await signer.getAddress();
  const network = await provider.getNetwork();
  return {
    provider,
    signer,
    address,
    chainId: Number(network.chainId),
    walletId: walletOption.id,
    walletLabel: walletOption.label,
  };
}

function dataUrlToBlob(dataUrl) {
  if (!dataUrl?.startsWith("data:")) return null;
  const [meta, data] = dataUrl.split(",");
  if (!meta || !data) return null;
  const mimeMatch = meta.match(/data:([^;]+);/);
  const mime = mimeMatch ? mimeMatch[1] : "image/png";
  const binary = atob(data);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return new Blob([bytes], { type: mime });
}

function copyText(value) {
  if (!value) return Promise.reject(new Error("Nothing to copy"));
  if (navigator?.clipboard?.writeText) {
    return navigator.clipboard.writeText(value);
  }
  const input = document.createElement("textarea");
  input.value = value;
  input.setAttribute("readonly", "");
  input.style.position = "absolute";
  input.style.left = "-9999px";
  document.body.appendChild(input);
  input.select();
  document.execCommand("copy");
  document.body.removeChild(input);
  return Promise.resolve();
}

function getAudioStatusIcon(type) {
  if (type === "deafened") {
    return (
      <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
        <path d="M4 13a8 8 0 0 1 16 0" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        <path d="M6 13v3a2 2 0 0 0 2 2h1v-5H8a2 2 0 0 0-2 2z" fill="none" stroke="currentColor" strokeWidth="2" />
        <path d="M18 13v3a2 2 0 0 1-2 2h-1v-5h1a2 2 0 0 1 2 2z" fill="none" stroke="currentColor" strokeWidth="2" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
      <path d="M12 3a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V6a3 3 0 0 0-3-3z" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M5 11a7 7 0 0 0 14 0" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M12 18v3" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M4 4l16 16" fill="none" stroke="currentColor" strokeWidth="2" />
    </svg>
  );
}

function parseNotificationPayload(notification) {
  if (!notification?.payload) return null;
  if (typeof notification.payload === "object") return notification.payload;
  try {
    return JSON.parse(notification.payload);
  } catch {
    return null;
  }
}

function isGameInviteExpired(notification, now = Date.now()) {
  const payload = parseNotificationPayload(notification);
  const expiresAt = payload?.expiresAt ? new Date(payload.expiresAt).getTime() : NaN;
  return Number.isFinite(expiresAt) && expiresAt <= now;
}

function StreamVideo({ stream, muted = true, className = "" }) {
  const videoRef = useRef(null);

  useEffect(() => {
    const node = videoRef.current;
    if (!node) return;
    if (node.srcObject !== stream) {
      node.srcObject = stream || null;
    }
    if (stream) {
      node.play().catch(() => {});
    }
    return () => {
      if (node.srcObject === stream) {
        node.srcObject = null;
      }
    };
  }, [stream]);

  return <video ref={videoRef} className={className} autoPlay muted={muted} playsInline />;
}

function chatKey(type, id) {
  return `${type}:${id}`;
}

function getLastMessagePreview(message, selfId, otherLabel, isGroup = false) {
  if (!message) return "";
  const isSelf = selfId && message.sender_id === selfId;
  const label = otherLabel ? `${otherLabel}` : "";
  if (message.is_system) {
    const body = (message.body || "").trim();
    if (!body) return "";
    if (isSelf) {
      return body.replace(/^@[^\s]+\s/, "You ");
    }
    return body;
  }
  const prefix = isSelf ? "You: " : label ? `${label}: ` : isGroup ? `${label}: ` : "";
  if (message.type === "image" || message.image_url) return `${prefix}Photo`;
  if (message.type === "audio" || message.audio_url) return `${prefix}Voice message`;
  const text = (message.body || "").trim();
  if (!text) return "";
  const max = 18;
  const clipped = text.length >= max ? `${text.slice(0, max)}..` : text;
  return `${prefix}${clipped}`;
}

function formatTime(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatDateShort(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString([], { year: "numeric", month: "short", day: "numeric" });
}

function formatDateTime(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatDuration(seconds) {
  const sec = Math.max(0, Math.floor(seconds));
  const mins = Math.floor(sec / 60);
  const hrs = Math.floor(mins / 60);
  const remM = mins % 60;
  const remS = sec % 60;
  if (hrs > 0) return `${hrs}h ${remM}m`;
  if (mins > 0) return `${mins}m ${remS}s`;
  return `${remS}s`;
}

function timeAgo(ts) {
  if (!ts) return "";
  const d = new Date(ts.endsWith("Z") ? ts : `${ts}Z`);
  if (Number.isNaN(d.getTime())) return "";
  const diff = Math.max(0, Date.now() - d.getTime());
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return `${sec} seconds ago`;
  const mins = Math.floor(sec / 60);
  if (mins < 60) return `${mins} minutes ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs} hours ago`;
  const days = Math.floor(hrs / 24);
  return `${days} days ago`;
}

function extractMentions(text) {
  if (!text) return [];
  const regex = /@([a-zA-Z0-9_]{2,20})/g;
  const result = [];
  let match;
  while ((match = regex.exec(text))) {
    result.push(match[1]);
  }
  return result;
}

function getStatusLabel(value) {
  const found = STATUS_OPTIONS.find((s) => s.value === value);
  return found ? found.label : "Online";
}

function getStatusColor(value) {
  const found = STATUS_OPTIONS.find((s) => s.value === value);
  return found ? found.color : "#2a9d3f";
}

function isImageType(type) {
  return type === "image";
}

function isAudioType(type) {
  return type === "audio";
}

function getDisplayName(user) {
  if (!user) return "";
  return (user.display_name || "").trim() || user.username;
}

function parseHexColor(hex) {
  if (!hex) return null;
  const clean = hex.replace("#", "").trim();
  if (clean.length === 3) {
    const r = parseInt(clean[0] + clean[0], 16);
    const g = parseInt(clean[1] + clean[1], 16);
    const b = parseInt(clean[2] + clean[2], 16);
    return { r, g, b };
  }
  if (clean.length === 6) {
    const r = parseInt(clean.slice(0, 2), 16);
    const g = parseInt(clean.slice(2, 4), 16);
    const b = parseInt(clean.slice(4, 6), 16);
    return { r, g, b };
  }
  return null;
}

function overlayFromRgb(rgb) {
  if (!rgb) return "rgba(255, 255, 255, 0.65)";
  const lum = (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255;
  const alpha = lum > 0.7 ? 0.75 : lum > 0.5 ? 0.6 : 0.45;
  return `rgba(255, 255, 255, ${alpha})`;
}

function getDmOverlay(bg) {
  if (!bg) return "rgba(255, 255, 255, 0.65)";
  if (bg.overlay) return bg.overlay;
  if (bg.type === "color") {
    return overlayFromRgb(parseHexColor(bg.value));
  }
  return "rgba(255, 255, 255, 0.65)";
}

function mergeMessages(prev, message) {
  if (!message) return prev;
  const existing = prev.find((m) => m.id === message.id);
  if (existing) {
    return prev.map((m) => (m.id === message.id ? { ...m, ...message } : m));
  }
  return [...prev, message];
}

function matchPending(prev, message) {
  if (!message) return -1;
  return prev.findIndex(
    (m) =>
      m.pending &&
      m.type === message.type &&
      (m.body || "") === (message.body || "") &&
      (m.image_url || "") === (message.image_url || "") &&
      (m.audio_url || "") === (message.audio_url || "")
  );
}

function getMessagePreview(msg) {
  if (!msg) return "";
  if (msg.type === "image") return "Photo";
  if (msg.type === "audio") return "Voice message";
  const text = (msg.body || "").trim();
  if (!text) return "";
  const max = 18;
  return text.length >= max ? `${text.slice(0, max)}..` : text;
}

function getSeenLabel(msg, friend, currentUserId) {
  if (!msg || !friend) return "";
  if (!currentUserId) return "";
  if (msg.sender_id !== currentUserId) return "";
  if (!friend.last_read_at) return "";
  const readAt = new Date(friend.last_read_at).getTime();
  const sentAt = new Date(msg.created_at).getTime();
  if (Number.isNaN(readAt) || Number.isNaN(sentAt)) return "";
  return readAt >= sentAt ? "Seen" : "";
}

function safeMessageText(message) {
  if (!message) return "";
  if (message.type === "image") return "[Photo]";
  if (message.type === "audio") return "[Voice message]";
  return message.body || "";
}

function muteKeyForChat(chat) {
  if (!chat) return "";
  return chatKey(chat.type, chat.id);
}

// (audio helpers are defined inside App so they can access state)

export default function App() {
  const [user, setUser] = useState(null);
  const [authMode, setAuthMode] = useState("login");
  const [authError, setAuthError] = useState("");
  const [form, setForm] = useState({ username: "", password: "", email: "" });
  const [authLoading, setAuthLoading] = useState(false);
  const [logoutLoading, setLogoutLoading] = useState(false);
  const [view, setView] = useState("chat");
  const [appMinimized, setAppMinimized] = useState(false);
  const [appMaximized, setAppMaximized] = useState(false);

  const [friends, setFriends] = useState([]);
  const [friendsAll, setFriendsAll] = useState([]);
  const [groups, setGroups] = useState([]);
  const [allUsers, setAllUsers] = useState([]);
  const [customStatusById, setCustomStatusById] = useState({});
  const [selectedChat, setSelectedChat] = useState(null);
  const [manualDmUsers, setManualDmUsers] = useState([]);
  const manualDmUsersRef = useRef([]);
  const [mutedChats, setMutedChats] = useState({});
  const [nicknameMap, setNicknameMap] = useState({});
  const [nicknameModal, setNicknameModal] = useState(null);
  const [friendMuteMenu, setFriendMuteMenu] = useState(null);
  const [groupMuteMenu, setGroupMuteMenu] = useState(null);
  const [messagesByChat, setMessagesByChat] = useState({});
  const [typingByChat, setTypingByChat] = useState({});
  const [messageInput, setMessageInput] = useState("");
  const [mentionMenu, setMentionMenu] = useState(null);
  const [mentionIndex, setMentionIndex] = useState(0);
  const [messageError, setMessageError] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [sentPulseChat, setSentPulseChat] = useState(null);
  const sentPulseTimerRef = useRef(null);
  const [unreadChats, setUnreadChats] = useState({});
  const mobileTouchRef = useRef({ x: 0, y: 0, active: false });
  const longPressTimerRef = useRef(null);
  const longPressStartRef = useRef({ x: 0, y: 0, active: false });
  const longPressTriggeredRef = useRef(false);

  const [friendSearch, setFriendSearch] = useState("");
  const [friendError, setFriendError] = useState("");
  const [friendSuccess, setFriendSuccess] = useState("");
  const [addFriendOpen, setAddFriendOpen] = useState(false);
  const [notifications, setNotifications] = useState([]);
  const [showNotifications, setShowNotifications] = useState(false);
  const [showChatProfile, setShowChatProfile] = useState(false);
  const [showMutualGroups, setShowMutualGroups] = useState(false);
  const [showMutualFriends, setShowMutualFriends] = useState(false);
  const [chatProfileConnections, setChatProfileConnections] = useState([]);
  const [requests, setRequests] = useState([]);

  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);

  const [settings, setSettings] = useState({
    username: "",
    displayName: "",
    email: "",
    emailVerified: false,
    avatar: "",
    status: "online",
    customStatus: "",
    bio: "",
    ringtoneUrl: "",
    songTitle: "",
    songArtist: "",
    songAlbum: "",
    songCoverUrl: "",
    songAudioUrl: "",
    songSource: "",
    songSourceUrl: "",
    aliases: [],
  });
  const [musicDraft, setMusicDraft] = useState({
    source: "spotify",
    sourceUrl: "",
    title: "",
    artist: "",
    album: "",
    coverUrl: "",
    audioUrl: "",
  });
  const [musicSaving, setMusicSaving] = useState(false);
  const [musicError, setMusicError] = useState("");
  const [blackjackPanelOpen, setBlackjackPanelOpen] = useState(false);
  const [blackjackMatch, setBlackjackMatch] = useState(null);
  const [blackjackInvite, setBlackjackInvite] = useState({
    wager: "10",
    token: "USDC",
    chain: "base",
    targetId: "",
  });
  const [blackjackError, setBlackjackError] = useState("");
  const [blackjackActionBusy, setBlackjackActionBusy] = useState(false);
  const [blackjackUiMessage, setBlackjackUiMessage] = useState("");
  const [blackjackClaimOpen, setBlackjackClaimOpen] = useState(false);
  const [blackjackClaimAddress, setBlackjackClaimAddress] = useState("");
  const [blackjackWallet, setBlackjackWallet] = useState("");
  const [blackjackWalletProviderId, setBlackjackWalletProviderId] = useState("");
  const [walletPickerOpen, setWalletPickerOpen] = useState(false);
  const [walletPickerMode, setWalletPickerMode] = useState("connect");
  const [miniGameHubOpen, setMiniGameHubOpen] = useState(false);
  const [miniGameMatch, setMiniGameMatch] = useState(null);
  const [miniGameInvite, setMiniGameInvite] = useState({
    gameType: "coinflip",
    wager: "10",
    token: "USDC",
    chain: "base",
    targetId: "",
  });
  const [miniGameError, setMiniGameError] = useState("");
  const [miniGameBusy, setMiniGameBusy] = useState(false);
  const [miniGameDepositAmount, setMiniGameDepositAmount] = useState("10");
  const [miniGameClaimOpen, setMiniGameClaimOpen] = useState(false);
  const [miniGameClaimAddress, setMiniGameClaimAddress] = useState("");
  const [miniGameUiMessage, setMiniGameUiMessage] = useState("");

  const [theme, setTheme] = useState("classic");
  const [customThemeMedia, setCustomThemeMedia] = useState(null);
  const [customThemeTint, setCustomThemeTint] = useState(null);

  const [statusMenuOpen, setStatusMenuOpen] = useState(false);
  const [statusEditOpen, setStatusEditOpen] = useState(false);
  const [customStatusInput, setCustomStatusInput] = useState("");
  const [statusSaving, setStatusSaving] = useState(false);
  const [customStatusAnim, setCustomStatusAnim] = useState("");
  const idleTimerRef = useRef(null);
  const autoIdleRef = useRef(false);
  const statusRef = useRef("online");
  const [closingModals, setClosingModals] = useState({});

  const [displayEditOpen, setDisplayEditOpen] = useState(false);
  const [emailEditOpen, setEmailEditOpen] = useState(false);
  const [emailPassword, setEmailPassword] = useState("");
  const [passwordModalOpen, setPasswordModalOpen] = useState(false);
  const [usernameModalOpen, setUsernameModalOpen] = useState(false);

  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newUsername, setNewUsername] = useState("");
  const [usernamePassword, setUsernamePassword] = useState("");
  const [passwordError, setPasswordError] = useState("");

  const [aboutInput, setAboutInput] = useState("");
  const [showBioEmoji, setShowBioEmoji] = useState(false);
  const [audioMeta, setAudioMeta] = useState({});
  const [audioPreview, setAudioPreview] = useState(null);
  const [imagePreview, setImagePreview] = useState(null);
  const [isRecording, setIsRecording] = useState(false);
  const [recordElapsed, setRecordElapsed] = useState(0);
  const [recordSlideOffset, setRecordSlideOffset] = useState(0);
  const recordTimerRef = useRef(null);
  const recordCancelRef = useRef(false);
  const recordSlideRef = useRef({ active: false, startX: 0 });

  const [connectionModalOpen, setConnectionModalOpen] = useState(false);
  const [connectionService, setConnectionService] = useState(null);
  const [connectionHandle, setConnectionHandle] = useState("");
  const [connectionUrl, setConnectionUrl] = useState("");
  const [connectionError, setConnectionError] = useState("");
  const [connections, setConnections] = useState([]);
  const [removeConnectionId, setRemoveConnectionId] = useState(null);

  const [usernameManagerOpen, setUsernameManagerOpen] = useState(false);
  const [aliasData, setAliasData] = useState(null);
  const [aliasCheck, setAliasCheck] = useState({ username: "", available: null, error: "" });
  const [aliasPassword, setAliasPassword] = useState("");
  const [aliasClaiming, setAliasClaiming] = useState(false);
  const [aliasMenuOpen, setAliasMenuOpen] = useState(null);
  const [primaryModal, setPrimaryModal] = useState(null);
  const [primaryPassword, setPrimaryPassword] = useState("");
  const [primaryError, setPrimaryError] = useState("");
  const [primarySwitching, setPrimarySwitching] = useState(false);
  const [transferModal, setTransferModal] = useState(null);
  const [transferError, setTransferError] = useState("");
  const [removeAliasModal, setRemoveAliasModal] = useState(null);
  const [aliasTransfering, setAliasTransfering] = useState(false);
  const [aliasRemoving, setAliasRemoving] = useState(false);

  const [profileUser, setProfileUser] = useState(null);
  const [profileNote, setProfileNote] = useState("");
  const [profileConnections, setProfileConnections] = useState([]);
  const [profileOpen, setProfileOpen] = useState(false);

  const [showMessageMenu, setShowMessageMenu] = useState(null);
  const [showChatMenu, setShowChatMenu] = useState(false);
  const [reactionPickerFor, setReactionPickerFor] = useState(null);
  const [showProfileMenu, setShowProfileMenu] = useState(null);
  const [friendGameMenu, setFriendGameMenu] = useState(null);
  const [groupMemberMenu, setGroupMemberMenu] = useState(null);
  const [groupMembersOpen, setGroupMembersOpen] = useState(false);
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [selectMode, setSelectMode] = useState(false);
  const [removeFriendConfirm, setRemoveFriendConfirm] = useState(null);
  const [leaveGroupConfirm, setLeaveGroupConfirm] = useState(null);
  const [pendingFriendIds, setPendingFriendIds] = useState([]);
  const [removedFriendIds, setRemovedFriendIds] = useState([]);
  const [profileIsFriend, setProfileIsFriend] = useState(false);
  const [deletingMessageIds, setDeletingMessageIds] = useState({});
  const [deletingDmId, setDeletingDmId] = useState(null);
  const [selectedMessageIds, setSelectedMessageIds] = useState({});
  const [editingMessageId, setEditingMessageId] = useState(null);
  const [editingText, setEditingText] = useState("");
  const [bioSaving, setBioSaving] = useState(false);
  const [forwardModal, setForwardModal] = useState(null);
  const [forwardTarget, setForwardTarget] = useState(null);
  const [forwardNote, setForwardNote] = useState("");
  const [isMobile, setIsMobile] = useState(() => window.innerWidth <= 720);
  const [mobileView, setMobileView] = useState("list");
  const [savingFlags, setSavingFlags] = useState({});
  const [avatarEditOpen, setAvatarEditOpen] = useState(false);
  const [avatarEditSrc, setAvatarEditSrc] = useState("");
  const [avatarEditSize, setAvatarEditSize] = useState({ w: 0, h: 0, base: 1 });
  const [avatarZoom, setAvatarZoom] = useState(1);
  const [avatarRotate, setAvatarRotate] = useState(0);
  const [avatarOffset, setAvatarOffset] = useState({ x: 0, y: 0 });
  const [avatarApplying, setAvatarApplying] = useState(false);
  const [avatarEditTarget, setAvatarEditTarget] = useState({ type: "user", groupId: null });
  const avatarImgRef = useRef(null);
  const avatarDragRef = useRef({ x: 0, y: 0, startX: 0, startY: 0, pinchDist: 0, pinchZoom: 1 });
  const avatarPointersRef = useRef(new Map());

  const [dmBgModal, setDmBgModal] = useState(null);
  const [dmBgColor, setDmBgColor] = useState("#f7f7f7");
  const [dmBgGradient, setDmBgGradient] = useState("");
  const [dmBgMode, setDmBgMode] = useState("color");
  const [dmBgOpacity, setDmBgOpacity] = useState(0.6);
  const [dmBgFit, setDmBgFit] = useState("fill");
  const [dmBgPreset, setDmBgPreset] = useState("soft");
  const [dmBgDirection, setDmBgDirection] = useState("horizontal");
  const [dmBgPresetOpen, setDmBgPresetOpen] = useState(false);
  const [dmBgDirectionOpen, setDmBgDirectionOpen] = useState(false);
  const [dmBgFitOpen, setDmBgFitOpen] = useState(false);
  const [dmBgRecent, setDmBgRecent] = useState([]);
  const [dmBgImagePreview, setDmBgImagePreview] = useState("");
  const [stories, setStories] = useState([]);
  const [storyViewer, setStoryViewer] = useState({ open: false, userIndex: 0, storyIndex: 0 });
  const [storyPaused, setStoryPaused] = useState(false);
  const [storyMenuOpen, setStoryMenuOpen] = useState(false);
  const [storyClosing, setStoryClosing] = useState(false);
  const [storyDeleteConfirm, setStoryDeleteConfirm] = useState(false);
  const [storyViewersOpen, setStoryViewersOpen] = useState(false);
  const [storyViewers, setStoryViewers] = useState([]);
  const [groupPickerOpen, setGroupPickerOpen] = useState(false);
  const [groupNameInput, setGroupNameInput] = useState("");
  const [groupSelected, setGroupSelected] = useState({});
  const [groupRenameOpen, setGroupRenameOpen] = useState(false);
  const [groupRenameInput, setGroupRenameInput] = useState("");
  const [groupContextMenu, setGroupContextMenu] = useState(null);
  const [editGroupModal, setEditGroupModal] = useState(null);
  const [editGroupName, setEditGroupName] = useState("");
  const [editGroupAvatar, setEditGroupAvatar] = useState("");
  const [editGroupConfirmOpen, setEditGroupConfirmOpen] = useState(false);
  const [addMembersModal, setAddMembersModal] = useState(null);
  const [addMembersSelected, setAddMembersSelected] = useState({});
  const [callState, setCallState] = useState({ status: "idle", withUserId: null, startTime: null, muted: false, deafened: false });
  const callStateRef = useRef(callState);
  const [callAudioStates, setCallAudioStates] = useState({});
  const [joiningCall, setJoiningCall] = useState(false);
  const [incomingCall, setIncomingCall] = useState(null);
  const [callDuration, setCallDuration] = useState(0);
  const [groupCallDuration, setGroupCallDuration] = useState(0);
  const [callSettingsOpen, setCallSettingsOpen] = useState(false);
  const [voicePanelOpen, setVoicePanelOpen] = useState(false);
  const [callDevices, setCallDevices] = useState({ inputs: [], outputs: [] });
  const [callSettings, setCallSettings] = useState({ inputId: "", outputId: "", micVolume: 1, speakerVolume: 1 });
  const [callInputMenuOpen, setCallInputMenuOpen] = useState(false);
  const [callOutputMenuOpen, setCallOutputMenuOpen] = useState(false);
  const [callWindowPos, setCallWindowPos] = useState({ x: 0, y: 0 });
  const [callQuality, setCallQuality] = useState({ label: "Good", level: "good" });
  const [inputSensitivity, setInputSensitivity] = useState(0.5);
  const [echoCancel, setEchoCancel] = useState(true);
  const [voiceEffect, setVoiceEffect] = useState("none");
  const [pitchShift, setPitchShift] = useState(0);
  const [eqLow, setEqLow] = useState(0);
  const [eqMid, setEqMid] = useState(0);
  const [eqHigh, setEqHigh] = useState(0);
  const [ringtoneError, setRingtoneError] = useState("");
  const [speakingLocal, setSpeakingLocal] = useState(false);
  const [speakingRemote, setSpeakingRemote] = useState(false);
  const [remotePresent, setRemotePresent] = useState(false);
  const [groupSpeakingMap, setGroupSpeakingMap] = useState({});
  const [audioStreamTick, setAudioStreamTick] = useState(0);
  const [micTestOn, setMicTestOn] = useState(false);
  const [micTestLevel, setMicTestLevel] = useState(0);
  const micTestStreamRef = useRef(null);
  const micTestRafRef = useRef(null);
  const micTestAudioRef = useRef(null);
  const dmScreenVideoSenderRef = useRef(null);
  const dmScreenAudioSenderRef = useRef(null);
  const micGainNodeRef = useRef(null);
  const micGateGainRef = useRef(null);
  const micLowRef = useRef(null);
  const micMidRef = useRef(null);
  const micHighRef = useRef(null);
  const micEffectOscRef = useRef(null);
  const micPitchParamRef = useRef(null);
  const micPitchNodeRef = useRef(null);
  const micBuildSeqRef = useRef(0);
  const lastVoiceEffectRef = useRef("none");
  const lastPitchRef = useRef(0);
  const micAnalyserRef = useRef(null);
  const micGateRafRef = useRef(null);
  const ringCtxRef = useRef(null);
  const ringOscRef = useRef(null);
  const ringGainRef = useRef(null);
  const ringTimerRef = useRef(null);
  const ringtoneAudioRef = useRef(null);
  const profileSongAudioRef = useRef(null);
  const gameMusicAudioRef = useRef(null);
  const coinflipSfxAudioRef = useRef(null);
  const gameMusicFadeRef = useRef(null);
  const lastCoinflipSfxRef = useRef("");
  const blackjackSfxCtxRef = useRef(null);
  const prevBlackjackSnapshotRef = useRef(null);
  const [profileSongPlayer, setProfileSongPlayer] = useState({
    ownerId: null,
    title: "",
    artist: "",
    album: "",
    coverUrl: "",
    audioUrl: "",
    playing: false,
  });
  const profileSongPlayerRef = useRef(profileSongPlayer);
  const [pushToTalk, setPushToTalk] = useState(false);
  const [pttKeybind, setPttKeybind] = useState("");
  const [pttListening, setPttListening] = useState(false);
  const [pttActive, setPttActive] = useState(false);
  const [groupCall, setGroupCall] = useState({ groupId: null, status: "idle", participants: [], startedAt: null });
  const groupCallRef = useRef(groupCall);
  const [groupCallAudioStates, setGroupCallAudioStates] = useState({});
  const [incomingGroupCall, setIncomingGroupCall] = useState(null);
  const [groupShares, setGroupShares] = useState({});
  const [dmShareStream, setDmShareStream] = useState(null);
  const [groupCallVisible, setGroupCallVisible] = useState(true);
  const [screenShareWindow, setScreenShareWindow] = useState({ open: false, label: "" });
  const [screenShareError, setScreenShareError] = useState("");
  const [dmShareActive, setDmShareActive] = useState(false);
  const [gameAudioMuted, setGameAudioMuted] = useState(false);

  const socketRef = useRef(null);
  const typingTimeoutRef = useRef(null);
  const typingTimersRef = useRef({});
  const fileInputRef = useRef(null);
  const messageInputRef = useRef(null);
  const audioInputRef = useRef(null);
  const chatBodyRef = useRef(null);
  const recorderRef = useRef(null);
  const recordChunksRef = useRef([]);
  const recordStreamRef = useRef(null);
  const audioTickRef = useRef({});
  const lastStoryViewedRef = useRef(null);
  const callDragRef = useRef({ dragging: false, startX: 0, startY: 0, x: 0, y: 0, pendingX: 0, pendingY: 0 });
  const callDragRafRef = useRef(null);
  const callEndedRef = useRef(false);
  const callSoundCtxRef = useRef(null);

  function updateAudioMeta(id, patch) {
    setAudioMeta((prev) => ({
      ...prev,
      [id]: { ...(prev[id] || {}), ...patch },
    }));
  }

  function startAudioTick(id, el) {
    stopAudioTick(id);
    audioTickRef.current[id] = setInterval(() => {
      if (!el || el.paused) return;
      const duration = el.duration || 0;
      const current = el.currentTime || 0;
      updateAudioMeta(id, {
        duration,
        progress: duration ? current / duration : 0,
      });
    }, 200);
  }

  function stopAudioTick(id) {
    if (audioTickRef.current[id]) {
      clearInterval(audioTickRef.current[id]);
      delete audioTickRef.current[id];
    }
  }

  function startRingToneOscillator() {
    if (ringOscRef.current) {
      if (ringCtxRef.current?.state === "suspended") {
        ringCtxRef.current.resume().catch(() => {});
      }
      return;
    }
    const ctx = ringCtxRef.current || new (window.AudioContext || window.webkitAudioContext)();
    ringCtxRef.current = ctx;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    ringOscRef.current = osc;
    ringGainRef.current = gain;
    osc.type = "sine";
    osc.frequency.value = 440;
    gain.gain.value = 0.0001;
    osc.connect(gain).connect(ctx.destination);
    osc.start();
    ctx.resume().catch(() => {});
    const pulse = () => {
      const now = ctx.currentTime;
      gain.gain.cancelScheduledValues(now);
      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.2, now + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.22);
    };
    pulse();
    ringTimerRef.current = setInterval(pulse, 700);
  }

  function startRingTone() {
    if (settings.ringtoneUrl) {
      const ringtoneSrc = resolveMediaUrl(settings.ringtoneUrl);
      if (!ringtoneAudioRef.current || ringtoneAudioRef.current.src !== ringtoneSrc) {
        if (ringtoneAudioRef.current) {
          ringtoneAudioRef.current.pause();
        }
        const audio = new Audio(ringtoneSrc);
        audio.loop = true;
        audio.volume = 0.7;
        audio.preload = "auto";
        ringtoneAudioRef.current = audio;
      }
      ringtoneAudioRef.current.currentTime = 0;
      ringtoneAudioRef.current
        .play()
        .catch(() => {
          startRingToneOscillator();
        });
      return;
    }
    startRingToneOscillator();
  }

  function stopRingTone() {
    if (ringtoneAudioRef.current) {
      ringtoneAudioRef.current.pause();
      ringtoneAudioRef.current.currentTime = 0;
    }
    if (ringTimerRef.current) {
      clearInterval(ringTimerRef.current);
      ringTimerRef.current = null;
    }
    if (ringOscRef.current) {
      ringOscRef.current.stop();
      ringOscRef.current.disconnect();
      ringOscRef.current = null;
    }
    if (ringGainRef.current) {
      ringGainRef.current.disconnect();
      ringGainRef.current = null;
    }
    if (ringCtxRef.current) {
      ringCtxRef.current.close().catch(() => {});
      ringCtxRef.current = null;
    }
  }

  useEffect(() => {
    if (settings.ringtoneUrl) return;
    if (ringtoneAudioRef.current) {
      ringtoneAudioRef.current.pause();
      ringtoneAudioRef.current = null;
    }
  }, [settings.ringtoneUrl]);

  function fadeGameMusic(targetVolume, duration = 500) {
    const audio = gameMusicAudioRef.current;
    if (!audio) return;
    if (gameMusicFadeRef.current) {
      clearInterval(gameMusicFadeRef.current);
      gameMusicFadeRef.current = null;
    }
    const startVolume = Number(audio.volume || 0);
    const steps = Math.max(1, Math.floor(duration / 40));
    const delta = (targetVolume - startVolume) / steps;
    let step = 0;
    gameMusicFadeRef.current = setInterval(() => {
      step += 1;
      const next = step >= steps ? targetVolume : Math.min(1, Math.max(0, startVolume + delta * step));
      audio.volume = next;
      if (step >= steps) {
        clearInterval(gameMusicFadeRef.current);
        gameMusicFadeRef.current = null;
        if (targetVolume <= 0.001) {
          audio.pause();
        }
      }
    }, 40);
  }

  function playBlackjackTone({
    type = "sine",
    start = 520,
    end = 420,
    duration = 0.12,
    gain = 0.035,
    delay = 0,
  }) {
    if (gameAudioMuted) return;
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      const ctx = blackjackSfxCtxRef.current || new Ctx();
      blackjackSfxCtxRef.current = ctx;
      if (ctx.state === "suspended") {
        ctx.resume().catch(() => {});
      }
      const now = ctx.currentTime + delay;
      const osc = ctx.createOscillator();
      const amp = ctx.createGain();
      osc.type = type;
      osc.frequency.setValueAtTime(start, now);
      osc.frequency.exponentialRampToValueAtTime(Math.max(80, end), now + duration);
      amp.gain.setValueAtTime(0.0001, now);
      amp.gain.exponentialRampToValueAtTime(gain, now + 0.02);
      amp.gain.exponentialRampToValueAtTime(0.0001, now + duration);
      osc.connect(amp);
      amp.connect(ctx.destination);
      osc.start(now);
      osc.stop(now + duration + 0.02);
    } catch {
      // ignore audio failures
    }
  }

  function playBlackjackDealSound(count = 1) {
    for (let i = 0; i < count; i += 1) {
      playBlackjackTone({
        type: "triangle",
        start: 920,
        end: 650,
        duration: 0.08,
        gain: 0.02,
        delay: i * 0.05,
      });
    }
  }

  function extractProfileSong(profile) {
    if (!profile) return null;
    return {
      title: profile.song_title || "",
      artist: profile.song_artist || "",
      album: profile.song_album || "",
      coverUrl: profile.song_cover_url || "",
      audioUrl: profile.song_audio_url || "",
      source: profile.song_source || "",
      sourceUrl: profile.song_source_url || "",
      updatedAt: profile.song_updated_at || "",
    };
  }

  function applyProfileSongUpdate(userId, payload) {
    const nextSong = extractProfileSong(payload);
    if (!nextSong) return;
    setProfileSongPlayer((prev) => {
      if (prev.ownerId !== userId) return prev;
      if (!nextSong.audioUrl) {
        return { ...prev, ...nextSong, playing: false };
      }
      return { ...prev, ...nextSong };
    });
  }

  function toggleProfileSong(profile) {
    if (!profile?.id) return;
    const song = extractProfileSong(profile);
    if (!song?.audioUrl) return;
    setProfileSongPlayer((prev) => {
      if (prev.ownerId === profile.id) {
        return { ...prev, playing: !prev.playing };
      }
      return {
        ownerId: profile.id,
        title: song.title,
        artist: song.artist,
        album: song.album,
        coverUrl: song.coverUrl,
        audioUrl: song.audioUrl,
        playing: true,
      };
    });
  }

  function playCallChime(type = "join") {
    try {
      const ctx = callSoundCtxRef.current || new (window.AudioContext || window.webkitAudioContext)();
      callSoundCtxRef.current = ctx;
      ctx.resume().catch(() => {});
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = type === "leave" ? 330 : 520;
      gain.gain.value = 0.0001;
      osc.connect(gain).connect(ctx.destination);
      osc.start();
      const now = ctx.currentTime;
      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.12, now + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.25);
      osc.stop(now + 0.28);
    } catch {
      // ignore
    }
  }

  function toggleAudio(id) {
    const el = document.getElementById(`xp-audio-${id}`);
    if (!el) return;
    if (el.paused) {
      el.muted = false;
      el.volume = 1;
      el.play().catch(() => {
        try {
          el.load();
          setTimeout(() => el.play().catch(() => {}), 50);
        } catch {
          // ignore
        }
      });
      updateAudioMeta(id, { playing: true });
      startAudioTick(id, el);
    } else {
      el.pause();
      updateAudioMeta(id, { playing: false });
      stopAudioTick(id);
    }
  }
  const avatarInputRef = useRef(null);
  const storyInputRef = useRef(null);
  const storyVideoRef = useRef(null);
  const customThemeInputRef = useRef(null);
  const ringtoneInputRef = useRef(null);
  const pcRef = useRef(null);
  const localStreamRef = useRef(null);
  const remoteStreamRef = useRef(null);
  const rawMicStreamRef = useRef(null);
  const micGainCtxRef = useRef(null);
  const lastInputIdRef = useRef("");
  const lastEchoRef = useRef(true);
  const speakingTimersRef = useRef({ localOn: null, localOff: null, remoteOn: null, remoteOff: null });
  const groupPeersRef = useRef(new Map());
  const groupLocalStreamRef = useRef(null);
  const groupLocalScreenRef = useRef(null);
  const dmLocalScreenRef = useRef(null);

  useEffect(() => {
    callStateRef.current = callState;
  }, [callState]);

  useEffect(() => {
    groupCallRef.current = groupCall;
  }, [groupCall]);

  const themeKey = user ? `xp-theme:${user.id}` : "xp-theme";
  const dmBgKey = user ? `xp-dm-bg:${user.id}` : "xp-dm-bg";
  const customThemeKey = user ? `xp-theme-custom:${user.id}` : "xp-theme-custom";

  const selectedKey = selectedChat && chatKey(selectedChat.type, selectedChat.id);

  const messages = useMemo(() => {
    if (!selectedKey) return [];
    return messagesByChat[selectedKey] || [];
  }, [messagesByChat, selectedKey]);

  useEffect(() => {
    if (!selectedKey || !chatBodyRef.current || messages.length === 0) return;
    const last = messages[messages.length - 1];
    if (last?.sender_id === user?.id) {
      chatBodyRef.current.scrollTop = chatBodyRef.current.scrollHeight;
    }
  }, [messages, selectedKey, user?.id]);

  const selectedFriend = useMemo(() => {
    if (selectedChat?.type !== "dm") return null;
    return friends.find((f) => f.id === selectedChat.id) || null;
  }, [friends, selectedChat]);

  const selectedGroup = useMemo(() => {
    if (selectedChat?.type !== "group") return null;
    return groups.find((g) => g.id === selectedChat.id) || null;
  }, [groups, selectedChat]);

  const lastOutgoingId = useMemo(() => {
    if (!user?.id || !messages.length) return null;
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const m = messages[i];
      if (m && !m.is_system && m.sender_id === user.id) return m.id;
    }
    return null;
  }, [messages, user?.id]);

  const readAtRaw =
    selectedChat?.type === "dm"
      ? selectedFriend?.last_read_at
      : selectedChat?.type === "group"
      ? selectedGroup?.last_read_at
      : null;
  const readAtTs = readAtRaw ? new Date(readAtRaw).getTime() : null;
  const firstUnreadIndex =
    messages.length === 0
      ? -1
      : readAtTs
      ? messages.findIndex((m) => new Date(m.created_at).getTime() > readAtTs)
      : 0;

  const groupShareEntries = useMemo(() => {
    return Object.entries(groupShares || {}).map(([id, stream]) => {
      const member = selectedGroup?.members?.find((m) => m.id === Number(id));
      return { id: Number(id), stream, name: member ? `@${member.username}` : `@${id}` };
    });
  }, [groupShares, selectedGroup]);

  const dmShareEntry = useMemo(() => {
    if (!dmShareStream || !callState.withUserId) return null;
    const other =
      friendsAll.find((f) => f.id === callState.withUserId) ||
      friends.find((f) => f.id === callState.withUserId) ||
      manualDmUsers.find((f) => f.id === callState.withUserId) ||
      null;
    return {
      id: `dm-${callState.withUserId}`,
      stream: dmShareStream,
      name: other ? `@${other.username}` : "Screen Share",
    };
  }, [dmShareStream, callState.withUserId, friendsAll, friends, manualDmUsers]);

  const screenShareEntries = useMemo(() => {
    return dmShareEntry ? [...groupShareEntries, dmShareEntry] : groupShareEntries;
  }, [groupShareEntries, dmShareEntry]);

  const [dmBackgrounds, setDmBackgrounds] = useState({});

  useEffect(() => {
    try {
      setDmBackgrounds(JSON.parse(localStorage.getItem(dmBgKey) || "{}") || {});
    } catch {
      setDmBackgrounds({});
    }
  }, [dmBgKey]);

  const selectedDmBackground = selectedChat?.type === "dm" ? dmBackgrounds[selectedChat.id] : null;

  useEffect(() => {
    const storedTheme = localStorage.getItem(themeKey) || "classic";
    setTheme(storedTheme);
  }, [themeKey]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    if (user) {
      localStorage.setItem(themeKey, theme);
    }
  }, [theme, themeKey, user]);

  useEffect(() => {
    try {
      const data = localStorage.getItem(customThemeKey);
      const parsed = data ? JSON.parse(data) : null;
      setCustomThemeMedia(parsed);
      if (parsed?.tint) {
        setCustomThemeTint(parsed.tint);
        document.documentElement.style.setProperty("--xp-custom-tint", parsed.tint);
      }
      if (parsed?.titlebar) {
        document.documentElement.style.setProperty("--xp-custom-titlebar", parsed.titlebar);
      }
    } catch {
      setCustomThemeMedia(null);
    }
  }, [customThemeKey]);

  useEffect(() => {
    apiFetch("/api/me")
      .then((data) => {
        if (data?.csrfToken) setCsrfToken(data.csrfToken);
        if (data?.user) {
          setUser(data.user);
          setSettings({
            username: data.user.username,
            displayName: data.user.display_name || "",
            email: data.user.email || "",
            emailVerified: !!data.user.email_verified,
            avatar: data.user.avatar || "",
            status: data.user.status || "online",
            customStatus: data.user.custom_status || "",
            bio: data.user.bio || "",
            ringtoneUrl: data.user.ringtone_url || "",
            songTitle: data.user.song_title || "",
            songArtist: data.user.song_artist || "",
            songAlbum: data.user.song_album || "",
            songCoverUrl: data.user.song_cover_url || "",
            songAudioUrl: data.user.song_audio_url || "",
            songSource: data.user.song_source || "",
            songSourceUrl: data.user.song_source_url || "",
            aliases: data.user.aliases || [],
          });
          setMusicDraft({
            source: data.user.song_source || "spotify",
            sourceUrl: data.user.song_source_url || "",
            title: data.user.song_title || "",
            artist: data.user.song_artist || "",
            album: data.user.song_album || "",
            coverUrl: data.user.song_cover_url || "",
            audioUrl: data.user.song_audio_url || "",
          });
          setCustomStatusInput(data.user.custom_status || "");
          setAboutInput(data.user.bio || "");
        }
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!user?.id) return;
    try {
      const muteRaw = localStorage.getItem(`xp-muted-chats:${user.id}`);
      const nickRaw = localStorage.getItem(`xp-nicknames:${user.id}`);
      setMutedChats(muteRaw ? JSON.parse(muteRaw) : {});
      setNicknameMap(nickRaw ? JSON.parse(nickRaw) : {});
    } catch {
      setMutedChats({});
      setNicknameMap({});
    }
  }, [user?.id]);

  useEffect(() => {
    if (!user) return;
    loadChats();
    loadNotifications();
    loadFriendRequests();
    loadConnections();
    loadUsernames();
    loadStories();

    const socket = io(API_URL, { withCredentials: true });
    socketRef.current = socket;

    socket.on("dm:new", (message) => {
      const key = chatKey("dm", message.sender_id === user.id ? message.recipient_id : message.sender_id);
      setMessagesByChat((prev) => {
        const list = prev[key] || [];
        const pendingIndex = message.sender_id === user.id ? matchPending(list, message) : -1;
        if (pendingIndex !== -1) {
          const next = [...list];
          next[pendingIndex] = message;
          return { ...prev, [key]: next };
        }
        return { ...prev, [key]: mergeMessages(list, message) };
      });
      if (message.sender_id !== user.id) {
        if (!isChatMutedValue(key)) {
          markChatUnread(key);
        }
      }
      loadChats();
    });

    socket.on("dm:edit", (message) => {
      const key = chatKey("dm", message.sender_id === user.id ? message.recipient_id : message.sender_id);
      setMessagesByChat((prev) => ({
        ...prev,
        [key]: mergeMessages(prev[key] || [], message),
      }));
    });

    socket.on("dm:typing", ({ fromId, isTyping }) => {
      const key = chatKey("dm", fromId);
      if (isTyping) {
        setTypingByChat((prev) => ({ ...prev, [key]: true }));
        if (typingTimersRef.current[key]) clearTimeout(typingTimersRef.current[key]);
        typingTimersRef.current[key] = setTimeout(() => {
          setTypingByChat((prev) => ({ ...prev, [key]: false }));
        }, 2200);
      } else {
        if (typingTimersRef.current[key]) clearTimeout(typingTimersRef.current[key]);
        setTypingByChat((prev) => ({ ...prev, [key]: false }));
      }
    });

    socket.on("dm:deleted", ({ userId }) => {
      const key = chatKey("dm", userId);
      setMessagesByChat((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      loadChats();
    });

    socket.on("dm:read", ({ readerId, lastReadAt }) => {
      if (!readerId) return;
      setFriends((prev) => prev.map((f) => (f.id === readerId ? { ...f, last_read_at: lastReadAt } : f)));
      setFriendsAll((prev) => prev.map((f) => (f.id === readerId ? { ...f, last_read_at: lastReadAt } : f)));
      setManualDmUsers((prev) => prev.map((u) => (u.id === readerId ? { ...u, last_read_at: lastReadAt } : u)));
    });

    socket.on("notify:new", () => {
      loadNotifications();
      loadFriendRequests();
      loadUsernames();
      loadChats();
    });

    socket.on("friends:update", () => {
      loadChats();
      loadFriendRequests();
      loadNotifications();
      if (profileOpen && profileUser?.id) {
        loadProfile(profileUser.id);
      }
    });

    socket.on("profile:update", (payload) => {
      if (!payload?.id) return;
      setFriends((prev) => prev.map((f) => (f.id === payload.id ? { ...f, ...payload } : f)));
      setFriendsAll((prev) => prev.map((f) => (f.id === payload.id ? { ...f, ...payload } : f)));
      setManualDmUsers((prev) => prev.map((u) => (u.id === payload.id ? { ...u, ...payload } : u)));
      setGroups((prev) =>
        prev.map((g) => ({
          ...g,
          members: (g.members || []).map((m) => (m.id === payload.id ? { ...m, ...payload } : m)),
        }))
      );
      setSelectedGroup((prev) =>
        prev
          ? {
              ...prev,
              members: (prev.members || []).map((m) => (m.id === payload.id ? { ...m, ...payload } : m)),
            }
          : prev
      );
      if (selectedFriend?.id === payload.id) {
        setSelectedFriend((prev) => ({ ...prev, ...payload }));
      }
      if (profileUser?.id === payload.id) {
        setProfileUser((prev) => ({ ...prev, ...payload }));
      }
      if (profileSongPlayerRef.current?.ownerId === payload.id) {
        applyProfileSongUpdate(payload.id, payload);
      }
    });

    socket.on("presence:update", ({ userId, status }) => {
      setFriends((prev) => prev.map((f) => (f.id === userId ? { ...f, status } : f)));
      setFriendsAll((prev) => prev.map((f) => (f.id === userId ? { ...f, status } : f)));
      setManualDmUsers((prev) => prev.map((u) => (u.id === userId ? { ...u, status } : u)));
      setGroups((prev) =>
        prev.map((g) => ({
          ...g,
          members: (g.members || []).map((m) => (m.id === userId ? { ...m, status } : m)),
        }))
      );
    });

    socket.on("message:reactions", ({ messageType, messageId, otherId, groupId, reactions }) => {
      const key = messageType === "group" ? chatKey("group", groupId) : chatKey("dm", otherId);
      setMessagesByChat((prev) => ({
        ...prev,
        [key]: (prev[key] || []).map((m) => (m.id === messageId ? { ...m, reactions } : m)),
      }));
    });

    socket.on("message:deleted", ({ messageType, messageId, otherId, groupId }) => {
      const key = messageType === "group" ? chatKey("group", groupId) : chatKey("dm", otherId);
      setMessagesByChat((prev) => ({
        ...prev,
        [key]: (prev[key] || []).filter((m) => m.id !== messageId),
      }));
    });

    socket.on("group:new", (message) => {
      const key = chatKey("group", message.group_id);
      setMessagesByChat((prev) => {
        const list = prev[key] || [];
        const pendingIndex = message.sender_id === user.id ? matchPending(list, message) : -1;
        if (pendingIndex !== -1) {
          const next = [...list];
          next[pendingIndex] = message;
          return { ...prev, [key]: next };
        }
        return { ...prev, [key]: mergeMessages(list, message) };
      });
      if (message.sender_id !== user.id) {
        if (!isChatMutedValue(key)) {
          markChatUnread(key);
        }
      }
      loadChats();
    });

    socket.on("group:edit", (message) => {
      const key = chatKey("group", message.group_id);
      setMessagesByChat((prev) => ({
        ...prev,
        [key]: mergeMessages(prev[key] || [], message),
      }));
    });

    socket.on("call:request", ({ fromId }) => {
      setIncomingCall({ fromId });
      // keep caller UI only on the caller side; callee should show accept/decline
      if (!callStateRef.current?.withUserId || callStateRef.current.status === "idle") {
        setCallState((prev) => ({ ...prev, status: "ringing", withUserId: fromId }));
      }
    });

    socket.on("call:calling", ({ otherId }) => {
      if (!otherId) return;
      if (callStateRef.current?.status === "in-call") return;
      setIncomingCall(null);
      setCallState({ status: "calling", withUserId: otherId, startTime: null, muted: false, deafened: false });
      setCallAudioStates({});
    });

    socket.on("call:accept", async ({ fromId }) => {
      setIncomingCall(null);
      callEndedRef.current = false;
      await ensurePeer(fromId);
      await createOffer(fromId);
      setCallState({ status: "in-call", withUserId: fromId, startTime: Date.now(), muted: false, deafened: false });
      playCallChime("join");
    });

    socket.on("call:decline", () => {
      setIncomingCall(null);
      setCallState({ status: "idle", withUserId: null, startTime: null, muted: false, deafened: false });
      setCallAudioStates({});
      teardownPeer();
    });

    socket.on("call:active", ({ otherId, startedAt, audioStates }) => {
      setIncomingCall(null);
      setCallState({
        status: "active",
        withUserId: otherId,
        startTime: startedAt || Date.now(),
        muted: false,
        deafened: false,
      });
      setCallAudioStates(audioStates || {});
    });

    socket.on("call:timeout", () => {
      // call stays joinable; keep it active
      if (callStateRef.current?.withUserId) {
        setCallState((prev) => ({ ...prev, status: "active" }));
      }
    });

    socket.on("call:end", () => {
      callEndedRef.current = true;
      setCallState({ status: "idle", withUserId: null, startTime: null, muted: false, deafened: false });
      setCallAudioStates({});
      setRemotePresent(false);
      teardownPeer();
    });

    socket.on("call:status", ({ userId: statusUserId, muted, deafened }) => {
      setCallAudioStates((prev) => ({
        ...prev,
        [statusUserId]: { muted: !!muted, deafened: !!deafened },
      }));
    });

    socket.on("call:joined", () => {
      playCallChime("join");
    });

    socket.on("call:left", ({ otherId }) => {
      if (otherId === callStateRef.current?.withUserId) {
        setCallState((prev) => ({ ...prev, status: "active" }));
        remoteStreamRef.current = null;
        const audio = document.getElementById("xp-remote-audio");
        if (audio) audio.srcObject = null;
        setRemotePresent(false);
        teardownPeer();
      }
      playCallChime("leave");
    });

    socket.on("call:offer", async ({ fromId, offer }) => {
      await ensurePeer(fromId);
      if (!pcRef.current) return;
      if (pcRef.current.signalingState !== "stable") {
        try {
          await pcRef.current.setLocalDescription({ type: "rollback" });
        } catch {
          // ignore
        }
      }
      try {
        await pcRef.current.setRemoteDescription(offer);
      } catch {
        return;
      }
      const answer = await pcRef.current.createAnswer();
      await pcRef.current.setLocalDescription(answer);
      socket.emit("call:answer", { toId: fromId, answer });
      setCallState({ status: "in-call", withUserId: fromId, startTime: Date.now(), muted: false, deafened: false });
    });

    socket.on("call:answer", async ({ answer }) => {
      if (!pcRef.current) return;
      if (pcRef.current.signalingState !== "have-local-offer") return;
      try {
        await pcRef.current.setRemoteDescription(answer);
      } catch {
        // ignore
      }
    });

    socket.on("blackjack:state", (payload) => {
      if (!payload?.id) return;
      setBlackjackMatch((prev) => {
        if (!prev || prev.id === payload.id) return payload;
        return prev;
      });
    });

    socket.on("minigame:state", (payload) => {
      if (!payload?.id) return;
      setMiniGameMatch((prev) => {
        if (!prev || prev.id === payload.id) return payload;
        return prev;
      });
    });

    socket.on("call:ice", async ({ candidate }) => {
      if (!pcRef.current || !candidate) return;
      try {
        await pcRef.current.addIceCandidate(candidate);
      } catch {
        // ignore
      }
    });

    socket.on("call:rejoin", async ({ fromId }) => {
      if (callEndedRef.current) return;
      await ensurePeer(fromId);
      await createOffer(fromId);
      setCallState({ status: "in-call", withUserId: fromId, startTime: Date.now(), muted: false, deafened: false });
    });

    socket.on("call:rejoin-ack", async ({ otherId, startedAt, audioStates }) => {
      if (callEndedRef.current) return;
      await ensurePeer(otherId);
      setCallState({ status: "in-call", withUserId: otherId, startTime: startedAt || Date.now(), muted: false, deafened: false });
      setCallAudioStates(audioStates || {});
      setJoiningCall(false);
    });

    socket.on("call:rejoin-denied", () => {
      setJoiningCall(false);
    });

    socket.on("group:call:ring", ({ groupId, fromId, startedAt }) => {
      setGroupCallVisible(true);
      setGroupCall((prev) => ({
        groupId,
        status: prev.groupId === groupId && prev.status === "in-call" ? "in-call" : "ringing",
        participants: prev.groupId === groupId ? prev.participants : [],
        startedAt,
        fromId,
      }));
    });

    socket.on("group:call:participants", ({ groupId, participants, startedAt, audioStates }) => {
      setGroupCallVisible(true);
      setGroupCall({ groupId, status: "in-call", participants, startedAt });
      setGroupCallAudioStates(audioStates || {});
      participants.forEach((pid) => setupGroupPeer(groupId, pid));
    });

    socket.on("group:call:join", ({ groupId, userId }) => {
      if (groupCallRef.current?.groupId !== groupId) return;
      setGroupCallVisible(true);
      setGroupCall((prev) => ({
        ...prev,
        status: "in-call",
        participants: prev.participants.includes(userId) ? prev.participants : [...prev.participants, userId],
      }));
      setupGroupPeer(groupId, userId);
      if (userId !== user.id) {
        playCallChime("join");
      }
    });

    socket.on("group:call:leave", ({ groupId, userId }) => {
      if (groupCallRef.current?.groupId !== groupId) return;
      cleanupGroupPeer(userId);
      setGroupCall((prev) => ({
        ...prev,
        participants: prev.participants.filter((id) => id !== userId),
      }));
      setGroupCallAudioStates((prev) => {
        const next = { ...prev };
        delete next[userId];
        return next;
      });
      if (userId !== user.id) {
        playCallChime("leave");
      }
    });

    socket.on("group:call:active", ({ groupId, startedAt, audioStates }) => {
      if (groupCallRef.current?.groupId !== groupId) return;
      setGroupCallVisible(true);
      setGroupCall((prev) => ({
        ...prev,
        status: "in-call",
        startedAt: prev.startedAt || startedAt || Date.now(),
      }));
      if (audioStates) {
        setGroupCallAudioStates(audioStates);
      }
    });

    socket.on("group:call:status", ({ groupId, userId: statusUserId, muted, deafened }) => {
      if (groupCallRef.current?.groupId !== groupId) return;
      setGroupCallAudioStates((prev) => ({
        ...prev,
        [statusUserId]: { muted: !!muted, deafened: !!deafened },
      }));
    });

    socket.on("group:call:timeout", ({ groupId }) => {
      if (groupCallRef.current?.groupId !== groupId) return;
      setGroupCall((prev) => ({
        ...prev,
        status: "in-call",
        participants: prev.participants.slice(0, 1),
      }));
    });

    socket.on("group:call:ended", ({ groupId }) => {
      if (groupCallRef.current?.groupId === groupId) {
        setGroupCallVisible(false);
        cleanupGroupCall();
      }
    });

    socket.on("group:call:offer", async ({ groupId, fromId, offer }) => {
      await setupGroupPeer(groupId, fromId);
      const pc = groupPeersRef.current.get(fromId)?.pc;
      if (!pc) return;
      if (pc.signalingState !== "stable") {
        try {
          await pc.setLocalDescription({ type: "rollback" });
        } catch {
          // ignore
        }
      }
      try {
        await pc.setRemoteDescription(offer);
      } catch {
        return;
      }
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      socket.emit("group:call:answer", { groupId, toId: fromId, answer });
    });

    socket.on("group:call:answer", async ({ fromId, answer }) => {
      const pc = groupPeersRef.current.get(fromId)?.pc;
      if (!pc) return;
      if (pc.signalingState !== "have-local-offer") return;
      try {
        await pc.setRemoteDescription(answer);
      } catch {
        // ignore
      }
    });

    socket.on("group:call:ice", async ({ fromId, candidate }) => {
      const pc = groupPeersRef.current.get(fromId)?.pc;
      if (!pc || !candidate) return;
      try {
        await pc.addIceCandidate(candidate);
      } catch {
        // ignore
      }
    });

    return () => {
      Object.values(typingTimersRef.current).forEach((t) => clearTimeout(t));
      typingTimersRef.current = {};
      socket.disconnect();
      socketRef.current = null;
    };
  }, [user]);

  useEffect(() => {
    function handleResize() {
      setIsMobile(window.innerWidth <= 720);
    }
    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (!isMobile) return;
    if (selectedChat && view !== "settings") {
      setMobileView("chat");
    } else {
      setMobileView("list");
    }
  }, [isMobile, selectedChat, view]);

  useEffect(() => {
    if (!isMobile || mobileView !== "chat") return;
    const el = chatBodyRef.current;
    if (!el) return;
    const onStart = (e) => {
      const touch = e.touches?.[0];
      if (!touch) return;
      mobileTouchRef.current = { x: touch.clientX, y: touch.clientY, active: true };
    };
    const onMove = (e) => {
      if (!mobileTouchRef.current.active) return;
      const touch = e.touches?.[0];
      if (!touch) return;
      const dx = touch.clientX - mobileTouchRef.current.x;
      const dy = touch.clientY - mobileTouchRef.current.y;
      if (dx > 80 && Math.abs(dy) < 60) {
        mobileTouchRef.current.active = false;
        backToList();
      }
    };
    const onEnd = () => {
      mobileTouchRef.current.active = false;
    };
    el.addEventListener("touchstart", onStart, { passive: true });
    el.addEventListener("touchmove", onMove, { passive: true });
    el.addEventListener("touchend", onEnd);
    return () => {
      el.removeEventListener("touchstart", onStart);
      el.removeEventListener("touchmove", onMove);
      el.removeEventListener("touchend", onEnd);
    };
  }, [isMobile, mobileView]);

  useEffect(() => {
    if (selectedChat?.type !== "dm") {
      setShowChatProfile(false);
    }
  }, [selectedChat?.type]);

  useEffect(() => {
    if (view === "settings") {
      setShowChatProfile(false);
    }
  }, [view]);

  useEffect(() => {
    let active = true;
    if (!showChatProfile || !selectedFriend?.id) {
      setChatProfileConnections([]);
      return;
    }
    apiFetch(`/api/connections/${selectedFriend.id}`)
      .then((data) => {
        if (!active) return;
        const list = (data.connections || []).filter((c) => c.visibility !== "hidden");
        setChatProfileConnections(list);
      })
      .catch(() => {
        if (!active) return;
        setChatProfileConnections([]);
      });
    return () => {
      active = false;
    };
  }, [showChatProfile, selectedFriend?.id]);

  useEffect(() => {
    return () => {
      Object.values(audioTickRef.current || {}).forEach((t) => clearInterval(t));
      audioTickRef.current = {};
    };
  }, []);

  useEffect(() => {
    statusRef.current = settings.status || "online";
  }, [settings.status]);

  useEffect(() => {
    if (!dmBgModal) return;
    try {
      const recents = JSON.parse(localStorage.getItem(`xp-dm-bg-recent:${user?.id || "guest"}`) || "[]");
      if (Array.isArray(recents)) setDmBgRecent(recents);
    } catch {
      setDmBgRecent([]);
    }
  }, [dmBgModal, user?.id]);

  useEffect(() => {
    if (!dmBgModal) return;
    const existing = dmBackgrounds[dmBgModal];
    if (!existing) return;
    setDmBgMode(existing.type || "color");
    if (existing.type === "color") setDmBgColor(existing.value || "#f7f7f7");
    if (existing.type === "gradient") setDmBgGradient(existing.value || "");
    if (existing.type === "image") {
      setDmBgFit(existing.fit || "fill");
    }
  }, [dmBgModal, dmBackgrounds]);

  useEffect(() => {
    if (!user) return;
    const IDLE_MS = 10 * 60 * 1000;

    function resetIdle() {
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
      idleTimerRef.current = setTimeout(() => {
        if (statusRef.current === "online") {
          autoIdleRef.current = true;
          changeStatus("away");
        }
      }, IDLE_MS);
    }

    function handleActivity() {
      if (autoIdleRef.current && statusRef.current === "away") {
        autoIdleRef.current = false;
        changeStatus("online");
      }
      resetIdle();
    }

    resetIdle();
    window.addEventListener("mousemove", handleActivity);
    window.addEventListener("keydown", handleActivity);
    window.addEventListener("mousedown", handleActivity);
    window.addEventListener("scroll", handleActivity, true);
    window.addEventListener("touchstart", handleActivity, { passive: true });

    return () => {
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
      window.removeEventListener("mousemove", handleActivity);
      window.removeEventListener("keydown", handleActivity);
      window.removeEventListener("mousedown", handleActivity);
      window.removeEventListener("scroll", handleActivity, true);
      window.removeEventListener("touchstart", handleActivity);
    };
  }, [user]);

  useEffect(() => {
    if (!storyViewer.open || storyPaused) return;
    const list = stories[storyViewer.userIndex]?.stories || [];
    const current = list[storyViewer.storyIndex];
    if (!current) return;
    const duration = current.type === "video" ? STORY_VIDEO_MS : STORY_IMAGE_MS;
    const id = setTimeout(() => {
      advanceStory();
    }, duration);
    return () => clearTimeout(id);
  }, [storyViewer, storyPaused, stories]);

  useEffect(() => {
    manualDmUsersRef.current = manualDmUsers;
  }, [manualDmUsers]);

  useEffect(() => {
    const list = stories[storyViewer.userIndex]?.stories || [];
    const current = list[storyViewer.storyIndex];
    if (!current || current.type !== "video") return;
    const el = storyVideoRef.current;
    if (!el) return;
    if (storyPaused) {
      el.pause();
    } else {
      el.play().catch(() => {});
    }
  }, [storyPaused, storyViewer, stories]);

  useEffect(() => {
    if (!storyViewer.open) return;
    const list = stories[storyViewer.userIndex]?.stories || [];
    const current = list[storyViewer.storyIndex];
    if (!current) return;
    if (lastStoryViewedRef.current === current.id) return;
    lastStoryViewedRef.current = current.id;
    apiFetch(`/api/stories/${current.id}/view`, { method: "POST" }).catch(() => {});
    loadStories();
  }, [storyViewer.open, storyViewer.userIndex, storyViewer.storyIndex, stories]);

  useEffect(() => {
    if (!storyViewer.open) return;
    const id = setInterval(() => {
      setStoryViewer((prev) => ({ ...prev }));
    }, 1000);
    return () => clearInterval(id);
  }, [storyViewer.open]);

  useEffect(() => {
    if (!callState.startTime) return;
    const id = setInterval(() => {
      setCallDuration((Date.now() - callState.startTime) / 1000);
    }, 1000);
    return () => clearInterval(id);
  }, [callState.startTime]);

  useEffect(() => {
    if (groupCall.status !== "in-call" || !groupCall.groupId) {
      setGroupCallDuration(0);
      return;
    }
    const base = groupCall.startedAt || Date.now();
    const id = setInterval(() => {
      setGroupCallDuration((Date.now() - base) / 1000);
    }, 1000);
    return () => clearInterval(id);
  }, [groupCall.status, groupCall.groupId, groupCall.startedAt]);

  useEffect(() => {
    if (!callSettingsOpen && !voicePanelOpen) return;
    navigator.mediaDevices.enumerateDevices().then((devices) => {
      setCallDevices({
        inputs: devices.filter((d) => d.kind === "audioinput"),
        outputs: devices.filter((d) => d.kind === "audiooutput"),
      });
    });
  }, [callSettingsOpen, voicePanelOpen]);

  useEffect(() => {
    const audio = document.getElementById("xp-remote-audio");
    if (audio) audio.volume = callSettings.speakerVolume;
    groupCall.participants.forEach((pid) => {
      const el = document.getElementById(`xp-group-audio-${pid}`);
      if (el) el.volume = callSettings.speakerVolume;
    });
  }, [callSettings.speakerVolume, groupCall.participants]);

  useEffect(() => {
    applyOutputDevice(callSettings.outputId || "");
  }, [callSettings.outputId, groupCall.participants, remotePresent]);

  useEffect(() => {
    return () => {
      if (micTestRafRef.current) cancelAnimationFrame(micTestRafRef.current);
      if (micTestStreamRef.current) {
        micTestStreamRef.current.getTracks().forEach((t) => t.stop());
        micTestStreamRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (callState.status !== "in-call" && callState.status !== "active") {
      setSpeakingLocal(false);
      setSpeakingRemote(false);
      return;
    }
    const local = rawMicStreamRef.current || localStreamRef.current;
    const remoteAudioEl = document.getElementById("xp-remote-audio");
    const remote = remoteStreamRef.current || remoteAudioEl?.srcObject;
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    ctx.resume().catch(() => {});
    const localAnalyser = ctx.createAnalyser();
    const remoteAnalyser = ctx.createAnalyser();
    localAnalyser.fftSize = 512;
    remoteAnalyser.fftSize = 512;
    if (local) {
      const localSource = ctx.createMediaStreamSource(local);
      localSource.connect(localAnalyser);
    }
    if (remote) {
      const remoteSource = ctx.createMediaStreamSource(remote);
      remoteSource.connect(remoteAnalyser);
    }
    const localData = new Uint8Array(localAnalyser.frequencyBinCount);
    const remoteData = new Uint8Array(remoteAnalyser.frequencyBinCount);
    const threshold = 0.01 + (1 - inputSensitivity) * 0.03;
    const tick = () => {
      localAnalyser.getByteTimeDomainData(localData);
      remoteAnalyser.getByteTimeDomainData(remoteData);
      const localLevel = Math.sqrt(
        localData.reduce((a, b) => {
          const v = (b - 128) / 128;
          return a + v * v;
        }, 0) / localData.length
      );
      const remoteLevel = Math.sqrt(
        remoteData.reduce((a, b) => {
          const v = (b - 128) / 128;
          return a + v * v;
        }, 0) / remoteData.length
      );
      if (micGateGainRef.current) {
        const gateFloor = 0.2;
        const target = localLevel > threshold ? 1 : gateFloor;
        micGateGainRef.current.gain.setTargetAtTime(target, ctx.currentTime, 0.08);
      }
      if (localLevel > threshold) {
        clearTimeout(speakingTimersRef.current.localOff);
        if (!speakingTimersRef.current.localOn) {
          speakingTimersRef.current.localOn = setTimeout(() => {
            setSpeakingLocal(true);
            speakingTimersRef.current.localOn = null;
          }, 120);
        }
      } else if (!speakingTimersRef.current.localOff) {
        speakingTimersRef.current.localOff = setTimeout(() => {
          setSpeakingLocal(false);
          speakingTimersRef.current.localOff = null;
        }, 220);
      }
      if (remoteLevel > threshold) {
        clearTimeout(speakingTimersRef.current.remoteOff);
        if (!speakingTimersRef.current.remoteOn) {
          speakingTimersRef.current.remoteOn = setTimeout(() => {
            setSpeakingRemote(true);
            speakingTimersRef.current.remoteOn = null;
          }, 120);
        }
      } else if (!speakingTimersRef.current.remoteOff) {
        speakingTimersRef.current.remoteOff = setTimeout(() => {
          setSpeakingRemote(false);
          speakingTimersRef.current.remoteOff = null;
        }, 220);
      }
      requestAnimationFrame(tick);
    };
    tick();
    return () => {
      ctx.close();
    };
  }, [callState.status, audioStreamTick, inputSensitivity]);

  useEffect(() => {
    if (groupCall.status !== "in-call" && groupCall.status !== "active") {
      setGroupSpeakingMap({});
      return;
    }
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    ctx.resume().catch(() => {});
    const analysers = new Map();
    const buffers = new Map();
    const threshold = 0.015 + (1 - inputSensitivity) * 0.04;

    const attach = (pid) => {
      let stream = null;
      if (pid === user?.id) {
        stream = rawMicStreamRef.current || localStreamRef.current;
      } else {
        const el = document.getElementById(`xp-group-audio-${pid}`);
        stream = el?.srcObject || null;
      }
      if (!stream) return;
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 256;
      const source = ctx.createMediaStreamSource(stream);
      source.connect(analyser);
      analysers.set(pid, analyser);
      buffers.set(pid, new Uint8Array(analyser.frequencyBinCount));
    };

    (groupCall.participants || []).forEach((pid) => attach(pid));

    let raf = 0;
    const tick = () => {
      const next = {};
      analysers.forEach((analyser, pid) => {
        const data = buffers.get(pid);
        if (!data) return;
        analyser.getByteTimeDomainData(data);
        const level = Math.sqrt(
          data.reduce((a, b) => {
            const v = (b - 128) / 128;
            return a + v * v;
          }, 0) / data.length
        );
        next[pid] = level > threshold;
      });
      setGroupSpeakingMap(next);
      raf = requestAnimationFrame(tick);
    };
    tick();
    return () => {
      if (raf) cancelAnimationFrame(raf);
      ctx.close();
    };
  }, [groupCall.status, groupCall.participants, inputSensitivity, audioStreamTick, user?.id]);

  function getHiFiAudioConstraints(deviceId) {
    return {
      deviceId: deviceId ? { exact: deviceId } : undefined,
      sampleRate: 48000,
      channelCount: 1,
      sampleSize: 16,
      echoCancellation: echoCancel,
      noiseSuppression: echoCancel,
      autoGainControl: echoCancel,
      latency: { ideal: 0.01 },
    };
  }

  async function applyInputDevice(deviceId) {
    const buildSeq = ++micBuildSeqRef.current;
    if (rawMicStreamRef.current) {
      rawMicStreamRef.current.getTracks().forEach((t) => t.stop());
    }
    const stream = await navigator.mediaDevices.getUserMedia({
      audio: getHiFiAudioConstraints(deviceId),
    });
    if (buildSeq !== micBuildSeqRef.current) {
      stream.getTracks().forEach((t) => t.stop());
      return;
    }
    rawMicStreamRef.current = stream;
    await rebuildAudioGraph(stream, buildSeq);
  }

  async function ensureProcessedMic() {
    if (rawMicStreamRef.current && localStreamRef.current) return;
    const buildSeq = ++micBuildSeqRef.current;
    if (rawMicStreamRef.current) {
      rawMicStreamRef.current.getTracks().forEach((t) => t.stop());
    }
    const stream = await navigator.mediaDevices.getUserMedia({
      audio: getHiFiAudioConstraints(),
    });
    if (buildSeq !== micBuildSeqRef.current) {
      stream.getTracks().forEach((t) => t.stop());
      return;
    }
    rawMicStreamRef.current = stream;
    await rebuildAudioGraph(stream, buildSeq);
  }

  async function rebuildAudioGraph(inputStream, buildSeq = ++micBuildSeqRef.current) {
    if (!inputStream) return;
    const safePitch = Math.max(-10, Math.min(10, pitchShift));
    let ctx;
    try {
      ctx = new (window.AudioContext || window.webkitAudioContext)({
        sampleRate: 48000,
        latencyHint: "interactive",
      });
      await ctx.resume();
    } catch {
      return;
    }
    if (buildSeq !== micBuildSeqRef.current) {
      ctx.close().catch(() => {});
      return;
    }
    if (ctx.state === "closed") return;
    if (micGainCtxRef.current) {
      micGainCtxRef.current.close().catch(() => {});
    }
    micGainCtxRef.current = ctx;
    const source = ctx.createMediaStreamSource(inputStream);
    const gateGain = ctx.createGain();
    gateGain.gain.value = 1;
    micGateGainRef.current = gateGain;
    const low = ctx.createBiquadFilter();
    low.type = "lowshelf";
    low.frequency.value = 200;
    low.gain.value = eqLow;
    micLowRef.current = low;
    const mid = ctx.createBiquadFilter();
    mid.type = "peaking";
    mid.frequency.value = 1000;
    mid.Q.value = 0.8;
    mid.gain.value = eqMid;
    micMidRef.current = mid;
    const high = ctx.createBiquadFilter();
    high.type = "highshelf";
    high.frequency.value = 3200;
    high.gain.value = eqHigh;
    micHighRef.current = high;
    const gain = ctx.createGain();
    gain.gain.value = callSettings.micVolume;
    micGainNodeRef.current = gain;
    const comp = ctx.createDynamicsCompressor();
    comp.threshold.value = -28;
    comp.knee.value = 24;
    comp.ratio.value = 6;
    comp.attack.value = 0.003;
    comp.release.value = 0.2;

    const dest = ctx.createMediaStreamDestination();
    let node = source;
    micPitchParamRef.current = null;
    micPitchNodeRef.current = null;
    node.connect(gateGain);
    node = gateGain;

    // higher-quality pitch shift using AudioWorklet (fallback to ScriptProcessor)
    if (voiceEffect === "pitch" && safePitch !== 0) {
      const rate = Math.pow(2, safePitch / 12);
      const useWorklet = !!ctx.audioWorklet && ctx.state === "running";
      let pitchApplied = false;
      if (useWorklet) {
        try {
          if (!window.__pitchWorkletUrl) {
            const workletCode = `
            class PitchShiftProcessor extends AudioWorkletProcessor {
              static get parameterDescriptors() {
                return [{ name: 'rate', defaultValue: 1.0, minValue: 0.25, maxValue: 4.0 }];
              }
              constructor() {
                super();
                this.phase = 0;
              }
              process(inputs, outputs, parameters) {
                const input = inputs[0];
                const output = outputs[0];
                if (!input || input.length === 0) return true;
                const rateArr = parameters.rate;
                const rate = rateArr.length ? rateArr[0] : 1.0;
                const inL = input[0] || new Float32Array(output[0].length);
                const inR = input[1] || inL;
                const outL = output[0];
                const outR = output[1] || output[0];
                const len = outL.length;
                for (let i = 0; i < len; i++) {
                  const idx = Math.floor(this.phase) % inL.length;
                  const idx2 = (idx + 1) % inL.length;
                  const frac = this.phase - Math.floor(this.phase);
                  const s0 = inL[idx] || 0;
                  const s1 = inL[idx2] || 0;
                  const sample = s0 * (1 - frac) + s1 * frac;
                  outL[i] = sample;
                  outR[i] = sample;
                  this.phase += rate;
                  if (this.phase >= inL.length) this.phase -= inL.length;
                }
                return true;
              }
            }
            registerProcessor('pitch-shift-processor', PitchShiftProcessor);
          `;
            const blob = new Blob([workletCode], { type: "application/javascript" });
            window.__pitchWorkletUrl = URL.createObjectURL(blob);
          }
          await ctx.audioWorklet.addModule(window.__pitchWorkletUrl);
          if (buildSeq !== micBuildSeqRef.current) {
            ctx.close().catch(() => {});
            return;
          }
          const shifter = new AudioWorkletNode(ctx, "pitch-shift-processor", {
            parameterData: { rate },
          });
          if (shifter.parameters?.get("rate")) {
            const param = shifter.parameters.get("rate");
            param.setValueAtTime(rate, ctx.currentTime);
            micPitchParamRef.current = param;
          }
          micPitchNodeRef.current = shifter;
          node.connect(shifter);
          node = shifter;
          pitchApplied = true;
        } catch {
          // fall back to script processor
        }
      }
      if (!pitchApplied) {
        const shifter = ctx.createScriptProcessor(2048, 2, 2);
        let phase = 0;
        shifter.onaudioprocess = (e) => {
          const inputL = e.inputBuffer.getChannelData(0);
          const outputL = e.outputBuffer.getChannelData(0);
          const inputR = e.inputBuffer.numberOfChannels > 1 ? e.inputBuffer.getChannelData(1) : inputL;
          const outputR = e.outputBuffer.getChannelData(1);
          const len = inputL.length;
          for (let i = 0; i < len; i++) {
            const idx = Math.floor(phase);
            const idx2 = (idx + 1) % len;
            const frac = phase - idx;
            outputL[i] = inputL[idx] * (1 - frac) + inputL[idx2] * frac;
            outputR[i] = inputR[idx] * (1 - frac) + inputR[idx2] * frac;
            phase += rate;
            if (phase >= len) phase -= len;
          }
        };
        node.connect(shifter);
        node = shifter;
      }
    }

    if (voiceEffect === "robot") {
      const osc = ctx.createOscillator();
      osc.type = "square";
      osc.frequency.value = 30;
      const mod = ctx.createGain();
      mod.gain.value = 0.5;
      osc.connect(mod.gain);
      node.connect(mod);
      node = mod;
      osc.start();
      micEffectOscRef.current = osc;
    } else {
      micEffectOscRef.current?.stop?.();
      micEffectOscRef.current = null;
    }

    if (voiceEffect === "warm") {
      low.gain.value = 4 * Math.min(1, Math.abs(safePitch) / 15 || 0.5);
      high.gain.value = -2 * Math.min(1, Math.abs(safePitch) / 15 || 0.5);
    } else {
      low.gain.value = eqLow;
      mid.gain.value = eqMid;
      high.gain.value = eqHigh;
    }

    node.connect(low);
    low.connect(mid);
    mid.connect(high);
    high.connect(gain);
    gain.connect(comp);
    comp.connect(dest);

    localStreamRef.current = dest.stream;
    applyOutgoingAudioState(callStateRef.current?.muted, callStateRef.current?.deafened);
    setAudioStreamTick((n) => n + 1);
    const track = dest.stream.getAudioTracks()[0];
    async function applySenderParams(sender) {
      if (!sender || !sender.getParameters) return;
      const params = sender.getParameters();
      if (!params.encodings || !params.encodings.length) params.encodings = [{}];
      params.encodings[0].maxBitrate = 192000;
      params.encodings[0].priority = "high";
      params.encodings[0].networkPriority = "high";
      try {
        await sender.setParameters(params);
      } catch {
        // ignore
      }
    }

    if (pcRef.current && track) {
      track.contentHint = "speech";
      const audioSenders = pcRef.current.getSenders().filter((s) => s.track && s.track.kind === "audio");
      const sender = audioSenders[0];
      if (sender) {
        await sender.replaceTrack(track);
        audioSenders.slice(1).forEach((s) => pcRef.current.removeTrack(s));
        await applySenderParams(sender);
      } else {
        const newSender = pcRef.current.addTrack(track, dest.stream);
        await applySenderParams(newSender);
      }
    }
    groupPeersRef.current.forEach(async (entry) => {
      const pc = entry?.pc || entry;
      if (!pc || !track) return;
      track.contentHint = "speech";
      const audioSenders = pc.getSenders().filter((s) => s.track && s.track.kind === "audio");
      const sender = audioSenders[0];
      if (sender) {
        await sender.replaceTrack(track);
        audioSenders.slice(1).forEach((s) => pc.removeTrack(s));
        await applySenderParams(sender);
      } else {
        const newSender = pc.addTrack(track, dest.stream);
        await applySenderParams(newSender);
      }
    });
  }

  async function applyOutputDevice(deviceId) {
    const audio = document.getElementById("xp-remote-audio");
    if (audio && audio.setSinkId) {
      try {
        await audio.setSinkId(deviceId);
      } catch {
        // ignore
      }
    }
    groupCall.participants.forEach(async (pid) => {
      const el = document.getElementById(`xp-group-audio-${pid}`);
      if (el && el.setSinkId) {
        try {
          await el.setSinkId(deviceId);
        } catch {
          // ignore
        }
      }
    });
  }

  async function toggleMicTest() {
    if (micTestOn) {
      setMicTestOn(false);
      setMicTestLevel(0);
      if (micTestRafRef.current) cancelAnimationFrame(micTestRafRef.current);
      if (micTestStreamRef.current) {
        micTestStreamRef.current.getTracks().forEach((t) => t.stop());
        micTestStreamRef.current = null;
      }
      if (micTestAudioRef.current) {
        micTestAudioRef.current.pause();
        micTestAudioRef.current.srcObject = null;
      }
      if (callState.status === "in-call" || callState.status === "active" || groupCall.status === "in-call") {
        await ensureProcessedMic();
      }
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: getHiFiAudioConstraints(callSettings.inputId),
      });
      micTestStreamRef.current = stream;
      if (micTestAudioRef.current) {
        micTestAudioRef.current.srcObject = stream;
        micTestAudioRef.current.muted = false;
        micTestAudioRef.current.volume = callSettings.speakerVolume;
        micTestAudioRef.current.play().catch(() => {});
      }
      setMicTestOn(true);
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 256;
      const source = ctx.createMediaStreamSource(stream);
      source.connect(analyser);
      const data = new Uint8Array(analyser.frequencyBinCount);
      const tick = () => {
        analyser.getByteTimeDomainData(data);
        const level = data.reduce((a, b) => a + Math.abs(b - 128), 0) / data.length;
        setMicTestLevel(Math.min(1, level / 35));
        micTestRafRef.current = requestAnimationFrame(tick);
      };
      tick();
    } catch {
      setMicTestOn(false);
    }
  }

  useEffect(() => {
    if (!screenShareWindow.open) return;
    if (!groupLocalScreenRef.current && !dmLocalScreenRef.current && screenShareEntries.length === 0) {
      setScreenShareWindow((prev) => ({ ...prev, open: false }));
    }
  }, [screenShareEntries, screenShareWindow.open]);

  useEffect(() => {
    const shouldRing =
      callState.status === "calling" ||
      !!incomingCall ||
      (groupCallVisible && groupCall.status === "ringing") ||
      !!incomingGroupCall;
    if (shouldRing) {
      startRingTone();
    } else {
      stopRingTone();
    }
    return () => stopRingTone();
  }, [callState.status, incomingCall, groupCall.status, incomingGroupCall]);

  useEffect(() => {
    profileSongPlayerRef.current = profileSongPlayer;
  }, [profileSongPlayer]);

  useEffect(() => {
    if (!user?.id) return;
    const stored = localStorage.getItem(`xp-game-audio-muted:${user.id}`);
    if (stored === "1") {
      setGameAudioMuted(true);
    }
  }, [user?.id]);

  useEffect(() => {
    if (!user?.id) return;
    localStorage.setItem(`xp-game-audio-muted:${user.id}`, gameAudioMuted ? "1" : "0");
  }, [user?.id, gameAudioMuted]);

  useEffect(() => {
    const audio = profileSongAudioRef.current;
    if (!audio) return;
    audio.onended = () => {
      setProfileSongPlayer((prev) => ({ ...prev, playing: false }));
    };
    audio.volume = 0.85;
    return () => {
      audio.onended = null;
    };
  }, []);

  useEffect(() => {
    const audio = profileSongAudioRef.current;
    if (!audio) return;
    if (profileSongPlayer.audioUrl) {
      audio.src = resolveMediaUrl(profileSongPlayer.audioUrl);
      audio.currentTime = 0;
      if (profileSongPlayer.playing) {
        audio.play().catch(() => {});
      }
    } else {
      audio.removeAttribute("src");
      audio.load();
    }
  }, [profileSongPlayer.audioUrl]);

  useEffect(() => {
    const audio = profileSongAudioRef.current;
    if (!audio) return;
    if (!profileSongPlayer.audioUrl) return;
    if (profileSongPlayer.playing) {
      audio.play().catch(() => {});
    } else {
      audio.pause();
    }
  }, [profileSongPlayer.playing, profileSongPlayer.audioUrl]);

  useEffect(() => {
    const audio = gameMusicAudioRef.current;
    if (!audio) return;
    audio.loop = true;
    audio.preload = "auto";
    audio.src = GAME_LOOP_URL;
    audio.volume = 0;
  }, []);

  useEffect(() => {
    const audio = coinflipSfxAudioRef.current;
    if (!audio) return;
    audio.preload = "auto";
    audio.src = COINFLIP_SFX_URL;
    audio.volume = gameAudioMuted ? 0 : 0.92;
  }, [gameAudioMuted]);

  const shouldPlayGameMusic = (blackjackPanelOpen || miniGameHubOpen) && !gameAudioMuted;

  useEffect(() => {
    const audio = gameMusicAudioRef.current;
    if (!audio) return;
    if (shouldPlayGameMusic) {
      audio.play().catch(() => {});
      fadeGameMusic(0.24, 520);
    } else {
      fadeGameMusic(0, 380);
    }
    return () => {
      if (gameMusicFadeRef.current) {
        clearInterval(gameMusicFadeRef.current);
        gameMusicFadeRef.current = null;
      }
    };
  }, [shouldPlayGameMusic]);

  useEffect(() => {
    if (gameAudioMuted) return;
    if (miniGameMatch?.game_type !== "coinflip") return;
    if (miniGameMatch?.status !== "ended") return;
    if (!miniGameMatch?.state?.result) return;
    const key = `${miniGameMatch.id}:${miniGameMatch.state.result}`;
    if (lastCoinflipSfxRef.current === key) return;
    lastCoinflipSfxRef.current = key;
    const audio = coinflipSfxAudioRef.current;
    if (!audio) return;
    audio.currentTime = 0;
    audio.play().catch(() => {});
  }, [miniGameMatch?.id, miniGameMatch?.game_type, miniGameMatch?.status, miniGameMatch?.state?.result, gameAudioMuted]);

  useEffect(() => {
    const match = blackjackMatch;
    const matchState = match?.state || null;
    if (!match?.id || !matchState) {
      prevBlackjackSnapshotRef.current = null;
      return;
    }
    const snapshot = {
      status: match.status,
      dealerCards: (matchState.dealer?.cards || []).length,
      dealerReveal: matchState.dealer?.status === "reveal",
      playerCardCounts: Object.fromEntries(
        Object.entries(matchState.players || {}).map(([playerId, info]) => [
          playerId,
          (info?.hands || []).reduce((sum, hand) => sum + (hand?.cards?.length || 0), 0),
        ])
      ),
      winnerId: match.winner_id || null,
    };
    const prev = prevBlackjackSnapshotRef.current;
    if (prev) {
      const prevTotal =
        prev.dealerCards + Object.values(prev.playerCardCounts || {}).reduce((sum, count) => sum + Number(count || 0), 0);
      const nextTotal =
        snapshot.dealerCards + Object.values(snapshot.playerCardCounts || {}).reduce((sum, count) => sum + Number(count || 0), 0);
      if (nextTotal > prevTotal) {
        playBlackjackDealSound(nextTotal - prevTotal);
      }
      if (snapshot.dealerReveal && !prev.dealerReveal) {
        playBlackjackTone({ type: "square", start: 420, end: 260, duration: 0.09, gain: 0.028 });
      }
      if (snapshot.status === "ended" && prev.status !== "ended") {
        if (snapshot.winnerId === user?.id) {
          playBlackjackTone({ type: "triangle", start: 540, end: 880, duration: 0.18, gain: 0.032 });
          playBlackjackTone({ type: "triangle", start: 760, end: 1180, duration: 0.22, gain: 0.026, delay: 0.08 });
        } else {
          playBlackjackTone({ type: "sawtooth", start: 320, end: 140, duration: 0.22, gain: 0.026 });
        }
      }
    }
    prevBlackjackSnapshotRef.current = snapshot;
  }, [blackjackMatch?.id, blackjackMatch?.status, blackjackMatch?.winner_id, blackjackMatch?.state, user?.id, gameAudioMuted]);

  useEffect(() => {
    if (!blackjackPanelOpen || !blackjackMatch?.id) return undefined;
    if (blackjackMatch.status !== "deposit" && blackjackMatch.status !== "ended") return undefined;
    if (CHAIN_MODE !== "evm") return undefined;
    const timer = setInterval(() => {
      apiFetch(`/api/blackjack/${blackjackMatch.id}/sync`, { method: "POST" })
        .then((res) => {
          if (res?.match) setBlackjackMatch(res.match);
        })
        .catch(() => {});
    }, 5000);
    return () => clearInterval(timer);
  }, [blackjackPanelOpen, blackjackMatch?.id, blackjackMatch?.status]);

  useEffect(() => {
    if (!miniGameHubOpen || !miniGameMatch?.id) return undefined;
    if (miniGameMatch.status !== "deposit" && miniGameMatch.status !== "ended") return undefined;
    if (CHAIN_MODE !== "evm") return undefined;
    const timer = setInterval(() => {
      apiFetch(`/api/minigames/${miniGameMatch.id}/sync`, { method: "POST" })
        .then((res) => {
          if (res?.match) setMiniGameMatch(res.match);
        })
        .catch(() => {});
    }, 5000);
    return () => clearInterval(timer);
  }, [miniGameHubOpen, miniGameMatch?.id, miniGameMatch?.status]);

  useEffect(() => {
    setMentionMenu(null);
    setMentionIndex(0);
  }, [selectedChat?.type, selectedChat?.id]);

  useEffect(() => {
    const injected = listInjectedWallets();
    const availableOptions = [...injected];
    if (WALLETCONNECT_PROJECT_ID) {
      availableOptions.push({ id: "walletconnect", label: "WalletConnect", type: "walletconnect" });
    }
    const autoWalletOption =
      availableOptions.find((option) => option.id === blackjackWalletProviderId) || null;
    if (!autoWalletOption || blackjackWallet || !blackjackPanelOpen) return;
    if (!blackjackMatch?.chain && !blackjackInvite.chain) return;
    connectWalletForChain(blackjackMatch?.chain || blackjackInvite.chain, autoWalletOption, false)
      .then((ctx) => {
        setBlackjackClaimAddress((prev) => prev || ctx.address);
      })
      .catch(() => {});
  }, [blackjackWalletProviderId, blackjackWallet, blackjackPanelOpen, blackjackMatch?.chain, blackjackInvite.chain]);

  useEffect(() => {
    const injected = listInjectedWallets();
    const availableOptions = [...injected];
    if (WALLETCONNECT_PROJECT_ID) {
      availableOptions.push({ id: "walletconnect", label: "WalletConnect", type: "walletconnect" });
    }
    const autoWalletOption =
      availableOptions.find((option) => option.id === blackjackWalletProviderId) || null;
    if (!autoWalletOption || !miniGameHubOpen) return;
    if (!miniGameClaimAddress && !miniGameMatch?.id) return;
    if (!blackjackWallet && (miniGameMatch?.chain || miniGameInvite.chain)) {
      connectWalletForChain(miniGameMatch?.chain || miniGameInvite.chain, autoWalletOption, false)
        .then((ctx) => {
          setMiniGameClaimAddress((prev) => prev || ctx.address);
        })
        .catch(() => {});
    }
  }, [blackjackWalletProviderId, blackjackWallet, miniGameHubOpen, miniGameMatch?.id, miniGameMatch?.chain, miniGameInvite.chain, miniGameClaimAddress]);

  useEffect(() => {
    if (!pttListening) return;
    function handleKey(e) {
      e.preventDefault();
      if (e.key === "Escape") {
        setPttListening(false);
        return;
      }
      const label = e.code || e.key || "";
      if (label) {
        setPttKeybind(label);
        setPttListening(false);
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [pttListening]);

  useEffect(() => {
    if (!selectedChat) return;
    if (selectedChat.type === "dm") {
      apiFetch(`/api/messages/${selectedChat.id}/read`, { method: "POST" }).catch(() => {});
    }
    if (selectedChat.type === "group") {
      apiFetch(`/api/groups/${selectedChat.id}/read`, { method: "POST" }).catch(() => {});
    }
  }, [selectedChat]);

  useEffect(() => {
    function handleKey(e) {
      if (e.key === "Escape") {
        e.preventDefault();
        if (storyDeleteConfirm) {
          setStoryDeleteConfirm(false);
        } else if (storyViewersOpen) {
          setStoryViewersOpen(false);
        } else if (storyMenuOpen) {
          setStoryMenuOpen(false);
        } else if (storyViewer.open) {
          closeStory();
        } else if (forwardModal) {
          setForwardModal(null);
          setForwardTarget(null);
          setForwardNote("");
        } else if (incomingCall) {
          setIncomingCall(null);
        } else if (incomingGroupCall) {
          setIncomingGroupCall(null);
        } else if (dmBgPresetOpen) {
          setDmBgPresetOpen(false);
        } else if (dmBgDirectionOpen) {
          setDmBgDirectionOpen(false);
        } else if (dmBgFitOpen) {
          setDmBgFitOpen(false);
        } else if (dmBgModal) {
          setDmBgModal(null);
        } else if (editGroupConfirmOpen) {
          closeModalWithAnim("editGroupConfirm", () => setEditGroupConfirmOpen(false));
        } else if (editGroupModal) {
          requestCloseEditGroup();
        } else if (addMembersModal) {
          closeModalWithAnim("addMembers", () => setAddMembersModal(null));
        } else if (passwordModalOpen) {
          closeModalWithAnim("password", () => setPasswordModalOpen(false));
        } else if (usernameModalOpen) {
          closeModalWithAnim("username", () => setUsernameModalOpen(false));
        } else if (displayEditOpen) {
          closeModalWithAnim("display", () => setDisplayEditOpen(false));
        } else if (emailEditOpen) {
          closeModalWithAnim("email", () => setEmailEditOpen(false));
        } else if (statusEditOpen) {
          closeModalWithAnim("status", () => setStatusEditOpen(false));
        } else if (avatarEditOpen) {
          closeModalWithAnim("avatar", () => setAvatarEditOpen(false));
        } else if (connectionModalOpen) {
          closeModalWithAnim("connection", () => setConnectionModalOpen(false));
        } else if (usernameManagerOpen) {
          closeModalWithAnim("usernameManager", () => setUsernameManagerOpen(false));
        } else if (primaryModal) {
          setPrimaryModal(null);
          setPrimaryPassword("");
          setPrimaryError("");
        } else if (transferModal) {
          closeModalWithAnim("transfer", () => setTransferModal(null));
        } else if (removeAliasModal) {
          closeModalWithAnim("removeAlias", () => setRemoveAliasModal(null));
        } else if (removeConnectionId) {
          closeModalWithAnim("removeConnection", () => setRemoveConnectionId(null));
        } else if (removeFriendConfirm) {
          closeModalWithAnim("removeFriend", () => setRemoveFriendConfirm(null));
        } else if (leaveGroupConfirm) {
          closeModalWithAnim("leaveGroup", () => setLeaveGroupConfirm(null));
        } else if (nicknameModal) {
          closeModalWithAnim("nickname", () => setNicknameModal(null));
        } else if (profileOpen) {
          closeModalWithAnim("profile", () => setProfileOpen(false));
        } else if (callInputMenuOpen) {
          setCallInputMenuOpen(false);
        } else if (callOutputMenuOpen) {
          setCallOutputMenuOpen(false);
        } else if (callSettingsOpen) {
          setCallSettingsOpen(false);
        } else if (voicePanelOpen) {
          setVoicePanelOpen(false);
        }
        setShowProfileMenu(null);
        setShowMessageMenu(null);
        setReactionPickerFor(null);
        setShowChatMenu(false);
        setGroupMemberMenu(null);
        setGroupContextMenu(null);
        setShowEmojiPicker(false);
        setStatusMenuOpen(false);
      }
    }
    function handleClick() {
      setShowProfileMenu(null);
      setShowMessageMenu(null);
      setReactionPickerFor(null);
      setGroupMemberMenu(null);
      setGroupContextMenu(null);
      setStatusMenuOpen(false);
      setFriendMuteMenu(null);
    }
    document.addEventListener("keydown", handleKey, true);
    window.addEventListener("click", handleClick);
    return () => {
      document.removeEventListener("keydown", handleKey, true);
      window.removeEventListener("click", handleClick);
    };
  }, [
    storyDeleteConfirm,
    storyViewersOpen,
    storyMenuOpen,
    storyViewer.open,
    forwardModal,
    incomingCall,
    incomingGroupCall,
    dmBgModal,
    dmBgPresetOpen,
    dmBgDirectionOpen,
    dmBgFitOpen,
    editGroupConfirmOpen,
    editGroupModal,
    addMembersModal,
    passwordModalOpen,
    usernameModalOpen,
    displayEditOpen,
    emailEditOpen,
    statusEditOpen,
    avatarEditOpen,
    connectionModalOpen,
    usernameManagerOpen,
    primaryModal,
    transferModal,
    removeAliasModal,
    removeConnectionId,
    removeFriendConfirm,
    leaveGroupConfirm,
    nicknameModal,
    profileOpen,
    callInputMenuOpen,
    callOutputMenuOpen,
    callSettingsOpen,
    voicePanelOpen,
  ]);

  async function loadChats() {
    const [chats, friendsRes, outgoing, usersRes] = await Promise.all([
      apiFetch("/api/chats"),
      apiFetch("/api/friends"),
      apiFetch("/api/friends/requests/outgoing").catch(() => ({ requests: [] })),
      apiFetch("/api/users").catch(() => ({ users: [] })),
    ]);
    if (Object.keys(mutedChats).length) {
      const now = Date.now();
      const next = { ...mutedChats };
      let changed = false;
      Object.entries(next).forEach(([key, value]) => {
        if (value !== "forever" && typeof value === "number" && value <= now) {
          delete next[key];
          changed = true;
        }
      });
      if (changed) saveMutedChats(next);
    }
    setAllUsers(usersRes.users || []);
    const statusMap = {};
    (chats.dms || []).forEach((d) => {
      if (d?.id) statusMap[d.id] = d.custom_status || "";
    });
    (chats.groups || []).forEach((g) => {
      (g.members || []).forEach((m) => {
        if (m?.id) statusMap[m.id] = m.custom_status || "";
      });
    });
    setCustomStatusById((prev) => ({ ...prev, ...statusMap }));
    const hidden = getHiddenDms();
    const base = (chats.dms || []).filter((d) => !hidden.includes(d.id));
    const extras = (manualDmUsersRef.current || []).filter((u) => !hidden.includes(u.id));
    const merged = [...base];
    extras.forEach((u) => {
      if (!merged.some((m) => Number(m.id) === Number(u.id))) merged.push(u);
    });
    setFriends(merged);
    setGroups(chats.groups || []);
    setFriendsAll(friendsRes.friends || []);
    setPendingFriendIds((outgoing.requests || []).map((r) => r.recipient_id));
    setRemovedFriendIds((prev) =>
      prev.filter((id) => !(friendsRes.friends || []).some((f) => Number(f.id) === Number(id)))
    );
    if (profileUser?.id) {
      setProfileIsFriend((friendsRes.friends || []).some((f) => Number(f.id) === Number(profileUser.id)));
    }
    if (socketRef.current) {
      const groupIds = (chats.groups || []).map((g) => g.id);
      socketRef.current.emit("groups:join", { groupIds });
    }
  }

  async function loadNotifications() {
    const data = await apiFetch("/api/notifications");
    setNotifications(data.notifications || []);
  }

  async function loadFriendRequests() {
    const data = await apiFetch("/api/friends/requests");
    setRequests(data.requests || []);
  }

  async function loadConnections() {
    const data = await apiFetch("/api/connections");
    setConnections(data.connections || []);
  }

  async function refreshCurrentUser() {
    const data = await apiFetch("/api/me");
    if (!data?.user) return;
    if (data?.csrfToken) setCsrfToken(data.csrfToken);
    setUser(data.user);
    setSettings((prev) => ({
      ...prev,
      username: data.user.username,
      displayName: data.user.display_name || "",
      email: data.user.email || "",
      emailVerified: !!data.user.email_verified,
      avatar: data.user.avatar || "",
      status: data.user.status || "online",
      customStatus: data.user.custom_status || "",
      bio: data.user.bio || "",
      ringtoneUrl: data.user.ringtone_url || "",
      songTitle: data.user.song_title || "",
      songArtist: data.user.song_artist || "",
      songAlbum: data.user.song_album || "",
      songCoverUrl: data.user.song_cover_url || "",
      songAudioUrl: data.user.song_audio_url || "",
      songSource: data.user.song_source || "",
      songSourceUrl: data.user.song_source_url || "",
      aliases: data.user.aliases || [],
    }));
  }

  async function loadUsernames() {
    try {
      const data = await apiFetch("/api/usernames");
      setAliasData(data);
      if (data?.primary?.username) {
        setSettings((prev) => ({ ...prev, aliases: data.aliases || [], username: data.primary.username }));
        setUser((prev) => (prev ? { ...prev, aliases: data.aliases || [], username: data.primary.username } : prev));
      }
    } catch {
      setAliasData(null);
    }
  }

  async function loadGroupMembers(groupId) {
    const data = await apiFetch(`/api/groups/${groupId}/members`);
    setGroups((prev) => prev.map((g) => (g.id === groupId ? { ...g, members: data.members || [] } : g)));
  }

  async function loadStories() {
    try {
      const data = await apiFetch("/api/stories");
      const users = (data.users || []).map((u) => ({
        ...u,
        stories: (u.stories || []).map((s) => ({
          ...s,
          url: s.media_url || s.url,
          type: s.media_type || s.type,
        })),
      }));
      setStories(users);
    } catch {
      setStories([]);
    }
  }

  function markStoryViewedForUser(userId) {
    if (!userId) return;
    setFriends((prev) =>
      prev.map((u) =>
        u.id === userId ? { ...u, has_unviewed_story: false, has_unviewed: false } : u
      )
    );
    setFriendsAll((prev) =>
      prev.map((u) =>
        u.id === userId ? { ...u, has_unviewed_story: false, has_unviewed: false } : u
      )
    );
    setManualDmUsers((prev) =>
      prev.map((u) =>
        u.id === userId ? { ...u, has_unviewed_story: false, has_unviewed: false } : u
      )
    );
    manualDmUsersRef.current = manualDmUsersRef.current.map((u) =>
      u.id === userId ? { ...u, has_unviewed_story: false, has_unviewed: false } : u
    );
  }

  async function viewStory(userIndex, storyIndex) {
    setStoryViewer({ open: true, userIndex, storyIndex });
    setStoryPaused(false);
    setStoryViewersOpen(false);
    setStoryViewers([]);
    const current = stories[userIndex]?.stories?.[storyIndex];
    if (current) {
      await apiFetch(`/api/stories/${current.id}/view`, { method: "POST" }).catch(() => {});
      lastStoryViewedRef.current = current.id;
      markStoryViewedForUser(stories[userIndex]?.user?.id);
      setStories((prev) =>
        prev.map((u, idx) =>
          idx === userIndex
            ? {
                ...u,
                has_unviewed: false,
                has_unviewed_story: false,
                stories: (u.stories || []).map((s, sidx) =>
                  sidx === storyIndex ? { ...s, viewed: true } : s
                ),
              }
            : u
        )
      );
      loadStories();
    }
  }

  function closeStory() {
    if (!storyViewer.open) return;
    setStoryClosing(true);
    setStoryMenuOpen(false);
    setStoryViewersOpen(false);
    setTimeout(() => {
      setStoryViewer((prev) => ({ ...prev, open: false }));
      setStoryClosing(false);
    }, 160);
  }

  async function toggleStoryViewers() {
    const current = stories[storyViewer.userIndex]?.stories?.[storyViewer.storyIndex];
    if (!current) return;
    if (storyViewersOpen) {
      setStoryViewersOpen(false);
      return;
    }
    const res = await apiFetch(`/api/stories/${current.id}/viewers`).catch(() => ({ viewers: [] }));
    setStoryViewers(res.viewers || []);
    setStoryViewersOpen(true);
  }

  function advanceStory() {
    setStoryViewer((prev) => {
      const list = stories[prev.userIndex]?.stories || [];
      if (prev.storyIndex < list.length - 1) {
        setStoryPaused(false);
        return { ...prev, storyIndex: prev.storyIndex + 1 };
      }
      if (prev.userIndex < stories.length - 1) {
        setStoryPaused(false);
        return { ...prev, userIndex: prev.userIndex + 1, storyIndex: 0 };
      }
      closeStory();
      return prev;
    });
  }

  async function uploadStory(file) {
    const formData = new FormData();
    formData.append("story", file);
    try {
      await apiUpload("/api/stories", formData);
      loadStories();
    } catch {
      // ignore upload errors for now
    }
  }

  async function openStoryByUserId(userId) {
    if (!userId) return;
    let idx = stories.findIndex((s) => s.user?.id === userId);
    if (idx === -1) {
      await loadStories();
      idx = stories.findIndex((s) => s.user?.id === userId);
    }
    if (idx !== -1) {
      viewStory(idx, 0);
    }
  }

  async function deleteStory(id) {
    await apiFetch(`/api/stories/${id}`, { method: "DELETE" });
    loadStories();
  }

  function closeModalWithAnim(name, closeFn) {
    setClosingModals((prev) => ({ ...prev, [name]: true }));
    setTimeout(() => {
      closeFn();
      setClosingModals((prev) => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
    }, 160);
  }

  async function ensurePeer(otherId) {
    if (pcRef.current) {
      const currentState = pcRef.current.connectionState;
      const iceState = pcRef.current.iceConnectionState;
      const unusable =
        currentState === "closed" ||
        currentState === "failed" ||
        currentState === "disconnected" ||
        iceState === "closed" ||
        iceState === "failed";
      if (!unusable) return;
      try {
        pcRef.current.close();
      } catch {
        // ignore
      }
      pcRef.current = null;
    }
    const pc = new RTCPeerConnection(STUN_CONFIG);
    pcRef.current = pc;

    pc.onicecandidate = (event) => {
      if (event.candidate && socketRef.current) {
        socketRef.current.emit("call:ice", { toId: otherId, candidate: event.candidate });
      }
    };

    pc.onnegotiationneeded = () => {
      if (callStateRef.current?.withUserId) {
        createOffer(callStateRef.current.withUserId);
      }
    };

    pc.ontrack = (event) => {
      if (event.track.kind === "audio") {
        let stream = remoteStreamRef.current;
        if (!stream) {
          stream = new MediaStream();
          remoteStreamRef.current = stream;
        }
        if (!stream.getTracks().some((t) => t.id === event.track.id)) {
          stream.addTrack(event.track);
        }
        playRemoteAudio(stream);
        setRemotePresent(true);
        setAudioStreamTick((n) => n + 1);
      }
      if (event.track.kind === "video") {
        const stream = new MediaStream([event.track]);
        setDmShareStream(stream);
        const other =
          friendsAll.find((f) => f.id === otherId) ||
          friends.find((f) => f.id === otherId) ||
          manualDmUsers.find((f) => f.id === otherId) ||
          null;
        setScreenShareWindow((prev) => ({
          ...prev,
          open: true,
          label: other ? `Sharing: @${other.username}` : "Screen Share",
        }));
        event.track.onended = () => {
          setDmShareStream(null);
        };
      }
    };

    pc.onconnectionstatechange = () => {
      const state = pc.connectionState;
      if (state === "connected") {
        setCallState((prev) => ({ ...prev, status: "in-call" }));
        return;
      }
      if (state === "disconnected" || state === "failed") {
        if (state === "failed") {
          try {
            pc.restartIce();
          } catch {
            // ignore
          }
        }
        const current = callStateRef.current;
        if (current?.withUserId && current.status !== "idle" && !callEndedRef.current) {
          setCallState((prev) => ({ ...prev, status: "reconnecting" }));
          setTimeout(() => {
            if (!callEndedRef.current && pc.connectionState !== "connected" && callStateRef.current?.withUserId) {
              if (pcRef.current === pc) {
                try {
                  pc.close();
                } catch {
                  // ignore
                }
                pcRef.current = null;
              }
              socketRef.current?.emit("call:rejoin", { toId: callStateRef.current.withUserId });
            }
          }, 1200);
        }
      }
    };

    if (rawMicStreamRef.current) {
      rawMicStreamRef.current.getTracks().forEach((t) => t.stop());
    }
    const mic = await navigator.mediaDevices.getUserMedia({
      audio: getHiFiAudioConstraints(callSettings.inputId),
    });
    rawMicStreamRef.current = mic;
    await rebuildAudioGraph(mic);
    const processed = localStreamRef.current;
    const outTrack = processed?.getAudioTracks?.()[0];
    if (outTrack && processed) {
      const audioSenders = pc.getSenders().filter((s) => s.track && s.track.kind === "audio");
      if (audioSenders.length) {
        await audioSenders[0].replaceTrack(outTrack);
        audioSenders.slice(1).forEach((s) => pc.removeTrack(s));
      } else {
        pc.addTrack(outTrack, processed);
      }
    }
    setAudioStreamTick((n) => n + 1);
  }

  function applyDmBackground(nextBg) {
    if (!dmBgModal) return;
    const next = { ...dmBackgrounds, [dmBgModal]: nextBg };
    setDmBackgrounds(next);
    localStorage.setItem(dmBgKey, JSON.stringify(next));
  }

  function updateDmBgMode(mode) {
    setDmBgMode(mode);
  }

  function handleDmBgColorChange(value) {
    setDmBgColor(value);
    const overlay = overlayFromRgb(parseHexColor(value));
    applyDmBackground({ type: "color", value, overlay });
    setDmBgRecent((prev) => {
      const next = [value, ...prev.filter((c) => c !== value)].slice(0, 6);
      localStorage.setItem(`xp-dm-bg-recent:${user?.id || "guest"}`, JSON.stringify(next));
      return next;
    });
  }

  function handleDmBgGradientChange(value) {
    setDmBgGradient(value);
    applyDmBackground({ type: "gradient", value, overlay: `rgba(255, 255, 255, ${dmBgOpacity})` });
  }

  function handleDmBgImageChange(file) {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const url = reader.result;
      setDmBgImagePreview(url);
      const img = new Image();
      img.onload = () => {
        const canvas = document.createElement("canvas");
        const size = 32;
        canvas.width = size;
        canvas.height = size;
        const ctx = canvas.getContext("2d");
        ctx.drawImage(img, 0, 0, size, size);
        const data = ctx.getImageData(0, 0, size, size).data;
        let r = 0, g = 0, b = 0;
        for (let i = 0; i < data.length; i += 4) {
          r += data[i];
          g += data[i + 1];
          b += data[i + 2];
        }
        const count = data.length / 4;
        r = Math.round(r / count);
        g = Math.round(g / count);
        b = Math.round(b / count);
        const overlay = `rgba(${r}, ${g}, ${b}, ${dmBgOpacity})`;
        applyDmBackground({ type: "image", value: url, overlay, fit: dmBgFit });
      };
      img.src = url;
    };
    reader.readAsDataURL(file);
  }

  async function uploadRingtone(file) {
    if (!file) return;
    setRingtoneError("");
    if (file.type !== "audio/mpeg" && file.type !== "audio/mp3") {
      setRingtoneError("MP3 only");
      return;
    }
    if (file.size > 2 * 1024 * 1024) {
      setRingtoneError("Max size is 2MB");
      return;
    }
    const url = URL.createObjectURL(file);
    const audio = new Audio(url);
    const duration = await new Promise((resolve) => {
      audio.onloadedmetadata = () => resolve(audio.duration || 0);
      audio.onerror = () => resolve(0);
    });
    URL.revokeObjectURL(url);
    if (duration > 15) {
      setRingtoneError("Max duration is 15 seconds");
      return;
    }
    const formData = new FormData();
    formData.append("ringtone", file);
    try {
      const data = await apiUpload("/api/uploads/ringtone", formData);
      setSettings((prev) => ({ ...prev, ringtoneUrl: data.url }));
    } catch (err) {
      setRingtoneError(err.message);
    }
  }

  function backToList() {
    setSelectedChat(null);
    setShowChatProfile(false);
    setShowChatMenu(false);
    setMobileView("list");
  }

  function startCallDrag(e) {
    e.preventDefault();
    callDragRef.current.dragging = true;
    callDragRef.current.startX = e.clientX;
    callDragRef.current.startY = e.clientY;
    callDragRef.current.x = callWindowPos.x;
    callDragRef.current.y = callWindowPos.y;
    window.addEventListener("mousemove", onCallDragMove);
    window.addEventListener("mouseup", stopCallDrag);
  }

  function onCallDragMove(e) {
    if (!callDragRef.current.dragging) return;
    const dx = e.clientX - callDragRef.current.startX;
    const dy = e.clientY - callDragRef.current.startY;
    callDragRef.current.pendingX = callDragRef.current.x + dx;
    callDragRef.current.pendingY = callDragRef.current.y + dy;
    if (!callDragRafRef.current) {
      callDragRafRef.current = requestAnimationFrame(() => {
        setCallWindowPos({ x: callDragRef.current.pendingX, y: callDragRef.current.pendingY });
        callDragRafRef.current = null;
      });
    }
  }

  function stopCallDrag() {
    callDragRef.current.dragging = false;
    window.removeEventListener("mousemove", onCallDragMove);
    window.removeEventListener("mouseup", stopCallDrag);
  }

  async function createOffer(otherId) {
    if (!pcRef.current || !socketRef.current) return;
    if (pcRef.current.signalingState !== "stable") return;
    if (pcRef.current.localDescription?.type === "offer") return;
    const offer = await pcRef.current.createOffer();
    await pcRef.current.setLocalDescription(offer);
    socketRef.current.emit("call:offer", { toId: otherId, offer });
  }

  // (sidebar resize removed)

  async function startCall(otherId) {
    callEndedRef.current = false;
    await ensurePeer(otherId);
    socketRef.current?.emit("call:request", { toId: otherId });
    setCallState({ status: "calling", withUserId: otherId, startTime: null, muted: false, deafened: false });
    setCallAudioStates({});
  }

  function joinCall(otherId) {
    if (!otherId) return;
    callEndedRef.current = false;
    setJoiningCall(true);
    socketRef.current?.emit("call:rejoin", { toId: otherId });
  }

  function emitTypingStop() {
    if (!selectedChat || selectedChat.type !== "dm") return;
    socketRef.current?.emit("dm:typing", { toId: selectedChat.id, isTyping: false });
  }

  async function acceptCall() {
    if (!incomingCall) return;
    const fromId = incomingCall.fromId;
    callEndedRef.current = false;
    await ensurePeer(fromId);
    socketRef.current?.emit("call:accept", { toId: fromId });
    setIncomingCall(null);
  }

  function declineCall() {
    if (!incomingCall) return;
    socketRef.current?.emit("call:decline", { toId: incomingCall.fromId });
    setIncomingCall(null);
    setCallState({ status: "idle", withUserId: null, startTime: null, muted: false, deafened: false });
    setCallAudioStates({});
    teardownPeer();
  }

  function cancelOutgoingCall() {
    if (callState.status !== "calling" || !callState.withUserId) return;
    socketRef.current?.emit("call:end", { toId: callState.withUserId });
    callEndedRef.current = true;
    setCallState({ status: "idle", withUserId: null, startTime: null, muted: false, deafened: false });
    setCallAudioStates({});
    teardownPeer();
  }

  function endCall() {
    if (callState.withUserId) {
      socketRef.current?.emit("call:leave", { toId: callState.withUserId });
    }
    playCallChime("leave");
    callEndedRef.current = true;
    setCallState((prev) => ({ ...prev, status: "active" }));
    teardownPeer();
  }

  function applyOutgoingAudioState(muted, deafened) {
    const shouldMute = !!muted || !!deafened;
    const tracks = localStreamRef.current?.getAudioTracks?.() || [];
    tracks.forEach((t) => {
      t.enabled = !shouldMute;
    });
    if (micGainNodeRef.current) {
      micGainNodeRef.current.gain.value = shouldMute ? 0 : (callSettings.micVolume ?? 1);
    }
  }

  function applyIncomingAudioState(deafened) {
    const remoteAudio = document.getElementById("xp-remote-audio");
    if (remoteAudio) {
      remoteAudio.muted = !!deafened;
      remoteAudio.volume = callSettings.speakerVolume ?? 1;
    }
    document.querySelectorAll("[id^='xp-group-audio-']").forEach((node) => {
      node.muted = !!deafened;
      node.volume = callSettings.speakerVolume ?? 1;
    });
  }

  function toggleMute() {
    const nextMuted = !callState.muted;
    applyOutgoingAudioState(nextMuted, callState.deafened);
    setCallState((prev) => ({ ...prev, muted: nextMuted }));
  }

  function toggleDeafen() {
    const nextDeafened = !callState.deafened;
    applyOutgoingAudioState(callState.muted, nextDeafened);
    applyIncomingAudioState(nextDeafened);
    setCallState((prev) => ({ ...prev, deafened: nextDeafened }));
  }

  function emitDirectCallStatus(nextState = callStateRef.current) {
    const otherId = nextState?.withUserId;
    if (!otherId) return;
    if (nextState.status !== "in-call" && nextState.status !== "active" && nextState.status !== "reconnecting") return;
    socketRef.current?.emit("call:status", {
      toId: otherId,
      muted: !!nextState.muted,
      deafened: !!nextState.deafened,
    });
    setCallAudioStates((prev) => ({
      ...prev,
      [user?.id]: { muted: !!nextState.muted, deafened: !!nextState.deafened },
    }));
  }

  function emitGroupCallStatus(nextStatus) {
    if (!groupCall.groupId || groupCall.status !== "in-call") return;
    socketRef.current?.emit("group:call:status", {
      groupId: groupCall.groupId,
      muted: !!nextStatus.muted,
      deafened: !!nextStatus.deafened,
    });
    setGroupCallAudioStates((prev) => ({
      ...prev,
      [user?.id]: { muted: !!nextStatus.muted, deafened: !!nextStatus.deafened },
    }));
  }

  useEffect(() => {
    applyOutgoingAudioState(callState.muted, callState.deafened);
    applyIncomingAudioState(callState.deafened);
  }, [callState.muted, callState.deafened, callSettings.micVolume, callSettings.speakerVolume]);

  useEffect(() => {
    if (!user?.id) return;
    if (callState.withUserId && (callState.status === "in-call" || callState.status === "active" || callState.status === "reconnecting")) {
      emitDirectCallStatus(callState);
    }
    if (groupCall.groupId && groupCall.status === "in-call") {
      emitGroupCallStatus(callState);
    }
  }, [
    callState.muted,
    callState.deafened,
    callState.status,
    callState.withUserId,
    groupCall.groupId,
    groupCall.status,
    user?.id,
  ]);

  useEffect(() => {
    if (!callState.withUserId || callState.status !== "in-call" || !pcRef.current) {
      setCallQuality({ label: "Good", level: "good" });
      return;
    }
    let mounted = true;
    const interval = setInterval(async () => {
      if (!pcRef.current) return;
      const stats = await pcRef.current.getStats();
      let rtt = 0;
      let packetsLost = 0;
      let packetsReceived = 0;
      stats.forEach((report) => {
        if (report.type === "candidate-pair" && report.currentRoundTripTime) {
          rtt = Math.max(rtt, report.currentRoundTripTime);
        }
        if (report.type === "inbound-rtp" && report.kind === "audio") {
          packetsLost += report.packetsLost || 0;
          packetsReceived += report.packetsReceived || 0;
        }
      });
      const loss = packetsReceived + packetsLost > 0 ? packetsLost / (packetsReceived + packetsLost) : 0;
      let level = "good";
      let label = "Good";
      if (rtt > 0.35 || loss > 0.08) {
        level = "poor";
        label = "Poor";
      } else if (rtt > 0.2 || loss > 0.04) {
        level = "fair";
        label = "Fair";
      }
      if (mounted) setCallQuality({ label, level });
    }, 2000);
    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [callState.status, callState.withUserId]);

  useEffect(() => {
    if (!callState.withUserId || (callState.status !== "in-call" && callState.status !== "active")) return;
    const inputId = callSettings.inputId || "";
    const needsNewStream =
      !rawMicStreamRef.current ||
      lastInputIdRef.current !== inputId ||
      lastEchoRef.current !== echoCancel;
    lastInputIdRef.current = inputId;
    lastEchoRef.current = echoCancel;
    if (needsNewStream || !micGainCtxRef.current) {
      applyInputDevice(inputId);
      lastVoiceEffectRef.current = voiceEffect;
      lastPitchRef.current = pitchShift;
      return;
    }
    const ctx = micGainCtxRef.current;
    if (micGainNodeRef.current) {
      micGainNodeRef.current.gain.setTargetAtTime(callSettings.micVolume ?? 1, ctx.currentTime, 0.04);
    }
    if (micLowRef.current) micLowRef.current.gain.setTargetAtTime(eqLow, ctx.currentTime, 0.04);
    if (micMidRef.current) micMidRef.current.gain.setTargetAtTime(eqMid, ctx.currentTime, 0.04);
    if (micHighRef.current) micHighRef.current.gain.setTargetAtTime(eqHigh, ctx.currentTime, 0.04);

    const effectChanged = lastVoiceEffectRef.current !== voiceEffect;
    const pitchChanged = lastPitchRef.current !== pitchShift;
    lastVoiceEffectRef.current = voiceEffect;
    lastPitchRef.current = pitchShift;

    if (voiceEffect === "pitch" && micPitchParamRef.current && pitchChanged) {
      const safePitch = Math.max(-10, Math.min(10, pitchShift));
      const rate = Math.pow(2, safePitch / 12);
      micPitchParamRef.current.setValueAtTime(rate, ctx.currentTime);
      return;
    }
    if (effectChanged || (voiceEffect === "pitch" && pitchChanged && !micPitchParamRef.current)) {
      rebuildAudioGraph(rawMicStreamRef.current);
    }
  }, [voiceEffect, pitchShift, eqLow, eqMid, eqHigh, echoCancel, callState.status, callState.withUserId]);

  useEffect(() => {
    if (!pushToTalk || (callState.status !== "in-call" && callState.status !== "active")) return;
    const tracks = localStreamRef.current?.getAudioTracks?.() || [];
    if (!pttActive) {
      tracks.forEach((t) => (t.enabled = false));
      setCallState((prev) => ({ ...prev, muted: true }));
    } else {
      tracks.forEach((t) => (t.enabled = true));
      setCallState((prev) => ({ ...prev, muted: false }));
    }
  }, [pushToTalk, pttActive, callState.status]);

  useEffect(() => {
    if (!pushToTalk || (callState.status !== "in-call" && callState.status !== "active")) return;
    function matchKey(e) {
      if (!pttKeybind) return false;
      return e.code === pttKeybind || e.key === pttKeybind;
    }
    function onDown(e) {
      if (matchKey(e)) {
        setPttActive(true);
      }
    }
    function onUp(e) {
      if (matchKey(e)) {
        setPttActive(false);
      }
    }
    window.addEventListener("keydown", onDown);
    window.addEventListener("keyup", onUp);
    return () => {
      window.removeEventListener("keydown", onDown);
      window.removeEventListener("keyup", onUp);
    };
  }, [pushToTalk, pttKeybind, callState.status]);

  function playRemoteAudio(stream) {
    const audio = document.getElementById("xp-remote-audio");
    if (!audio) return;
    audio.srcObject = stream;
    audio.autoplay = true;
    audio.playsInline = true;
    audio.muted = !!callStateRef.current?.deafened;
    audio.volume = callSettings.speakerVolume ?? 1;
    audio.play().catch(() => {});
  }

  function teardownPeer() {
    micBuildSeqRef.current += 1;
    if (pcRef.current) {
      pcRef.current.close();
      pcRef.current = null;
    }
    localStreamRef.current?.getTracks().forEach((t) => t.stop());
    localStreamRef.current = null;
    rawMicStreamRef.current?.getTracks().forEach((t) => t.stop());
    rawMicStreamRef.current = null;
    micGainCtxRef.current?.close?.().catch?.(() => {});
    micGainCtxRef.current = null;
    dmLocalScreenRef.current?.getTracks().forEach((t) => t.stop());
    dmLocalScreenRef.current = null;
    dmScreenVideoSenderRef.current = null;
    dmScreenAudioSenderRef.current = null;
    setDmShareStream(null);
    setScreenShareError("");
    setScreenShareWindow((prev) => ({ ...prev, open: false }));
    setDmShareActive(false);
    remoteStreamRef.current = null;
    const audio = document.getElementById("xp-remote-audio");
    if (audio) {
      audio.pause?.();
      audio.srcObject = null;
    }
  }

  async function startGroupCall(groupId) {
    socketRef.current?.emit("group:call:start", { groupId });
    socketRef.current?.emit("group:call:join", { groupId });
    setGroupCall({ groupId, status: "ringing", participants: [], startedAt: Date.now() });
    setGroupCallVisible(true);
  }

  async function joinGroupCall(groupId) {
    socketRef.current?.emit("group:call:join", { groupId });
  }

  async function leaveGroupCall() {
    if (!groupCall.groupId) return;
    socketRef.current?.emit("group:call:leave", { groupId: groupCall.groupId });
    cleanupGroupCall();
    setGroupCallVisible(false);
  }

  async function setupGroupPeer(groupId, peerId) {
    if (groupPeersRef.current.has(peerId)) return;
    const pc = new RTCPeerConnection(STUN_CONFIG);
    groupPeersRef.current.set(peerId, { pc, audioStream: null });

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        socketRef.current?.emit("group:call:ice", { groupId, toId: peerId, candidate: event.candidate });
      }
    };

    pc.onnegotiationneeded = async () => {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      socketRef.current?.emit("group:call:offer", { groupId, toId: peerId, offer });
    };

    pc.ontrack = (event) => {
      if (event.track.kind === "video") {
        const stream = new MediaStream([event.track]);
        setGroupShares((prev) => ({ ...prev, [peerId]: stream }));
        const member = selectedGroup?.members?.find((m) => m.id === peerId);
        const label = member ? `Sharing: @${member.username}` : `Sharing: @${peerId}`;
        setScreenShareWindow((prev) => ({ ...prev, open: true, label }));
      }
      if (event.track.kind === "audio") {
        const entry = groupPeersRef.current.get(peerId);
        let stream = entry?.audioStream;
        if (!stream) {
          stream = new MediaStream();
          if (entry) entry.audioStream = stream;
        }
        if (stream && !stream.getTracks().some((t) => t.id === event.track.id)) {
          stream.addTrack(event.track);
        }
        const audio = document.getElementById(`xp-group-audio-${peerId}`);
        if (audio && stream) {
          audio.srcObject = stream;
          audio.muted = !!callStateRef.current?.deafened;
          audio.volume = callSettings.speakerVolume ?? 1;
          audio.play().catch(() => {});
        }
      }
    };

    await ensureProcessedMic();
    groupLocalStreamRef.current = localStreamRef.current;
    const outTrack = groupLocalStreamRef.current?.getAudioTracks?.()[0];
    if (outTrack && groupLocalStreamRef.current) {
      const audioSenders = pc.getSenders().filter((s) => s.track && s.track.kind === "audio");
      if (audioSenders.length) {
        await audioSenders[0].replaceTrack(outTrack);
        audioSenders.slice(1).forEach((s) => pc.removeTrack(s));
      } else {
        pc.addTrack(outTrack, groupLocalStreamRef.current);
      }
    }

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    socketRef.current?.emit("group:call:offer", { groupId, toId: peerId, offer });
  }

  function cleanupGroupPeer(peerId) {
    const entry = groupPeersRef.current.get(peerId);
    if (entry?.pc) {
      entry.pc.close();
    }
    if (entry?.audioStream) {
      entry.audioStream.getTracks().forEach((t) => t.stop());
    }
    groupPeersRef.current.delete(peerId);
    setGroupShares((prev) => {
      const next = { ...prev };
      delete next[peerId];
      return next;
    });
  }

  function cleanupGroupCall() {
    groupPeersRef.current.forEach((entry) => entry.pc.close());
    groupPeersRef.current.clear();
    if (groupLocalStreamRef.current && groupLocalStreamRef.current !== localStreamRef.current) {
      groupLocalStreamRef.current.getTracks().forEach((t) => t.stop());
    }
    groupLocalStreamRef.current = null;
    groupLocalScreenRef.current?.getTracks().forEach((t) => t.stop());
    groupLocalScreenRef.current = null;
    setGroupCall({ groupId: null, status: "idle", participants: [], startedAt: null });
    setGroupCallAudioStates({});
    setGroupShares({});
    setGroupCallVisible(false);
    setScreenShareError("");
  }

  async function startGroupScreenShare() {
    if (!groupCall.groupId) return;
    try {
      const display = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
      setScreenShareError("");
      groupLocalScreenRef.current = display;
      const track = display.getVideoTracks()[0];
      if (track) track.contentHint = "detail";
      const screenAudio = display.getAudioTracks()[0];
      groupPeersRef.current.forEach(async (entry, peerId) => {
        const sender = entry.pc.getSenders().find((s) => s.track && s.track.kind === "video");
        if (sender) {
          await sender.replaceTrack(track);
        } else {
          entry.pc.addTrack(track, display);
        }
        if (screenAudio) {
          const audioSender = entry.pc.getSenders().find(
            (s) => s.track && s.track.kind === "audio" && s.track.label === screenAudio.label
          );
          if (!audioSender) {
            entry.pc.addTrack(screenAudio, display);
          }
        }
        const offer = await entry.pc.createOffer();
        await entry.pc.setLocalDescription(offer);
        socketRef.current?.emit("group:call:offer", { groupId: groupCall.groupId, toId: peerId, offer });
      });
      setScreenShareWindow((prev) => ({ ...prev, open: true, label: "Sharing: You" }));
      track.onended = () => stopGroupScreenShare();
    } catch {
      setScreenShareError("Screen share permission denied or unavailable.");
    }
  }

  async function stopGroupScreenShare() {
    groupLocalScreenRef.current?.getTracks().forEach((t) => t.stop());
    groupLocalScreenRef.current = null;
    setScreenShareWindow((prev) => ({ ...prev, open: false }));
    groupPeersRef.current.forEach(async (entry, peerId) => {
      if (!entry?.pc) return;
      const offer = await entry.pc.createOffer();
      await entry.pc.setLocalDescription(offer);
      socketRef.current?.emit("group:call:offer", { groupId: groupCall.groupId, toId: peerId, offer });
    });
  }

  async function startDmScreenShare() {
    if (!callState.withUserId) return;
    try {
      const display = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
      setScreenShareError("");
      dmLocalScreenRef.current = display;
      setDmShareActive(true);
      const videoTrack = display.getVideoTracks()[0];
      if (videoTrack) {
        videoTrack.contentHint = "detail";
        if (pcRef.current) {
          let sender = dmScreenVideoSenderRef.current || pcRef.current.getSenders().find((s) => s.track?.kind === "video");
          if (sender) {
            await sender.replaceTrack(videoTrack);
          } else {
            sender = pcRef.current.addTrack(videoTrack, display);
          }
          dmScreenVideoSenderRef.current = sender;
        }
      }
      const audioTrack = display.getAudioTracks()[0];
      if (audioTrack && pcRef.current) {
        let sender = dmScreenAudioSenderRef.current;
        if (sender) {
          await sender.replaceTrack(audioTrack);
        } else {
          sender = pcRef.current.addTrack(audioTrack, display);
        }
        dmScreenAudioSenderRef.current = sender;
      }
      setScreenShareWindow((prev) => ({ ...prev, open: true, label: "Sharing: You" }));
      videoTrack.onended = () => stopDmScreenShare();
      if (pcRef.current) {
        await createOffer(callState.withUserId);
      }
    } catch {
      setScreenShareError("Screen share permission denied or unavailable.");
    }
  }

  function stopDmScreenShare() {
    dmLocalScreenRef.current?.getTracks().forEach((t) => t.stop());
    dmLocalScreenRef.current = null;
    setDmShareActive(false);
    if (pcRef.current && dmScreenVideoSenderRef.current) {
      pcRef.current.removeTrack(dmScreenVideoSenderRef.current);
    }
    if (pcRef.current && dmScreenAudioSenderRef.current) {
      pcRef.current.removeTrack(dmScreenAudioSenderRef.current);
    }
    dmScreenVideoSenderRef.current = null;
    dmScreenAudioSenderRef.current = null;
    setScreenShareWindow((prev) => ({ ...prev, open: false }));
    if (callState.withUserId) {
      createOffer(callState.withUserId);
    }
  }

  async function loadProfile(userId) {
    if (!userId) return;
    const data = await apiFetch("/api/users");
    const u = (data.users || []).find((row) => row.id === userId);
    if (!u) return;
    setProfileUser(u);
    setProfileOpen(true);
    const note = await apiFetch(`/api/notes/${userId}`).catch(() => ({ note: "" }));
    setProfileNote(note.note || "");
    const con = await apiFetch(`/api/connections/${userId}`).catch(() => ({ connections: [] }));
    setProfileConnections((con.connections || []).filter((c) => c.visibility !== "hidden"));
    const friendsRes = await apiFetch("/api/friends").catch(() => ({ friends: [] }));
    setProfileIsFriend((friendsRes.friends || []).some((f) => Number(f.id) === Number(userId)));
    if (u?.id) {
      setCustomStatusById((prev) => ({ ...prev, [u.id]: u.custom_status || "" }));
    }
    try {
      const outgoing = await apiFetch("/api/friends/requests/outgoing");
      setPendingFriendIds((outgoing.requests || []).map((r) => r.recipient_id));
    } catch {
      // keep existing pending state if endpoint fails
    }
  }

  async function loadProfileByAlias(alias) {
    const data = await apiFetch("/api/users");
    const u = (data.users || []).find((row) =>
      row.username?.toLowerCase() === alias.toLowerCase() ||
      (row.aliases || []).some((a) => a.toLowerCase() === alias.toLowerCase())
    );
    if (u) {
      loadProfile(u.id);
    }
  }

  async function handleAuth(event) {
    event.preventDefault();
    setAuthError("");
    if (authLoading) return;
    setAuthLoading(true);
    try {
      if (authMode === "register") {
        const res = await apiFetch("/api/register", { method: "POST", body: JSON.stringify(form) });
        if (res?.csrfToken) setCsrfToken(res.csrfToken);
      } else {
        const res = await apiFetch("/api/login", { method: "POST", body: JSON.stringify(form) });
        if (res?.csrfToken) setCsrfToken(res.csrfToken);
      }
      const data = await apiFetch("/api/me");
      if (data?.csrfToken) setCsrfToken(data.csrfToken);
      setUser(data.user);
      setSettings({
        username: data.user.username,
        displayName: data.user.display_name || "",
        email: data.user.email || "",
        emailVerified: !!data.user.email_verified,
        avatar: data.user.avatar || "",
        status: data.user.status || "online",
        customStatus: data.user.custom_status || "",
        bio: data.user.bio || "",
        ringtoneUrl: data.user.ringtone_url || "",
        songTitle: data.user.song_title || "",
        songArtist: data.user.song_artist || "",
        songAlbum: data.user.song_album || "",
        songCoverUrl: data.user.song_cover_url || "",
        songAudioUrl: data.user.song_audio_url || "",
        songSource: data.user.song_source || "",
        songSourceUrl: data.user.song_source_url || "",
        aliases: data.user.aliases || [],
      });
      setMusicDraft({
        source: data.user.song_source || "spotify",
        sourceUrl: data.user.song_source_url || "",
        title: data.user.song_title || "",
        artist: data.user.song_artist || "",
        album: data.user.song_album || "",
        coverUrl: data.user.song_cover_url || "",
        audioUrl: data.user.song_audio_url || "",
      });
      setCustomStatusInput(data.user.custom_status || "");
      setAboutInput(data.user.bio || "");
      setForm({ username: "", password: "", email: "" });
    } catch (err) {
      setAuthError(err.message);
    } finally {
      setTimeout(() => setAuthLoading(false), 300);
    }
  }

  async function handleLogout() {
    if (logoutLoading) return;
    setLogoutLoading(true);
    await apiFetch("/api/logout", { method: "POST" }).catch(() => {});
    setCsrfToken("");
    setUser(null);
    setProfileSongPlayer({
      ownerId: null,
      title: "",
      artist: "",
      album: "",
      coverUrl: "",
      audioUrl: "",
      playing: false,
    });
    if (profileSongAudioRef.current) {
      profileSongAudioRef.current.pause();
      profileSongAudioRef.current.currentTime = 0;
      profileSongAudioRef.current.removeAttribute("src");
    }
    setView("chat");
    setFriends([]);
    setGroups([]);
    setSelectedChat(null);
    setMessagesByChat({});
    setTimeout(() => setLogoutLoading(false), 300);
  }

  async function selectDm(userId) {
    setChatLoading(true);
    setSelectedChat({ type: "dm", id: userId });
    setShowChatProfile(true);
    if (isMobile) setMobileView("chat");
    clearChatUnread(chatKey("dm", userId));
    setShowChatMenu(false);
    const key = chatKey("dm", userId);
    if (!messagesByChat[key]) {
      try {
        const data = await apiFetch(`/api/messages/${userId}`);
        setMessagesByChat((prev) => ({ ...prev, [key]: data.messages || [] }));
      } catch {
        // keep selected chat even if fetch fails
      }
    }
    setTimeout(() => setChatLoading(false), 180);
  }

  async function openDmFromProfile(target) {
    if (!target?.id) return;
    const uid = Number(target.id);
    const hidden = getHiddenDms().filter((id) => Number(id) !== uid);
    localStorage.setItem(`xp-hidden-dms:${user?.id}`, JSON.stringify(hidden));
    ensureFriendCard(target);
    setView("chat");
    setShowChatMenu(false);
    if (isMobile) setMobileView("chat");
    await selectDm(uid);
  }

  async function selectGroup(groupId) {
    setChatLoading(true);
    setSelectedChat({ type: "group", id: groupId });
    if (isMobile) setMobileView("chat");
    clearChatUnread(chatKey("group", groupId));
    setShowChatMenu(false);
    const key = chatKey("group", groupId);
    if (!messagesByChat[key]) {
      const data = await apiFetch(`/api/groups/${groupId}/messages`);
      setMessagesByChat((prev) => ({ ...prev, [key]: data.messages || [] }));
    }
    loadGroupMembers(groupId);
    setTimeout(() => setChatLoading(false), 180);
  }

  function handleTyping() {
    if (!selectedChat || selectedChat.type !== "dm") return;
    if (!socketRef.current) return;
    socketRef.current.emit("dm:typing", { toId: selectedChat.id, isTyping: true });
    if (typingTimeoutRef.current) clearTimeout(typingTimeoutRef.current);
    typingTimeoutRef.current = setTimeout(() => {
      socketRef.current?.emit("dm:typing", { toId: selectedChat.id, isTyping: false });
    }, 1200);
  }

  function addOptimisticMessage(payload) {
    if (!selectedChat || !user) return;
    const now = new Date().toISOString();
    const tempId = `temp-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const base = {
      id: tempId,
      sender_id: user.id,
      created_at: now,
      is_system: false,
      pending: true,
      ...payload,
    };
    const key = chatKey(selectedChat.type, selectedChat.id);
    const message =
      selectedChat.type === "dm"
        ? { ...base, recipient_id: selectedChat.id }
        : { ...base, group_id: selectedChat.id };
    setMessagesByChat((prev) => ({
      ...prev,
      [key]: [...(prev[key] || []), message],
    }));
  }

  async function sendMessage() {
    if (!selectedChat || !socketRef.current) return;
    const trimmed = messageInput.trim();
    if (!trimmed) return;
    setMessageError("");
    if (selectedChat.type === "dm") {
      socketRef.current.emit("dm:send", { toId: selectedChat.id, body: trimmed, type: "text" });
    } else {
      socketRef.current.emit("group:send", { groupId: selectedChat.id, body: trimmed, type: "text" });
    }
    addOptimisticMessage({ type: "text", body: trimmed });
    setSentPulseChat(chatKey(selectedChat.type, selectedChat.id));
    setTimeout(() => setSentPulseChat(null), 500);
    setMessageInput("");
    setMentionMenu(null);
    setMentionIndex(0);
    requestAnimationFrame(() => {
      if (chatBodyRef.current) {
        chatBodyRef.current.scrollTop = chatBodyRef.current.scrollHeight;
      }
    });
  }

  function updateMentionMenu(value, caretPos = value.length) {
    const beforeCaret = value.slice(0, caretPos);
    const match = beforeCaret.match(/(^|\s)@([a-zA-Z0-9_]*)$/);
    if (!match || !selectedChat) {
      setMentionMenu(null);
      setMentionIndex(0);
      return;
    }
    const query = (match[2] || "").toLowerCase();
    const triggerIndex = beforeCaret.lastIndexOf("@");
    const users = getMessageContextUsers(selectedChat.type === "group")
      .filter(Boolean)
      .filter((person, idx, arr) => arr.findIndex((entry) => Number(entry.id) === Number(person.id)) === idx)
      .filter((person) => {
        const username = String(person.username || "").toLowerCase();
        const display = String(person.display_name || "").toLowerCase();
        return !query || username.includes(query) || display.includes(query);
      })
      .slice(0, 8);
    if (!users.length) {
      setMentionMenu(null);
      setMentionIndex(0);
      return;
    }
    setMentionMenu({
      query,
      start: triggerIndex,
      end: caretPos,
      users,
    });
    setMentionIndex((prev) => Math.min(prev, users.length - 1));
  }

  function applyMention(userEntry) {
    if (!mentionMenu || !userEntry) return;
    const nextValue =
      `${messageInput.slice(0, mentionMenu.start)}@${userEntry.username} ${messageInput.slice(mentionMenu.end)}`;
    const nextCaret = mentionMenu.start + userEntry.username.length + 2;
    setMessageInput(nextValue);
    setMentionMenu(null);
    setMentionIndex(0);
    requestAnimationFrame(() => {
      if (messageInputRef.current) {
        messageInputRef.current.focus();
        messageInputRef.current.setSelectionRange(nextCaret, nextCaret);
      }
    });
  }

  async function sendImage(file) {
    if (!selectedChat || !file) return;
    const formData = new FormData();
    formData.append("image", file);
    let data;
    try {
      data = await apiUpload("/api/uploads", formData);
    } catch (err) {
      setMessageError(err.message);
      return;
    }
    const imageUrl = data.url;
    if (selectedChat.type === "dm") {
      socketRef.current?.emit("dm:send", { toId: selectedChat.id, type: "image", imageUrl, body: "" });
    } else {
      socketRef.current?.emit("group:send", { groupId: selectedChat.id, type: "image", imageUrl, body: "" });
    }
    addOptimisticMessage({ type: "image", image_url: imageUrl, body: "" });
    clearImagePreview();
    setSentPulseChat(chatKey(selectedChat.type, selectedChat.id));
    setTimeout(() => setSentPulseChat(null), 500);
  }

  function setImagePreviewFromFile(file) {
    if (!file) return;
    const url = URL.createObjectURL(file);
    setImagePreview({ file, url, name: file.name });
  }

  function clearImagePreview() {
    if (imagePreview?.url) {
      URL.revokeObjectURL(imagePreview.url);
    }
    setImagePreview(null);
  }

  async function sendAudio(file) {
    if (!selectedChat || !file) return;
    setMessageError("");
    const formData = new FormData();
    formData.append("audio", file);
    let data;
    try {
      data = await apiUpload("/api/uploads/audio", formData);
    } catch (err) {
      setMessageError(err.message);
      return;
    }
    const audioUrl = data.url;
    if (selectedChat.type === "dm") {
      socketRef.current?.emit("dm:send", { toId: selectedChat.id, type: "audio", audioUrl, body: "" });
    } else {
      socketRef.current?.emit("group:send", { groupId: selectedChat.id, type: "audio", audioUrl, body: "" });
    }
    addOptimisticMessage({ type: "audio", audio_url: audioUrl, body: "" });
    clearAudioPreview();
    setSentPulseChat(chatKey(selectedChat.type, selectedChat.id));
    setTimeout(() => setSentPulseChat(null), 500);
  }

  function handlePasteImage(event) {
    const items = event.clipboardData?.items;
    if (!items) return;
    for (const item of items) {
      if (item.type && item.type.startsWith("image/")) {
        const file = item.getAsFile();
        if (file) {
          event.preventDefault();
          setImagePreviewFromFile(file);
        }
        break;
      }
    }
  }

  function setAudioPreviewFromFile(file) {
    if (!file) return;
    const url = URL.createObjectURL(file);
    setAudioPreview({ file, url, name: file.name });
  }

  function clearAudioPreview() {
    if (audioPreview?.url) {
      URL.revokeObjectURL(audioPreview.url);
    }
    setAudioPreview(null);
  }

  function getRecordingMimeType() {
    const candidates = [
      "audio/webm;codecs=opus",
      "audio/webm",
      "audio/ogg;codecs=opus",
      "audio/ogg",
    ];
    for (const type of candidates) {
      if (window.MediaRecorder?.isTypeSupported?.(type)) return type;
    }
    return "";
  }

  async function startRecording() {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      recordStreamRef.current = stream;
      const mimeType = getRecordingMimeType();
      const recorder = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
      recordChunksRef.current = [];
      recordCancelRef.current = false;
      setRecordElapsed(0);
      if (recordTimerRef.current) clearInterval(recordTimerRef.current);
      recordTimerRef.current = setInterval(() => {
        setRecordElapsed((prev) => prev + 1);
      }, 1000);
      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0) recordChunksRef.current.push(e.data);
      };
      recorder.onstop = () => {
        if (recordTimerRef.current) {
          clearInterval(recordTimerRef.current);
          recordTimerRef.current = null;
        }
        const blob = new Blob(recordChunksRef.current, { type: "audio/webm" });
        if (!recordCancelRef.current) {
          if (!blob.size) {
            setMessageError("No audio captured");
          } else {
            const file = new File([blob], `voice-${Date.now()}.webm`, { type: blob.type });
            sendAudio(file);
          }
        }
        recordChunksRef.current = [];
        recordStreamRef.current?.getTracks().forEach((t) => t.stop());
        recordStreamRef.current = null;
      };
      recorder.start(250);
      recorderRef.current = recorder;
      setIsRecording(true);
      setRecordSlideOffset(0);
    } catch (err) {
      setMessageError("Microphone access failed");
      setIsRecording(false);
    }
  }

  function stopRecording() {
    if (recorderRef.current && recorderRef.current.state !== "inactive") {
      recorderRef.current.stop();
    }
    setIsRecording(false);
    setRecordSlideOffset(0);
  }

  function cancelRecording() {
    recordCancelRef.current = true;
    if (recorderRef.current && recorderRef.current.state !== "inactive") {
      recorderRef.current.stop();
    }
    setIsRecording(false);
    setRecordElapsed(0);
    setRecordSlideOffset(0);
  }

  function handleRecordSlideDown(e) {
    if (!isRecording) return;
    recordSlideRef.current.active = true;
    recordSlideRef.current.startX = e.clientX;
    e.currentTarget.setPointerCapture(e.pointerId);
  }

  function handleRecordSlideMove(e) {
    if (!recordSlideRef.current.active) return;
    const dx = e.clientX - recordSlideRef.current.startX;
    const next = Math.min(0, dx);
    setRecordSlideOffset(next);
    if (next < -120) {
      recordSlideRef.current.active = false;
      cancelRecording();
    }
  }

  function handleRecordSlideUp() {
    recordSlideRef.current.active = false;
    setRecordSlideOffset(0);
  }

  async function sendFriendRequest() {
    if (!friendSearch.trim()) return;
    setFriendError("");
    setFriendSuccess("");
    try {
      await apiFetch("/api/friends/request", {
        method: "POST",
        body: JSON.stringify({ username: friendSearch.trim() }),
      });
      setFriendSuccess(`Friend request sent to @${friendSearch.trim()}`);
      setFriendSearch("");
      loadChats();
      loadFriendRequests();
    } catch (err) {
      setFriendError(err.message);
    }
  }

  async function acceptRequest(id) {
    await apiFetch(`/api/friends/requests/${id}/accept`, { method: "POST" });
    loadChats();
    loadFriendRequests();
  }

  async function denyRequest(id) {
    await apiFetch(`/api/friends/requests/${id}/deny`, { method: "POST" });
    loadFriendRequests();
  }

  async function clearNotifications() {
    await apiFetch("/api/notifications/clear", { method: "POST" });
    loadNotifications();
    setShowNotifications(false);
  }

  async function removeFriend(userId) {
    const uid = Number(userId);
    const userRow =
      friendsAll.find((f) => Number(f.id) === uid) ||
      friends.find((f) => Number(f.id) === uid) ||
      allUsers.find((u) => Number(u.id) === uid) ||
      profileUser;
    setPendingFriendIds((prev) => prev.filter((id) => Number(id) !== uid));
    setRemovedFriendIds((prev) => Array.from(new Set([...prev, uid])));
    // keep the card visible even after removal
    setFriendsAll((prev) => prev.filter((f) => Number(f.id) !== uid));
    if (profileUser?.id === uid) {
      setProfileIsFriend(false);
    }
    if (userRow) {
      setManualDmUsers((prev) => {
        if (prev.some((u) => Number(u.id) === uid)) return prev;
        const updated = [userRow, ...prev];
        manualDmUsersRef.current = updated;
        return updated;
      });
    }
    try {
      await apiFetch(`/api/friends/${userId}`, { method: "DELETE" });
    } catch {
      // ignore
    }
    await loadChats();
    if (userRow) ensureFriendCard(userRow);
    if (selectedChat?.type === "dm" && selectedChat.id === uid) {
      setSelectedChat(null);
    }
  }

  async function deleteDM(userId) {
    setShowChatMenu(false);
    setDeletingDmId(userId);
    setTimeout(() => {
      socketRef.current?.emit("dm:delete", { userId });
      setDeletingDmId(null);
    }, 220);
  }

  async function sendFriendRequestFromProfile(target) {
    if (!target?.username) return;
    const tid = Number(target.id);
    setPendingFriendIds((prev) => Array.from(new Set([...prev, tid])));
    try {
      await apiFetch("/api/friends/request", {
        method: "POST",
        body: JSON.stringify({ username: target.username }),
      });
      setRemovedFriendIds((prev) => prev.filter((id) => Number(id) !== tid));
      loadChats();
      loadFriendRequests();
    } catch {
      // revert if request failed
      setPendingFriendIds((prev) => prev.filter((id) => Number(id) !== tid));
    }
  }

  async function cancelFriendRequestFromProfile(target) {
    if (!target?.id) return;
    const tid = Number(target.id);
    setPendingFriendIds((prev) => prev.filter((id) => Number(id) !== tid));
    try {
      await apiFetch(`/api/friends/requests/outgoing/${tid}`, { method: "DELETE" });
      loadChats();
      loadFriendRequests();
    } catch {
      // fallback: re-fetch outgoing to sync state
      try {
        const outgoing = await apiFetch("/api/friends/requests/outgoing");
        setPendingFriendIds((outgoing.requests || []).map((r) => r.recipient_id));
      } catch {
        // ignore
      }
    }
  }

  function ensureFriendCard(userRow) {
    if (!userRow?.id) return;
    setFriends((prev) => {
      if (prev.some((f) => Number(f.id) === Number(userRow.id))) return prev;
      return [userRow, ...prev];
    });
    setManualDmUsers((prev) => {
      if (prev.some((u) => Number(u.id) === Number(userRow.id))) return prev;
      const updated = [userRow, ...prev];
      manualDmUsersRef.current = updated;
      return updated;
    });
    if (!manualDmUsersRef.current.some((u) => Number(u.id) === Number(userRow.id))) {
      manualDmUsersRef.current = [userRow, ...manualDmUsersRef.current];
    }
  }

  function closeDM(userId) {
    setDeletingDmId(userId);
    const hidden = getHiddenDms();
    const updated = Array.from(new Set([...hidden, userId]));
    localStorage.setItem(`xp-hidden-dms:${user?.id}`, JSON.stringify(updated));
    setTimeout(() => {
      setFriends((prev) => prev.filter((f) => f.id !== userId));
      if (selectedChat?.type === "dm" && selectedChat.id === userId) {
        setSelectedChat(null);
      }
      setDeletingDmId(null);
      setShowChatMenu(false);
    }, 280);
  }

  async function reopenDM(userId) {
    const hidden = getHiddenDms().filter((id) => id !== userId);
    localStorage.setItem(`xp-hidden-dms:${user?.id}`, JSON.stringify(hidden));
    await loadChats();
  }

  function getHiddenDms() {
    try {
      return JSON.parse(localStorage.getItem(`xp-hidden-dms:${user?.id}`) || "[]") || [];
    } catch {
      return [];
    }
  }

  function handleSearchChange(value) {
    setSearchQuery(value);
    if (!value) {
      setSearchResults([]);
      return;
    }
    const lower = value.toLowerCase();
    const dmMatches = friends.filter(
      (f) =>
        f.username.toLowerCase().includes(lower) ||
        (f.display_name || "").toLowerCase().includes(lower)
    );
    const groupMatches = groups.filter((g) => (g.name || "").toLowerCase().includes(lower));
    const results = [
      ...dmMatches.map((f) => ({
        type: "dm",
        id: f.id,
        label: f.display_name || f.username,
        sub: `@${f.username}`,
        avatar: f.avatar,
        status: f.status,
      })),
      ...groupMatches.map((g) => ({
        type: "group",
        id: g.id,
        label: g.name,
        sub: `${(g.members || []).length} members`,
      })),
    ];
    setSearchResults(results.slice(0, 8));
  }

  function markChatUnread(key) {
    setUnreadChats((prev) => ({ ...prev, [key]: true }));
  }

  function clearChatUnread(key) {
    setUnreadChats((prev) => {
      if (!prev[key]) return prev;
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }

  function saveMutedChats(next) {
    setMutedChats(next);
    if (user?.id) {
      localStorage.setItem(`xp-muted-chats:${user.id}`, JSON.stringify(next));
    }
  }

  function isChatMutedValue(key) {
    const value = mutedChats[key];
    if (!value) return false;
    if (value === "forever") return true;
    return Date.now() < value;
  }

  function setChatMute(key, durationMs) {
    const next = { ...mutedChats };
    if (!durationMs) {
      delete next[key];
    } else {
      next[key] = durationMs === "forever" ? "forever" : Date.now() + durationMs;
    }
    saveMutedChats(next);
  }

  function saveNickname(userId, value) {
    const next = { ...nicknameMap };
    if (!value) {
      delete next[userId];
    } else {
      next[userId] = value;
    }
    setNicknameMap(next);
    if (user?.id) {
      localStorage.setItem(`xp-nicknames:${user.id}`, JSON.stringify(next));
    }
  }

  async function saveNicknameWithDelay(userId, value) {
    if (savingFlags.nickname) return;
    setSavingFlags((prev) => ({ ...prev, nickname: true }));
    setTimeout(() => {
      saveNickname(userId, value);
      setSavingFlags((prev) => ({ ...prev, nickname: false }));
    }, 320);
  }

  async function changeStatus(newStatus) {
    setSettings((prev) => ({ ...prev, status: newStatus }));
    await apiFetch("/api/settings", { method: "PATCH", body: JSON.stringify({ status: newStatus }) });
    setUser((prev) => (prev ? { ...prev, status: newStatus } : prev));
    setStatusMenuOpen(false);
  }

  function cycleStatus() {
    const idx = STATUS_OPTIONS.findIndex((s) => s.value === settings.status);
    const next = STATUS_OPTIONS[(idx + 1) % STATUS_OPTIONS.length];
    changeStatus(next.value);
  }

  function pulseChatCard(key) {
    if (!key) return;
    setSentPulseChat(key);
    if (sentPulseTimerRef.current) clearTimeout(sentPulseTimerRef.current);
    sentPulseTimerRef.current = setTimeout(() => setSentPulseChat(null), 600);
  }

  async function saveCustomStatus() {
    if (savingFlags.customStatus) return;
    setSavingFlags((prev) => ({ ...prev, customStatus: true }));
    setStatusSaving(true);
    setCustomStatusAnim("saving");
    const trimmed = customStatusInput.slice(0, MAX_CUSTOM_STATUS);
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ customStatus: trimmed }),
      });
      setTimeout(() => {
        setSettings((prev) => ({ ...prev, customStatus: trimmed }));
        setStatusSaving(false);
        setCustomStatusAnim("saved");
        setTimeout(() => setCustomStatusAnim(""), 600);
        setStatusEditOpen(false);
        setSavingFlags((prev) => ({ ...prev, customStatus: false }));
      }, 320);
    } catch {
      setStatusSaving(false);
      setCustomStatusAnim("");
      setSavingFlags((prev) => ({ ...prev, customStatus: false }));
    }
  }

  async function saveDisplayName() {
    if (savingFlags.displayName) return;
    setSavingFlags((prev) => ({ ...prev, displayName: true }));
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ displayName: settings.displayName }),
      });
      setTimeout(() => {
        setDisplayEditOpen(false);
      }, 320);
    } finally {
      setTimeout(() => {
        setSavingFlags((prev) => ({ ...prev, displayName: false }));
      }, 320);
    }
  }

  async function saveEmail() {
    if (savingFlags.email) return;
    setSavingFlags((prev) => ({ ...prev, email: true }));
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ email: settings.email, password: emailPassword }),
      });
      setTimeout(() => {
        setEmailEditOpen(false);
        setEmailPassword("");
      }, 320);
    } finally {
      setTimeout(() => {
        setSavingFlags((prev) => ({ ...prev, email: false }));
      }, 320);
    }
  }

  async function savePassword() {
    if (savingFlags.password) return;
    setSavingFlags((prev) => ({ ...prev, password: true }));
    setPasswordError("");
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ password: currentPassword, newPassword }),
      });
      setTimeout(() => {
        setPasswordModalOpen(false);
        setCurrentPassword("");
        setNewPassword("");
      }, 320);
    } catch (err) {
      setPasswordError(err.message || "Incorrect password");
    } finally {
      setTimeout(() => {
        setSavingFlags((prev) => ({ ...prev, password: false }));
      }, 320);
    }
  }

  async function saveUsername() {
    if (savingFlags.username) return;
    setSavingFlags((prev) => ({ ...prev, username: true }));
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ username: newUsername, password: usernamePassword }),
      });
      setTimeout(async () => {
        setUsernameModalOpen(false);
        setNewUsername("");
        setUsernamePassword("");
        const data = await apiFetch("/api/me");
        setUser(data.user);
        setSettings((prev) => ({ ...prev, username: data.user.username }));
      }, 320);
    } finally {
      setTimeout(() => {
        setSavingFlags((prev) => ({ ...prev, username: false }));
      }, 320);
    }
  }

  async function saveBio() {
    if (bioSaving) return;
    setBioSaving(true);
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ bio: aboutInput }),
      });
      setSettings((prev) => ({ ...prev, bio: aboutInput }));
    } finally {
      setTimeout(() => setBioSaving(false), 350);
    }
  }

  async function openBlackjackMatch(matchId) {
    if (!matchId) return;
    setBlackjackError("");
    try {
      const data = await apiFetch(`/api/blackjack/${matchId}`);
      if (data?.match) {
        setBlackjackMatch(data.match);
        setBlackjackPanelOpen(true);
        if (data.match?.players && user?.id) {
          const me = data.match.players.find((p) => p.user_id === user.id);
          setBlackjackWallet(me?.wallet_address || "");
        }
      }
    } catch (err) {
      setBlackjackError(err.message || "Unable to load match");
    }
  }

  async function sendBlackjackInvite(targetId) {
    if (!targetId) return;
    setBlackjackError("");
    setBlackjackUiMessage("");
    try {
      const res = await apiFetch("/api/blackjack/invite", {
        method: "POST",
        body: JSON.stringify({
          toId: targetId,
          wagerAmount: blackjackInvite.wager,
          token: blackjackInvite.token,
          chain: blackjackInvite.chain,
        }),
      });
      if (res?.matchId) {
        await openBlackjackMatch(res.matchId);
      }
    } catch (err) {
      setBlackjackError(err.message || "Unable to invite");
    }
  }

  async function acceptBlackjackInvite(matchId, notificationId) {
    setBlackjackError("");
    try {
      const res = await apiFetch(`/api/blackjack/${matchId}/accept`, { method: "POST" });
      if (notificationId) {
        await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" });
      }
      if (res?.match) {
        setBlackjackMatch(res.match);
        setBlackjackPanelOpen(true);
        const me = res.match.players?.find((p) => p.user_id === user?.id);
        setBlackjackWallet(me?.wallet_address || "");
      } else {
        openBlackjackMatch(matchId);
      }
      loadNotifications();
    } catch (err) {
      if (notificationId && /expired/i.test(err.message || "")) {
        await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" }).catch(() => {});
        loadNotifications();
      }
      setBlackjackError(err.message || "Unable to accept");
    }
  }

  async function declineBlackjackInvite(matchId, notificationId) {
    setBlackjackError("");
    try {
      await apiFetch(`/api/blackjack/${matchId}/decline`, { method: "POST" });
      if (notificationId) {
        await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" });
      }
      loadNotifications();
    } catch (err) {
      setBlackjackError(err.message || "Unable to decline");
    }
  }

  async function simulateBlackjackDeposit() {
    if (!blackjackMatch?.id) return;
    setBlackjackActionBusy(true);
    setBlackjackError("");
    try {
      const res = await apiFetch(`/api/blackjack/${blackjackMatch.id}/deposit/mock`, { method: "POST" });
      if (res?.match) setBlackjackMatch(res.match);
    } catch (err) {
      setBlackjackError(err.message || "Deposit failed");
    } finally {
      setBlackjackActionBusy(false);
    }
  }

  function openWalletPicker(mode = "connect") {
    setWalletPickerMode(mode);
    setWalletPickerOpen(true);
  }

  async function chooseBlackjackWallet(walletOption) {
    if (!walletOption) return;
    setWalletPickerOpen(false);
    try {
      if (miniGameClaimOpen) {
        const ctx = await connectWalletForChain(
          miniGameMatch?.chain || miniGameInvite.chain,
          walletOption,
          true
        );
        setMiniGameClaimAddress((prev) => prev || ctx.address);
      } else if (miniGameHubOpen && miniGameMatch?.status === "deposit") {
        await registerMiniGameWallet(walletOption);
      } else if (walletPickerMode === "claim") {
        await connectBlackjackWallet(walletOption, true);
      } else {
        await registerBlackjackWallet(walletOption);
      }
    } catch (err) {
      setBlackjackError(err.message || "Unable to connect wallet");
    }
  }

  async function connectWalletForChain(chain, walletOption = selectedWalletOption, requestAccounts = true) {
    if (!walletOption) {
      throw new Error("Choose a wallet first.");
    }
    const ctx = await getWalletContext(walletOption, chain, requestAccounts);
    setBlackjackWallet(ctx.address);
    setBlackjackWalletProviderId(walletOption.id);
    return ctx;
  }

  async function connectBlackjackWallet(walletOption = selectedWalletOption, requestAccounts = true) {
    setBlackjackError("");
    const ctx = await connectWalletForChain(
      blackjackMatch?.chain || blackjackInvite.chain,
      walletOption,
      requestAccounts
    );
    setBlackjackClaimAddress((prev) => prev || ctx.address);
    return ctx;
  }

  async function registerBlackjackWallet(walletOption = selectedWalletOption) {
    if (!blackjackMatch?.id) return null;
    setBlackjackActionBusy(true);
    setBlackjackError("");
    try {
      const option = walletOption || selectedWalletOption;
      if (!option) {
        throw new Error("Choose a wallet first.");
      }
      const ctx = await connectBlackjackWallet(option, true);
      const res = await apiFetch(`/api/blackjack/${blackjackMatch.id}/wallet`, {
        method: "POST",
        body: JSON.stringify({ walletAddress: ctx.address }),
      });
      if (res?.match) setBlackjackMatch(res.match);
      setBlackjackWallet(ctx.address);
      return ctx;
    } catch (err) {
      setBlackjackError(err.message || "Unable to register wallet");
      return null;
    } finally {
      setBlackjackActionBusy(false);
    }
  }

  async function syncBlackjackMatch() {
    if (!blackjackMatch?.id) return;
    try {
      const res = await apiFetch(`/api/blackjack/${blackjackMatch.id}/sync`, { method: "POST" });
      if (res?.match) setBlackjackMatch(res.match);
    } catch (err) {
      setBlackjackError(err.message || "Unable to sync deposits");
    }
  }

  async function depositBlackjackOnchain() {
    if (!blackjackMatch?.id) return;
    setBlackjackActionBusy(true);
    setBlackjackError("");
    try {
      if (!selectedWalletOption) {
        throw new Error("Choose a wallet first.");
      }
      const ctx = await registerBlackjackWallet(selectedWalletOption);
      if (!ctx) return;
      const currentWallet = blackjackWalletRegistered || ctx.address;
      if (currentWallet.toLowerCase() !== ctx.address.toLowerCase()) {
        throw new Error("Connect the wallet registered for this match before depositing.");
      }
      const token = new ethers.Contract(blackjackMatch.token_address, ERC20_ABI, ctx.signer);
      const decimals = Number(await token.decimals());
      const wager = ethers.parseUnits(String(blackjackMatch.wager_amount), decimals);
      const allowance = await token.allowance(ctx.address, blackjackMatch.escrow_factory_address);
      if (allowance < wager) {
        const approveTx = await token.approve(blackjackMatch.escrow_factory_address, wager);
        await approveTx.wait();
      }
      const factory = new ethers.Contract(
        blackjackMatch.escrow_factory_address,
        BLACKJACK_FACTORY_ABI,
        ctx.signer
      );
      const depositTx = await factory.deposit(blackjackMatch.escrow_match_id);
      await depositTx.wait();
      playBlackjackTone({ type: "square", start: 300, end: 220, duration: 0.08, gain: 0.025 });
      playBlackjackTone({ type: "square", start: 440, end: 300, duration: 0.1, gain: 0.018, delay: 0.04 });
      await syncBlackjackMatch();
    } catch (err) {
      setBlackjackError(err.shortMessage || err.message || "Deposit failed");
    } finally {
      setBlackjackActionBusy(false);
    }
  }

  async function blackjackAction(action) {
    if (!blackjackMatch?.id) return;
    if (blackjackActionBusy) return;
    setBlackjackActionBusy(true);
    setBlackjackError("");
    try {
      if (action === "hit") {
        playBlackjackTone({ type: "triangle", start: 880, end: 620, duration: 0.08, gain: 0.02 });
      } else if (action === "stand") {
        playBlackjackTone({ type: "sine", start: 420, end: 360, duration: 0.07, gain: 0.018 });
      } else if (action === "double") {
        playBlackjackTone({ type: "square", start: 300, end: 240, duration: 0.08, gain: 0.024 });
        playBlackjackTone({ type: "square", start: 420, end: 300, duration: 0.09, gain: 0.018, delay: 0.04 });
      } else if (action === "split") {
        playBlackjackTone({ type: "triangle", start: 760, end: 520, duration: 0.07, gain: 0.018 });
        playBlackjackTone({ type: "triangle", start: 760, end: 520, duration: 0.07, gain: 0.018, delay: 0.05 });
      }
      const res = await apiFetch(`/api/blackjack/${blackjackMatch.id}/action`, {
        method: "POST",
        body: JSON.stringify({ action }),
      });
      if (res?.match) setBlackjackMatch(res.match);
    } catch (err) {
      setBlackjackError(err.message || "Action failed");
    } finally {
      setBlackjackActionBusy(false);
    }
  }

  async function claimBlackjackWinnings() {
    if (!blackjackMatch?.id) return;
    if (!blackjackClaimAddress.trim()) return;
    setBlackjackActionBusy(true);
    setBlackjackError("");
    try {
      if (CHAIN_MODE === "evm") {
        if (!selectedWalletOption) {
          throw new Error("Choose a wallet first.");
        }
        const ctx = await connectBlackjackWallet(selectedWalletOption, true);
        const factory = new ethers.Contract(
          blackjackMatch.escrow_factory_address,
          BLACKJACK_FACTORY_ABI,
          ctx.signer
        );
        const recipient = ethers.getAddress(blackjackClaimAddress.trim());
        const tx = await factory.claim(blackjackMatch.escrow_match_id, recipient);
        await tx.wait();
        const res = await apiFetch(`/api/blackjack/${blackjackMatch.id}/claim`, {
          method: "POST",
          body: JSON.stringify({ walletAddress: recipient, txHash: tx.hash }),
        });
        if (res?.match) setBlackjackMatch(res.match);
      } else {
      const res = await apiFetch(`/api/blackjack/${blackjackMatch.id}/claim`, {
        method: "POST",
        body: JSON.stringify({ walletAddress: blackjackClaimAddress.trim() }),
      });
      if (res?.match) setBlackjackMatch(res.match);
      }
      setBlackjackClaimOpen(false);
      setBlackjackClaimAddress("");
    } catch (err) {
      setBlackjackError(err.message || "Claim failed");
    } finally {
      setBlackjackActionBusy(false);
    }
  }

  async function openMiniGameMatch(matchId) {
    if (!matchId) return;
    setMiniGameError("");
    try {
      const data = await apiFetch(`/api/minigames/${matchId}`);
      if (data?.match) {
        setMiniGameMatch(data.match);
        setMiniGameHubOpen(true);
        setMiniGameDepositAmount(String(data.match.wager_amount || 10));
        const mine = (data.match.players || []).find((p) => p.user_id === user?.id);
        if (mine?.wallet_address) {
          setMiniGameClaimAddress((prev) => prev || mine.wallet_address);
        }
      }
    } catch (err) {
      setMiniGameError(err.message || "Unable to load game");
    }
  }

  async function sendMiniGameInvite(targetId) {
    if (!targetId) return;
    setMiniGameBusy(true);
    setMiniGameError("");
    setMiniGameUiMessage("");
    try {
      const res = await apiFetch("/api/minigames/invite", {
        method: "POST",
        body: JSON.stringify({
          toId: targetId,
          gameType: miniGameInvite.gameType,
          wagerAmount: miniGameInvite.wager,
          token: miniGameInvite.token,
          chain: miniGameInvite.chain,
        }),
      });
      if (res?.matchId) {
        await openMiniGameMatch(res.matchId);
      }
    } catch (err) {
      setMiniGameError(err.message || "Unable to send game invite");
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function acceptMiniGameInvite(matchId, notificationId) {
    setMiniGameBusy(true);
    setMiniGameError("");
    try {
      const res = await apiFetch(`/api/minigames/${matchId}/accept`, { method: "POST" });
      if (notificationId) {
        await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" });
      }
      if (res?.match) {
        setMiniGameMatch(res.match);
        setMiniGameHubOpen(true);
        setMiniGameDepositAmount(String(res.match.wager_amount || 10));
        const mine = (res.match.players || []).find((p) => p.user_id === user?.id);
        if (mine?.wallet_address) {
          setMiniGameClaimAddress((prev) => prev || mine.wallet_address);
        }
      }
      loadNotifications();
    } catch (err) {
      if (notificationId && /expired/i.test(err.message || "")) {
        await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" }).catch(() => {});
        loadNotifications();
      }
      setMiniGameError(err.message || "Unable to accept game");
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function declineMiniGameInvite(matchId, notificationId) {
    setMiniGameBusy(true);
    setMiniGameError("");
    try {
      await apiFetch(`/api/minigames/${matchId}/decline`, { method: "POST" });
      if (notificationId) {
        await apiFetch(`/api/notifications/${notificationId}/read`, { method: "POST" });
      }
      loadNotifications();
    } catch (err) {
      setMiniGameError(err.message || "Unable to decline game");
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function depositMiniGame() {
    if (!miniGameMatch?.id) return;
    setMiniGameBusy(true);
    setMiniGameError("");
    try {
      if (CHAIN_MODE === "evm") {
        throw new Error("Use the on-chain deposit flow.");
      }
      const res = await apiFetch(`/api/minigames/${miniGameMatch.id}/deposit`, {
        method: "POST",
        body: JSON.stringify({
          amount: miniGameMatch.game_type === "coinflip" ? miniGameMatch.wager_amount : miniGameDepositAmount,
        }),
      });
      if (res?.match) setMiniGameMatch(res.match);
    } catch (err) {
      setMiniGameError(err.message || "Unable to lock your wager");
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function registerMiniGameWallet(walletOption = selectedWalletOption) {
    if (!miniGameMatch?.id) return null;
    setMiniGameBusy(true);
    setMiniGameError("");
    try {
      const option = walletOption || selectedWalletOption;
      if (!option) {
        throw new Error("Choose a wallet first.");
      }
      const ctx = await connectWalletForChain(miniGameMatch?.chain || miniGameInvite.chain, option, true);
      const res = await apiFetch(`/api/minigames/${miniGameMatch.id}/wallet`, {
        method: "POST",
        body: JSON.stringify({ walletAddress: ctx.address }),
      });
      if (res?.match) setMiniGameMatch(res.match);
      setMiniGameClaimAddress((prev) => prev || ctx.address);
      return ctx;
    } catch (err) {
      setMiniGameError(err.message || "Unable to register wallet");
      return null;
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function syncMiniGameMatch() {
    if (!miniGameMatch?.id) return;
    try {
      const res = await apiFetch(`/api/minigames/${miniGameMatch.id}/sync`, { method: "POST" });
      if (res?.match) setMiniGameMatch(res.match);
    } catch (err) {
      setMiniGameError(err.message || "Unable to sync game");
    }
  }

  async function depositMiniGameOnchain() {
    if (!miniGameMatch?.id) return;
    setMiniGameBusy(true);
    setMiniGameError("");
    try {
      if (!selectedWalletOption) {
        throw new Error("Choose a wallet first.");
      }
      const ctx = await registerMiniGameWallet(selectedWalletOption);
      if (!ctx) return;
      const myPlayer = (miniGameMatch?.players || []).find((p) => p.user_id === user?.id) || null;
      const registeredWallet = String(myPlayer?.wallet_address || ctx.address).toLowerCase();
      if (registeredWallet !== String(ctx.address).toLowerCase()) {
        throw new Error("Connect the wallet registered for this game before depositing.");
      }
      const token = new ethers.Contract(miniGameMatch.token_address, ERC20_ABI, ctx.signer);
      const decimals = Number(await token.decimals());
      const amountValue =
        miniGameMatch.game_type === "coinflip"
          ? String(miniGameMatch.wager_amount)
          : String(miniGameDepositAmount || 0);
      const depositAmount = ethers.parseUnits(amountValue, decimals);
      const allowance = await token.allowance(ctx.address, miniGameMatch.escrow_factory_address);
      if (allowance < depositAmount) {
        const approveTx = await token.approve(miniGameMatch.escrow_factory_address, depositAmount);
        await approveTx.wait();
      }
      const factory = new ethers.Contract(
        miniGameMatch.escrow_factory_address,
        MINI_GAME_FACTORY_ABI,
        ctx.signer
      );
      const depositTx = await factory.deposit(miniGameMatch.escrow_match_id, depositAmount);
      await depositTx.wait();
      await syncMiniGameMatch();
    } catch (err) {
      setMiniGameError(err.shortMessage || err.message || "Deposit failed");
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function claimMiniGameWinnings() {
    if (!miniGameMatch?.id) return;
    if (!miniGameClaimAddress.trim()) return;
    setMiniGameBusy(true);
    setMiniGameError("");
    try {
      if (CHAIN_MODE === "evm") {
        if (!selectedWalletOption) {
          throw new Error("Choose a wallet first.");
        }
        const ctx = await connectWalletForChain(
          miniGameMatch?.chain || miniGameInvite.chain,
          selectedWalletOption,
          true
        );
        const recipient = ethers.getAddress(miniGameClaimAddress.trim());
        const factory = new ethers.Contract(
          miniGameMatch.escrow_factory_address,
          MINI_GAME_FACTORY_ABI,
          ctx.signer
        );
        const tx = await factory.claim(miniGameMatch.escrow_match_id, recipient);
        await tx.wait();
        const res = await apiFetch(`/api/minigames/${miniGameMatch.id}/claim`, {
          method: "POST",
          body: JSON.stringify({ walletAddress: recipient, txHash: tx.hash }),
        });
        if (res?.match) setMiniGameMatch(res.match);
      } else {
        const res = await apiFetch(`/api/minigames/${miniGameMatch.id}/claim`, {
          method: "POST",
          body: JSON.stringify({ walletAddress: miniGameClaimAddress.trim() }),
        });
        if (res?.match) setMiniGameMatch(res.match);
      }
      setMiniGameClaimOpen(false);
    } catch (err) {
      setMiniGameError(err.message || "Claim failed");
    } finally {
      setMiniGameBusy(false);
    }
  }

  async function copyEscrowAddress(address, setMessage) {
    try {
      await copyText(address);
      setMessage("Escrow address copied.");
      setTimeout(() => setMessage(""), 1800);
    } catch {
      setMessage("Unable to copy address.");
      setTimeout(() => setMessage(""), 1800);
    }
  }

  function prepareBlackjackRematch() {
    if (!blackjackMatch) return;
    const opponent = (blackjackMatch.players || []).find((p) => p.user_id !== user?.id);
    setBlackjackInvite({
      wager: String(blackjackMatch.wager_amount || 10),
      token: blackjackMatch.token || "USDC",
      chain: blackjackMatch.chain || "base",
      targetId: opponent?.user_id ? String(opponent.user_id) : "",
    });
    setBlackjackMatch(null);
    setBlackjackClaimOpen(false);
    setBlackjackClaimAddress("");
    setBlackjackError("");
    setBlackjackUiMessage("");
    setBlackjackPanelOpen(true);
  }

  function prepareMiniGameRematch() {
    if (!miniGameMatch) return;
    const opponent = (miniGameMatch.players || []).find((p) => p.user_id !== user?.id);
    setMiniGameInvite({
      gameType: miniGameMatch.game_type || "coinflip",
      wager: String(miniGameMatch.wager_amount || 10),
      token: miniGameMatch.token || "USDC",
      chain: miniGameMatch.chain || "base",
      targetId: opponent?.user_id ? String(opponent.user_id) : "",
    });
    setMiniGameMatch(null);
    setMiniGameClaimOpen(false);
    setMiniGameClaimAddress("");
    setMiniGameError("");
    setMiniGameUiMessage("");
    setMiniGameHubOpen(true);
  }

  async function saveProfileSong() {
    if (musicSaving) return;
    setMusicSaving(true);
    setMusicError("");
    const payload = {
      source: musicDraft.sourceUrl ? "spotify" : "custom",
      sourceUrl: musicDraft.sourceUrl,
      title: musicDraft.title,
      artist: musicDraft.artist,
      album: musicDraft.album,
      coverUrl: musicDraft.coverUrl,
      audioUrl: musicDraft.audioUrl,
    };
    try {
      const res = await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ song: payload }),
      });
      if (res?.user) {
        setUser(res.user);
        setSettings((prev) => ({
          ...prev,
          songTitle: res.user.song_title || "",
          songArtist: res.user.song_artist || "",
          songAlbum: res.user.song_album || "",
          songCoverUrl: res.user.song_cover_url || "",
          songAudioUrl: res.user.song_audio_url || "",
          songSource: res.user.song_source || "",
          songSourceUrl: res.user.song_source_url || "",
        }));
        setMusicDraft({
          source: res.user.song_source || "spotify",
          sourceUrl: res.user.song_source_url || "",
          title: res.user.song_title || "",
          artist: res.user.song_artist || "",
          album: res.user.song_album || "",
          coverUrl: res.user.song_cover_url || "",
          audioUrl: res.user.song_audio_url || "",
        });
      }
    } catch (err) {
      setMusicError(err.message || "Unable to save song");
    } finally {
      setTimeout(() => setMusicSaving(false), 300);
    }
  }

  async function clearProfileSong() {
    if (musicSaving) return;
    setMusicSaving(true);
    setMusicError("");
    try {
      const res = await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ song: null }),
      });
      if (res?.user) {
        setUser(res.user);
        setSettings((prev) => ({
          ...prev,
          songTitle: "",
          songArtist: "",
          songAlbum: "",
          songCoverUrl: "",
          songAudioUrl: "",
          songSource: "",
          songSourceUrl: "",
        }));
        setMusicDraft({
          source: "spotify",
          sourceUrl: "",
          title: "",
          artist: "",
          album: "",
          coverUrl: "",
          audioUrl: "",
        });
      }
    } catch (err) {
      setMusicError(err.message || "Unable to clear song");
    } finally {
      setTimeout(() => setMusicSaving(false), 300);
    }
  }

  async function applyAvatarDataUrl(dataUrl) {
    await new Promise((r) => setTimeout(r, 180));
    const blob = dataUrlToBlob(dataUrl);
    if (!blob) return;
    const formData = new FormData();
    formData.append("image", blob, "avatar.png");
    const upload = await apiUpload("/api/uploads", formData);
    const avatarUrlValue = upload.url;
    await apiFetch("/api/settings", {
      method: "PATCH",
      body: JSON.stringify({ avatar: avatarUrlValue }),
    });
    setSettings((prev) => ({ ...prev, avatar: avatarUrlValue }));
  }

  async function applyAvatarFile(file) {
    const formData = new FormData();
    formData.append("image", file);
    const upload = await apiUpload("/api/uploads", formData);
    const avatarUrlValue = upload.url;
    await apiFetch("/api/settings", {
      method: "PATCH",
      body: JSON.stringify({ avatar: avatarUrlValue }),
    });
    setSettings((prev) => ({ ...prev, avatar: avatarUrlValue }));
  }

  async function resetAvatar() {
    if (avatarApplying) return;
    setAvatarApplying(true);
    try {
      await apiFetch("/api/settings", {
        method: "PATCH",
        body: JSON.stringify({ avatar: "" }),
      });
      setSettings((prev) => ({ ...prev, avatar: "" }));
    } finally {
      setTimeout(() => setAvatarApplying(false), 300);
    }
  }

  function openAvatarEditor(file, target = { type: "user", groupId: null }) {
    if (file.type === "image/gif") {
      if (target.type === "group") {
        const url = URL.createObjectURL(file);
        setTimeout(() => {
          setEditGroupAvatar(url);
        }, 180);
      } else {
        applyAvatarFile(file);
      }
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const src = reader.result;
      const img = new Image();
      img.onload = () => {
        avatarImgRef.current = img;
        const cropSize = 240;
        const base = Math.max(cropSize / img.width, cropSize / img.height);
        setAvatarEditSize({ w: img.width, h: img.height, base });
        setAvatarEditSrc(src);
        setAvatarZoom(1);
        setAvatarRotate(0);
        setAvatarOffset({ x: 0, y: 0 });
        setAvatarEditTarget(target);
        setAvatarEditOpen(true);
      };
      img.src = src;
    };
    reader.readAsDataURL(file);
  }

  async function applyAvatarCrop() {
    if (avatarApplying) return;
    setAvatarApplying(true);
    const img = avatarImgRef.current;
    if (!img) return;
    const cropSize = 240;
    const canvasSize = 256;
    const canvas = document.createElement("canvas");
    canvas.width = canvasSize;
    canvas.height = canvasSize;
    const ctx = canvas.getContext("2d");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, canvasSize, canvasSize);
    const scale = avatarEditSize.base * avatarZoom * (canvasSize / cropSize);
    ctx.translate(canvasSize / 2, canvasSize / 2);
    ctx.rotate((avatarRotate * Math.PI) / 180);
    ctx.translate(avatarOffset.x * (canvasSize / cropSize), avatarOffset.y * (canvasSize / cropSize));
    ctx.scale(scale, scale);
    ctx.drawImage(img, -avatarEditSize.w / 2, -avatarEditSize.h / 2);
    const dataUrl = canvas.toDataURL("image/png", 0.92);
    if (avatarEditTarget.type === "group") {
      setEditGroupAvatar(dataUrl);
      setTimeout(() => {
        setAvatarEditOpen(false);
        setAvatarApplying(false);
      }, 320);
      return;
    }
    await applyAvatarDataUrl(dataUrl);
    setTimeout(() => {
      setAvatarEditOpen(false);
      setAvatarApplying(false);
    }, 320);
  }

  function onAvatarPointerDown(e) {
    if (!avatarEditOpen) return;
    e.preventDefault();
    const pointers = avatarPointersRef.current;
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    avatarDragRef.current.startX = e.clientX;
    avatarDragRef.current.startY = e.clientY;
    avatarDragRef.current.x = avatarOffset.x;
    avatarDragRef.current.y = avatarOffset.y;
    if (pointers.size === 2) {
      const [a, b] = Array.from(pointers.values());
      const dist = Math.hypot(a.x - b.x, a.y - b.y);
      avatarDragRef.current.pinchDist = dist;
      avatarDragRef.current.pinchZoom = avatarZoom;
    }
    e.currentTarget.setPointerCapture(e.pointerId);
  }

  function onAvatarPointerMove(e) {
    if (!avatarEditOpen) return;
    const pointers = avatarPointersRef.current;
    if (!pointers.has(e.pointerId)) return;
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.size === 2) {
      const [a, b] = Array.from(pointers.values());
      const dist = Math.hypot(a.x - b.x, a.y - b.y);
      const base = avatarDragRef.current.pinchDist || dist;
      const next = Math.min(3, Math.max(1, avatarDragRef.current.pinchZoom * (dist / base)));
      setAvatarZoom(next);
      return;
    }
    const dx = e.clientX - avatarDragRef.current.startX;
    const dy = e.clientY - avatarDragRef.current.startY;
    setAvatarOffset({ x: avatarDragRef.current.x + dx, y: avatarDragRef.current.y + dy });
  }

  function onAvatarPointerUp(e) {
    const pointers = avatarPointersRef.current;
    pointers.delete(e.pointerId);
  }

  function onAvatarWheel(e) {
    if (!avatarEditOpen) return;
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.08 : 0.08;
    setAvatarZoom((z) => Math.min(3, Math.max(1, z + delta)));
  }

  function handleCustomThemeUpload(e) {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const media = {
        type: file.type.startsWith("video/") ? "video" : "image",
        url: reader.result,
      };
      if (media.type === "image") {
        const img = new Image();
        img.onload = () => {
          const canvas = document.createElement("canvas");
          const size = 48;
          canvas.width = size;
          canvas.height = size;
          const ctx = canvas.getContext("2d");
          ctx.drawImage(img, 0, 0, size, size);
          const data = ctx.getImageData(0, 0, size, size).data;
          let r = 0, g = 0, b = 0, count = 0;
          for (let i = 0; i < data.length; i += 4) {
            r += data[i];
            g += data[i + 1];
            b += data[i + 2];
            count += 1;
          }
          r = Math.round(r / count);
          g = Math.round(g / count);
          b = Math.round(b / count);
          const tint = `rgba(${r}, ${g}, ${b}, 0.82)`;
          const titlebar = `rgba(${Math.round(r * 0.55)}, ${Math.round(g * 0.55)}, ${Math.round(b * 0.55)}, 0.95)`;
          setCustomThemeTint(tint);
          document.documentElement.style.setProperty("--xp-custom-tint", tint);
          document.documentElement.style.setProperty("--xp-custom-titlebar", titlebar);
          const stored = { ...media, tint, titlebar };
          setCustomThemeMedia(stored);
          if (user) {
            localStorage.setItem(customThemeKey, JSON.stringify(stored));
          }
          setTheme("custom");
        };
        img.src = media.url;
      } else {
        const tint = customThemeTint || "rgba(30, 32, 38, 0.82)";
        const titlebar = "rgba(30, 32, 38, 0.95)";
        setCustomThemeTint(tint);
        document.documentElement.style.setProperty("--xp-custom-tint", tint);
        document.documentElement.style.setProperty("--xp-custom-titlebar", titlebar);
        const stored = { ...media, tint, titlebar };
        setCustomThemeMedia(stored);
        if (user) {
          localStorage.setItem(customThemeKey, JSON.stringify(stored));
        }
        setTheme("custom");
      }
    };
    reader.readAsDataURL(file);
    e.target.value = "";
  }

  async function createConnection() {
    setConnectionError("");
    if (!connectionService) return;
    if (!connectionHandle.trim()) {
      setConnectionError("Handle required");
      return;
    }
    if (connectionService === "spotify" && !connectionUrl.trim()) {
      setConnectionError("Paste a Spotify track link so the profile music card can sync automatically.");
      return;
    }
    if (!/^[a-zA-Z0-9._-]+$/.test(connectionHandle.trim())) {
      setConnectionError("Only letters, numbers, ., -, _ allowed");
      return;
    }
    const body = {
      service: connectionService,
      handle: connectionHandle.trim(),
      url: connectionUrl.trim(),
    };
    await apiFetch("/api/connections", { method: "POST", body: JSON.stringify(body) });
    setConnectionModalOpen(false);
    setConnectionService(null);
    setConnectionHandle("");
    setConnectionUrl("");
    await loadConnections();
    if (body.service === "spotify") {
      await refreshCurrentUser();
    }
  }

  async function toggleConnectionVisibility(id, visible) {
    const visibility = visible ? "public" : "hidden";
    await apiFetch(`/api/connections/${id}`, { method: "PATCH", body: JSON.stringify({ visibility }) });
    loadConnections();
  }

  async function removeConnection(id) {
    const target = connections.find((c) => c.id === id);
    await apiFetch(`/api/connections/${id}`, { method: "DELETE" });
    setRemoveConnectionId(null);
    await loadConnections();
    if (target?.service === "spotify") {
      await refreshCurrentUser();
    }
  }

  function openRemoveConnection(connection) {
    if (!connection?.id) return;
    setRemoveConnectionId(connection.id);
  }

  async function checkAliasAvailability() {
    const username = aliasCheck.username.trim().replace(/^@/, "");
    if (!username) return;
    try {
      const data = await apiFetch("/api/usernames/check", {
        method: "POST",
        body: JSON.stringify({ username }),
      });
      setAliasCheck({ username, available: data.available, error: "" });
    } catch (err) {
      setAliasCheck({ username, available: null, error: err.message });
    }
  }

  async function claimAlias() {
    const username = aliasCheck.username.trim().replace(/^@/, "");
    if (!username) return;
    if (!aliasPassword.trim()) {
      setAliasCheck({ username, available: null, error: "Enter current password" });
      return;
    }
    try {
      setAliasClaiming(true);
      await apiFetch("/api/usernames/claim", {
        method: "POST",
        body: JSON.stringify({ username, password: aliasPassword }),
      });
      await new Promise((r) => setTimeout(r, 180));
      setAliasPassword("");
      setAliasCheck({ username: "", available: null, error: "" });
      loadUsernames();
    } catch (err) {
      setAliasCheck({ username, available: null, error: err.message });
    } finally {
      setAliasClaiming(false);
    }
  }

  async function setPrimaryAlias(username) {
    if (!primaryPassword.trim()) {
      setPrimaryError("Enter current password");
      return;
    }
    try {
      setPrimarySwitching(true);
      await apiFetch("/api/usernames/set-primary", {
        method: "POST",
        body: JSON.stringify({ username, password: primaryPassword }),
      });
      // slight delay for smooth transition
      await new Promise((r) => setTimeout(r, 320));
      await loadUsernames();
      const data = await apiFetch("/api/me");
      setUser(data.user);
      setSettings((prev) => ({ ...prev, username: data.user.username || prev.username }));
      setPrimaryPassword("");
      setPrimaryError("");
      setPrimaryModal(null);
      setTimeout(() => setPrimarySwitching(false), 220);
    } catch (err) {
      setPrimaryError(err.message || "Incorrect password");
      setPrimarySwitching(false);
    }
  }

  async function removeAlias(username, password) {
    setAliasRemoving(true);
    await apiFetch("/api/usernames/remove", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    await new Promise((r) => setTimeout(r, 280));
    await loadUsernames();
    setAliasRemoving(false);
  }

  async function createGroup(name, memberIds) {
    await apiFetch("/api/groups", {
      method: "POST",
      body: JSON.stringify({ name, memberIds }),
    });
    await new Promise((r) => setTimeout(r, 160));
    loadChats();
  }

  async function renameGroup(groupId, name) {
    await apiFetch(`/api/groups/${groupId}`, {
      method: "PATCH",
      body: JSON.stringify({ name }),
    });
    await new Promise((r) => setTimeout(r, 160));
    loadChats();
  }

  async function updateGroup(groupId, name, avatar) {
    const payload = {};
    if (typeof name === "string" && name.trim()) payload.name = name.trim();
    if (typeof avatar === "string") payload.avatar = avatar;
    await apiFetch(`/api/groups/${groupId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    await new Promise((r) => setTimeout(r, 180));
    setGroups((prev) =>
      prev.map((g) =>
        g.id === groupId
          ? {
              ...g,
              name: payload.name || g.name,
              avatar: Object.prototype.hasOwnProperty.call(payload, "avatar")
                ? payload.avatar
                : g.avatar,
            }
          : g
      )
    );
    await new Promise((r) => setTimeout(r, 120));
    loadChats();
  }

  async function leaveGroup(groupId) {
    await apiFetch(`/api/groups/${groupId}/leave`, { method: "POST" });
    await new Promise((r) => setTimeout(r, 160));
    loadChats();
    if (selectedChat?.type === "group" && selectedChat.id === groupId) {
      setSelectedChat(null);
    }
  }

  function toggleGroupSelect(id) {
    setGroupSelected((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  function requestCloseEditGroup() {
    if (!editGroupModal) return;
    const dirty =
      (editGroupName || "").trim() !== (editGroupModal.name || "").trim() ||
      (editGroupAvatar || "") !== (editGroupModal.avatar || "");
    if (dirty) {
      setEditGroupConfirmOpen(true);
    } else {
      closeModalWithAnim("editGroup", () => setEditGroupModal(null));
    }
  }

  async function submitAddMembers() {
    if (!addMembersModal) return;
    const groupForAdd =
      selectedGroup && selectedGroup.id === addMembersModal
        ? selectedGroup
        : groups.find((g) => g.id === addMembersModal) || null;
    if (!groupForAdd) return;
    const ids = Object.keys(addMembersSelected).filter((id) => addMembersSelected[id]);
    if (ids.length === 0) return;
    const currentCount = groupForAdd?.members?.length || 0;
    if (currentCount + ids.length > 10) return;
    await apiFetch(`/api/groups/${addMembersModal}/members`, {
      method: "POST",
      body: JSON.stringify({ memberIds: ids.map((id) => Number(id)) }),
    });
    await new Promise((r) => setTimeout(r, 160));
    setAddMembersSelected({});
    setAddMembersModal(null);
    loadChats();
  }

  async function submitGroupCreate() {
    const ids = Object.keys(groupSelected)
      .filter((id) => groupSelected[id])
      .map((id) => Number(id));
    if (ids.length === 0) return;
    await createGroup(groupNameInput.trim(), ids);
    setGroupNameInput("");
    setGroupSelected({});
    setGroupPickerOpen(false);
  }

  async function removeGroupMember(groupId, memberId) {
    await apiFetch(`/api/groups/${groupId}/remove`, {
      method: "POST",
      body: JSON.stringify({ userId: memberId }),
    });
    setGroupMemberMenu(null);
    loadChats();
  }

  async function transferGroupOwner(groupId, memberId) {
    await apiFetch(`/api/groups/${groupId}/owner`, {
      method: "POST",
      body: JSON.stringify({ userId: memberId }),
    });
    setGroupMemberMenu(null);
    loadChats();
  }

  async function transferAlias(username, toUsername, password) {
    setAliasTransfering(true);
    setTransferError("");
    try {
      const cleanTo = (toUsername || "").trim();
      const cleanPass = password || "";
      if (!cleanTo || !cleanPass) {
        throw new Error("Recipient and password required");
      }
      await apiFetch("/api/usernames/transfer", {
        method: "POST",
        body: JSON.stringify({ username, toUsername: cleanTo, password: cleanPass }),
      });
      await new Promise((r) => setTimeout(r, 280));
      await loadUsernames();
      setTransferModal(null);
    } catch (err) {
      setTransferError(err.message || "Transfer failed");
    } finally {
      setAliasTransfering(false);
    }
  }

  async function acceptTransfer(id) {
    await apiFetch(`/api/usernames/transfers/${id}/accept`, { method: "POST" });
    loadUsernames();
  }

  async function denyTransfer(id) {
    await apiFetch(`/api/usernames/transfers/${id}/deny`, { method: "POST" });
    loadUsernames();
  }

  function openMessageMenu(event, message) {
    if (message?.pending) return;
    event.preventDefault();
    setReactionPickerFor(null);
    setShowMessageMenu({ x: event.clientX, y: event.clientY, message });
  }

  function handleMessageLongPressStart(event, message) {
    if (!isMobile) return;
    const touch = event.touches?.[0];
    if (!touch) return;
    longPressTriggeredRef.current = false;
    longPressStartRef.current = { x: touch.clientX, y: touch.clientY, active: true };
    if (longPressTimerRef.current) {
      clearTimeout(longPressTimerRef.current);
    }
    longPressTimerRef.current = setTimeout(() => {
      longPressTriggeredRef.current = true;
      openMessageMenu(
        { preventDefault() {}, clientX: touch.clientX, clientY: touch.clientY },
        message
      );
      if (navigator?.vibrate) navigator.vibrate(20);
    }, 520);
  }

  function handleMessageLongPressMove(event) {
    if (!longPressStartRef.current.active) return;
    const touch = event.touches?.[0];
    if (!touch) return;
    const dx = touch.clientX - longPressStartRef.current.x;
    const dy = touch.clientY - longPressStartRef.current.y;
    if (Math.hypot(dx, dy) > 12) {
      longPressStartRef.current.active = false;
      if (longPressTimerRef.current) clearTimeout(longPressTimerRef.current);
    }
  }

  function handleMessageLongPressEnd(event) {
    longPressStartRef.current.active = false;
    if (longPressTimerRef.current) clearTimeout(longPressTimerRef.current);
    if (longPressTriggeredRef.current) {
      event.preventDefault();
      event.stopPropagation();
    }
  }

  function openProfileMenu(event, userId) {
    event.preventDefault();
    setFriendMuteMenu(null);
    setFriendGameMenu(null);
    setShowProfileMenu({ x: event.clientX, y: event.clientY, userId });
  }

  function openGameInviteFromProfile(userId, gameType) {
    const normalizedId = Number(userId);
    if (!normalizedId) return;
    setShowProfileMenu(null);
    setFriendGameMenu(null);
    if (gameType === "blackjack") {
      setBlackjackInvite((prev) => ({ ...prev, targetId: String(normalizedId) }));
      setBlackjackMatch(null);
      setBlackjackError("");
      setBlackjackPanelOpen(true);
      return;
    }
    setMiniGameMatch(null);
    setMiniGameError("");
    setMiniGameInvite((prev) => ({
      ...prev,
      gameType,
      targetId: String(normalizedId),
    }));
    setMiniGameHubOpen(true);
  }

  function openGroupMemberMenu(event, memberId) {
    event.preventDefault();
    setGroupMemberMenu({ x: event.clientX, y: event.clientY, memberId });
  }

  function shouldShowTimestamp(index, list) {
    const current = list[index];
    const next = list[index + 1];
    if (!current) return false;
    if (!next) return true;
    const currentTime = new Date(current.created_at).getTime();
    const nextTime = new Date(next.created_at).getTime();
    return nextTime - currentTime > 10000;
  }

  async function deleteMessage(messageId) {
    if (!selectedChat) return;
    setDeletingMessageIds((prev) => ({ ...prev, [messageId]: true }));
    await new Promise((r) => setTimeout(r, 280));
    try {
      if (selectedChat.type === "dm") {
        await apiFetch(`/api/messages/item/${messageId}`, { method: "DELETE" });
        const key = chatKey("dm", selectedChat.id);
        setMessagesByChat((prev) => ({
          ...prev,
          [key]: (prev[key] || []).filter((m) => m.id !== Number(messageId)),
        }));
      } else {
        await apiFetch(`/api/groups/${selectedChat.id}/messages/${messageId}`, { method: "DELETE" });
        const key = chatKey("group", selectedChat.id);
        setMessagesByChat((prev) => ({
          ...prev,
          [key]: (prev[key] || []).filter((m) => m.id !== Number(messageId)),
        }));
      }
    } finally {
      setDeletingMessageIds((prev) => {
        const next = { ...prev };
        delete next[messageId];
        return next;
      });
    }
  }

  async function editMessage(message) {
    if (!message) return;
    const nextBody = (editingText || "").trim();
    if (!nextBody) return;
    const key = selectedChat ? chatKey(selectedChat.type, selectedChat.id) : null;
    try {
      if (selectedChat?.type === "dm") {
        const data = await apiFetch(`/api/messages/${message.id}`, {
          method: "PATCH",
          body: JSON.stringify({ body: nextBody }),
        });
        const updated =
          data.message ||
          {
            ...message,
            body: nextBody,
            edited_at: new Date().toISOString(),
          };
        if (!updated.edited_at) updated.edited_at = new Date().toISOString();
        if (key) {
          setMessagesByChat((prev) => ({
            ...prev,
            [key]: mergeMessages(prev[key] || [], updated),
          }));
        }
      } else if (selectedChat?.type === "group") {
        const data = await apiFetch(`/api/groups/${selectedChat.id}/messages/${message.id}`, {
          method: "PATCH",
          body: JSON.stringify({ body: nextBody }),
        });
        const updated =
          data.message ||
          {
            ...message,
            body: nextBody,
            edited_at: new Date().toISOString(),
          };
        if (!updated.edited_at) updated.edited_at = new Date().toISOString();
        if (key) {
          setMessagesByChat((prev) => ({
            ...prev,
            [key]: mergeMessages(prev[key] || [], updated),
          }));
        }
      }
    } catch {
      // keep edit open if request fails
      return;
    }
    setEditingMessageId(null);
    setEditingText("");
  }

  function toggleSelectMessage(messageId) {
    setSelectedMessageIds((prev) => ({ ...prev, [messageId]: !prev[messageId] }));
  }

  async function deleteSelectedMessages() {
    const ids = Object.keys(selectedMessageIds).filter((id) => selectedMessageIds[id]);
    if (ids.length === 0 || !selectedChat) return;
    setDeletingMessageIds((prev) => {
      const next = { ...prev };
      ids.forEach((id) => {
        next[id] = true;
      });
      return next;
    });
    await new Promise((r) => setTimeout(r, 280));
    const key = chatKey(selectedChat.type, selectedChat.id);
    setMessagesByChat((prev) => ({
      ...prev,
      [key]: (prev[key] || []).filter((m) => !ids.includes(String(m.id))),
    }));
    if (selectedChat.type === "dm") {
      await Promise.allSettled(
        ids.map((id) => apiFetch(`/api/messages/item/${id}`, { method: "DELETE" }))
      );
    } else {
      await Promise.allSettled(
        ids.map((id) => apiFetch(`/api/groups/${selectedChat.id}/messages/${id}`, { method: "DELETE" }))
      );
    }
    setDeletingMessageIds((prev) => {
      const next = { ...prev };
      ids.forEach((id) => {
        delete next[id];
      });
      return next;
    });
    setSelectMode(false);
    setSelectedMessageIds({});
  }

  function highlightMentions(text, contextUsers) {
    const parts = [];
    if (!text) return text;
    const regex = /@([a-zA-Z0-9_]{2,20})/g;
    let lastIndex = 0;
    let match;
    while ((match = regex.exec(text))) {
      const start = match.index;
      const end = regex.lastIndex;
      if (start > lastIndex) parts.push(text.slice(lastIndex, start));
      const name = match[1];
      const isMention = contextUsers.some(
        (u) => u.username.toLowerCase() === name.toLowerCase() ||
          (u.aliases || []).some((a) => a.toLowerCase() === name.toLowerCase())
      );
      if (isMention) {
        parts.push(
          <button
            key={`${start}-${name}`}
            className="xp-mention"
            type="button"
            onClick={() => {
              const target = contextUsers.find(
                (u) => u.username.toLowerCase() === name.toLowerCase() ||
                  (u.aliases || []).some((a) => a.toLowerCase() === name.toLowerCase())
              );
              if (target) {
                loadProfile(target.id);
              }
            }}
          >
            @{name}
          </button>
        );
      } else {
        parts.push(text.slice(start, end));
      }
      lastIndex = end;
    }
    if (lastIndex < text.length) parts.push(text.slice(lastIndex));
    return parts;
  }

  function renderNotificationText(n, handlers) {
    const text = n?.message || "Notification";
    const payload = parseNotificationPayload(n);
    if (n?.type === "group_invite" && n.group_id) {
      return (
        <>
          {n.from_username ? <>@{n.from_username}</> : null}{" "}
          has added you to{" "}
          <button className="xp-mention xp-mention-inline" type="button" onClick={() => handlers.selectGroup(n.group_id)}>
            #{n.group_name || "group"}
          </button>
        </>
      );
    }
    if (n?.type === "friend_request" || n?.type === "friend_accept") return text;
    if (n?.type === "mention" && n?.context === "group" && n?.group_id) {
      return (
        <>
          @{n.from_username} has mentioned you in{" "}
          <button className="xp-mention xp-mention-inline" type="button" onClick={() => handlers.selectGroup(n.group_id)}>
            #{n.group_name || "group"}
          </button>
        </>
      );
    }
    if (n?.type === "mention" && n?.context === "dm") {
      return `@${n.from_username} has mentioned you in private`;
    }
    if (n?.type === "blackjack_invite") {
      if (isGameInviteExpired(n)) {
        return "Blackjack invitation expired.";
      }
      const wager = payload?.wagerAmount ? `${payload.wagerAmount} ${payload.token || "USDC"}` : "a wager";
      const chainLabel = payload?.chain ? ` on ${payload.chain}` : "";
      return (
        <>
          @{n.from_username || "friend"} invited you to Blackjack — {wager}{chainLabel}
        </>
      );
    }
    if (n?.type === "blackjack_claim") {
      return "You won a Blackjack match. Claim your winnings.";
    }
    if (n?.type === "minigame_invite") {
      if (isGameInviteExpired(n)) {
        const gameLabel = payload?.gameType === "bigbank" ? "Big Bank Small Bank" : "Coinflip";
        return `${gameLabel} invitation expired.`;
      }
      const gameLabel = payload?.gameType === "bigbank" ? "Big Bank Small Bank" : "Coinflip";
      const wager = payload?.wagerAmount ? `${payload.wagerAmount} ${payload.token || "USDC"}` : "a wager";
      return (
        <>
          @{n.from_username || "friend"} invited you to {gameLabel} for {wager}
        </>
      );
    }
    if (n?.type === "minigame_claim") {
      return "You won a mini game. Claim your winnings.";
    }
    const from = (n?.from_username || "").toLowerCase();
    if (!from) return text;
    const parts = [];
    const regex = /@([a-zA-Z0-9_]{2,20})/g;
    let lastIndex = 0;
    let match;
    while ((match = regex.exec(text))) {
      const start = match.index;
      const end = regex.lastIndex;
      if (start > lastIndex) parts.push(text.slice(lastIndex, start));
      const name = match[1];
      if (name.toLowerCase() === from && n.from_user_id) {
        parts.push(
          <button
            key={`${start}-${name}`}
            type="button"
            className="xp-mention"
            onClick={() => handlers.loadProfile(n.from_user_id)}
          >
            @{name}
          </button>
        );
      } else {
        parts.push(text.slice(start, end));
      }
      lastIndex = end;
    }
    if (lastIndex < text.length) parts.push(text.slice(lastIndex));
    return parts;
  }

  function getMessageContextUsers(includeAll = false) {
    if (selectedChat?.type === "group") {
      const base = selectedGroup?.members || [];
      if (!includeAll) return base;
      const merged = [...base];
      (allUsers || []).forEach((u) => {
        if (!merged.some((m) => Number(m.id) === Number(u.id))) merged.push(u);
      });
      return merged;
    }
    return [selectedFriend, user].filter(Boolean);
  }

  function getUserById(id) {
    const num = Number(id);
    return (
      (selectedGroup?.members || []).find((m) => Number(m.id) === num) ||
      friendsAll.find((f) => Number(f.id) === num) ||
      friends.find((f) => Number(f.id) === num) ||
      allUsers.find((u) => Number(u.id) === num) ||
      (user && Number(user.id) === num ? user : null)
    );
  }

  function getForwardInfo(msg) {
    if (msg.forwarded_from_username) {
      return {
        id: msg.forwarded_from_id || null,
        username: msg.forwarded_from_username,
        displayName: msg.forwarded_from_display || "",
      };
    }
    const sender = getUserById(msg.sender_id);
    return {
      id: sender?.id || null,
      username: sender?.username || "",
      displayName: sender?.display_name || "",
    };
  }

  function openForwardModal(messages) {
    if (!messages || !messages.length) return;
    setForwardTarget(null);
    setForwardNote("");
    setForwardModal({ messages });
  }

  async function sendForward() {
    if (!forwardTarget || !forwardModal?.messages?.length) return;
    const note = forwardNote.trim();
    for (const msg of forwardModal.messages) {
      const forwardedFrom = getForwardInfo(msg);
      if (forwardTarget.type === "dm") {
        socketRef.current?.emit("dm:send", {
          toId: forwardTarget.id,
          type: msg.type || "text",
          body: msg.body || "",
          imageUrl: msg.image_url || null,
          audioUrl: msg.audio_url || null,
          forwardedFrom,
        });
        if (note) {
          socketRef.current?.emit("dm:send", {
            toId: forwardTarget.id,
            type: "text",
            body: note.slice(0, 300),
          });
        }
      } else {
        socketRef.current?.emit("group:send", {
          groupId: forwardTarget.id,
          type: msg.type || "text",
          body: msg.body || "",
          imageUrl: msg.image_url || null,
          audioUrl: msg.audio_url || null,
          forwardedFrom,
        });
        if (note) {
          socketRef.current?.emit("group:send", {
            groupId: forwardTarget.id,
            type: "text",
            body: note.slice(0, 300),
          });
        }
      }
    }
    // Refresh target chat so forwarded messages appear immediately
    try {
      if (forwardTarget.type === "dm") {
        const data = await apiFetch(`/api/messages/${forwardTarget.id}`);
        const key = chatKey("dm", forwardTarget.id);
        setMessagesByChat((prev) => ({ ...prev, [key]: data.messages || [] }));
      } else {
        const data = await apiFetch(`/api/groups/${forwardTarget.id}/messages`);
        const key = chatKey("group", forwardTarget.id);
        setMessagesByChat((prev) => ({ ...prev, [key]: data.messages || [] }));
      }
      loadChats();
    } catch {
      // ignore refresh errors
    }
    setForwardModal(null);
    setForwardTarget(null);
    setForwardNote("");
  }

  async function handleReaction(message, emoji) {
    if (!message) return;
    if (selectedChat?.type === "group") {
      socketRef.current?.emit("group:react", {
        groupId: selectedChat.id,
        messageId: message.id,
        emoji,
      });
    } else if (selectedChat?.type === "dm") {
      socketRef.current?.emit("dm:react", {
        messageId: message.id,
        emoji,
      });
    }
  }

  const profileStory = profileUser ? stories.find((s) => s.user?.id === profileUser.id) : null;
  const profileHasStory = Boolean(profileStory?.stories?.length);
  const profileHasUnviewed = Boolean(profileStory?.has_unviewed);
  const profileSong = profileUser ? extractProfileSong(profileUser) : null;
  const profileSongHasAudio = !!profileSong?.audioUrl;
  const profileSongIsPlaying =
    !!profileSong &&
    profileSongPlayer.ownerId === profileUser?.id &&
    profileSongPlayer.playing;
  const blackjackState = blackjackMatch?.state || null;
  const blackjackPlayers = blackjackMatch?.players || [];
  const blackjackMe = blackjackPlayers.find((p) => p.user_id === user?.id) || null;
  const blackjackOpponent = blackjackPlayers.find((p) => p.user_id !== user?.id) || null;
  const blackjackIsMyTurn = blackjackState?.currentPlayerId === user?.id;
  const blackjackPot = blackjackMatch
    ? Number(blackjackMatch.wager_amount || 0) * blackjackPlayers.length
    : 0;
  const blackjackDepositRemaining = blackjackMatch?.deposit_deadline
    ? Math.max(0, new Date(blackjackMatch.deposit_deadline).getTime() - Date.now())
    : 0;
  const blackjackCallOpponent = resolveUserById(callState.withUserId, {
    user,
    friends,
    friendsAll,
    manualDmUsers,
    allUsers,
  });
  const blackjackTargetId = Number(blackjackInvite.targetId || blackjackCallOpponent?.id || selectedFriend?.id || friends?.[0]?.id || 0);
  const blackjackTargetUser = resolveUserById(blackjackTargetId, {
    user,
    friends,
    friendsAll,
    manualDmUsers,
    allUsers,
  });
  const blackjackWalletRegistered = blackjackMe?.wallet_address || "";
  const blackjackEscrowReady =
    !!blackjackMatch?.escrow_match_id &&
    !!blackjackMatch?.escrow_factory_address &&
    !!blackjackMatch?.token_address;
  const miniGamePlayers = miniGameMatch?.players || [];
  const miniGameMe = miniGamePlayers.find((p) => p.user_id === user?.id) || null;
  const miniGameOpponent = miniGamePlayers.find((p) => p.user_id !== user?.id) || null;
  const miniGamePot = miniGamePlayers.reduce(
    (sum, p) => sum + Number(p.deposited_amount || p.deposit_amount || 0),
    0
  );
  const miniGameCountdownRemaining = miniGameMatch?.state?.countdownEndsAt
    ? Math.max(0, new Date(miniGameMatch.state.countdownEndsAt).getTime() - Date.now())
    : 0;
  const miniGameTargetId = Number(miniGameInvite.targetId || selectedFriend?.id || friends?.[0]?.id || 0);
  const miniGameWinner = miniGameMatch?.winner_id
    ? resolveUserById(miniGameMatch.winner_id, { user, friends, friendsAll, manualDmUsers, allUsers })
    : null;
  const miniGameAssignments = miniGameMatch?.state?.assignments || {};
  const miniGameWalletRegistered = miniGameMe?.wallet_address || "";
  const miniGameEscrowReady =
    !!miniGameMatch?.escrow_match_id &&
    !!miniGameMatch?.escrow_factory_address &&
    !!miniGameMatch?.token_address;
  const miniGameSelectedType = miniGameMatch?.game_type || miniGameInvite.gameType || "coinflip";
  const miniGameGameLabel =
    miniGameSelectedType === "bigbank" ? "Big Bank Small Bank" : "Coinflip";
  const walletOptions = useMemo(() => {
    const injected = listInjectedWallets();
    const options = [...injected];
    if (WALLETCONNECT_PROJECT_ID) {
      options.push({ id: "walletconnect", label: "WalletConnect", type: "walletconnect" });
    }
    return options;
  }, []);
  const selectedWalletOption = walletOptions.find((option) => option.id === blackjackWalletProviderId) || null;
  const renderGameAudioStrip = (mode = "casino") => (
    <div className="xp-game-audio-strip">
      <div className="xp-game-audio-meta">
        <div className="xp-game-audio-title">Game Audio</div>
        <div className="xp-game-audio-sub">
          {mode === "coinflip" ? "Bet loop + coinflip sting" : "Bet loop ambience"}
        </div>
      </div>
      <button
        className={`xp-button xp-game-audio-toggle ${gameAudioMuted ? "muted" : ""}`}
        type="button"
        onClick={() => setGameAudioMuted((prev) => !prev)}
      >
        {gameAudioMuted ? "Unmute" : "Mute"}
      </button>
    </div>
  );

  useEffect(() => {
    if (!user?.id) return;
    const stored = localStorage.getItem(`xp-wallet-provider:${user.id}`) || "";
    if (stored && walletOptions.some((option) => option.id === stored)) {
      setBlackjackWalletProviderId((prev) => prev || stored);
    }
  }, [user?.id, walletOptions]);

  useEffect(() => {
    if (!user?.id || !blackjackWalletProviderId) return;
    localStorage.setItem(`xp-wallet-provider:${user.id}`, blackjackWalletProviderId);
  }, [user?.id, blackjackWalletProviderId]);
  const renderBjCard = (card, idx, hidden = false) => {
    const rank = card?.slice(0, -1) || "";
    const suit = card?.slice(-1) || "";
    const suitMap = { H: "♥", D: "♦", C: "♣", S: "♠" };
    const isRed = suit === "H" || suit === "D";
    return (
      <div
        key={`${card}-${idx}`}
        className={`xp-bj-card ${hidden ? "back" : ""} ${isRed ? "red" : ""}`}
        style={{ animationDelay: `${idx * 80}ms` }}
      >
        {hidden ? (
          <div className="xp-bj-card-back" />
        ) : (
          <>
            <div className="xp-bj-card-rank">{rank}</div>
            <div className="xp-bj-card-suit">{suitMap[suit] || suit}</div>
          </>
        )}
      </div>
    );
  };

  if (!user) {
    return (
      <div className="xp-desktop">
        <div className="xp-window auth-window">
          <div className="xp-titlebar">
            <span>{authMode === "login" ? "Log In" : "Register"}</span>
          </div>
          <div className="xp-window-body">
            <form onSubmit={handleAuth} className="xp-form">
              <label>
                Username
                <input
                  value={form.username}
                  onChange={(e) => setForm({ ...form, username: e.target.value })}
                  minLength={USERNAME_MIN}
                  maxLength={USERNAME_MAX}
                  required
                />
              </label>
              {authMode === "register" && (
                <label>
                  Email
                  <input
                    type="email"
                    value={form.email}
                    onChange={(e) => setForm({ ...form, email: e.target.value })}
                  />
                </label>
              )}
              <label>
                Password
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  required
                />
              </label>
              {authError && <div className="xp-error">{authError}</div>}
              <div className="xp-button-row">
                <button className="xp-button" type="submit" disabled={authLoading}>
                  {authLoading ? "Please wait..." : authMode === "login" ? "Log In" : "Create Account"}
                </button>
                <button
                  className="xp-button secondary"
                  type="button"
                  disabled={authLoading}
                  onClick={() => setAuthMode(authMode === "login" ? "register" : "login")}
                >
                  {authMode === "login" ? "Register" : "Back to Login"}
                </button>
              </div>
            </form>
          </div>
          
        </div>
      </div>
    );
  }

  const canJoinActiveCall =
    selectedChat?.type === "dm" &&
    callState.status === "active" &&
    callState.withUserId === selectedChat.id;
  const canJoinGroupFromMessage =
    selectedChat?.type === "group" &&
    groupCall.groupId === selectedChat.id &&
    groupCall.status !== "idle";
  const canJoinFromMessage =
    selectedChat?.type === "dm" &&
    ((callState.withUserId === selectedChat.id && callState.status === "active") ||
      incomingCall?.fromId === selectedChat.id);

  return (
    <div className="xp-desktop">
      {theme === "custom" && customThemeMedia && (
        <div className="xp-custom-bg">
          {customThemeMedia.type === "video" ? (
            <video src={customThemeMedia.url} autoPlay loop muted />
          ) : (
            <img src={customThemeMedia.url} alt="" />
          )}
          <div className="xp-custom-bg-overlay" />
        </div>
      )}
      {!appMinimized && (
      <div className={`xp-window app-window ${appMaximized ? "is-maximized" : ""}`}>
        <div className="xp-titlebar">
          <span>Tarot Club</span>
          <div className="xp-titlebar-controls">
            <button
              className="xp-titlebar-btn"
              type="button"
              aria-label="Maximize"
              onClick={() => setAppMaximized((prev) => !prev)}
            >
              {appMaximized ? "❐" : "□"}
            </button>
          </div>
        </div>
        <div
          className={`xp-window-body app-body ${view === "settings" ? "settings" : ""} ${
            selectedChat?.type === "group" && view !== "settings" ? "xp-group" : ""
          } ${showChatProfile && selectedChat?.type === "dm" ? "dm-profile" : ""} ${
            isMobile && view !== "settings" && mobileView === "chat" ? "mobile-chat" : ""
          } ${isMobile && view !== "settings" && mobileView === "list" ? "mobile-list" : ""
          }`}
        >
          <aside className="xp-sidebar">
            <div className="xp-profile">
              <div className="xp-profile-left">
                <div className="xp-avatar-status-row">
                  <div className="xp-story-wrap">
                    <button
                      className={`xp-story-ring ${stories.some((s) => s.user?.id === user.id && s.has_unviewed) ? "active" : ""}`}
                      type="button"
                      onClick={() => {
                        const index = stories.findIndex((s) => s.user?.id === user.id);
                        if (index !== -1) {
                          viewStory(index, 0);
                        }
                      }}
                    >
                      <img
                        src={avatarUrl(settings.avatar, user.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, user.username)}
                      />
                    </button>
                    <button
                      type="button"
                      className="xp-story-add"
                      onClick={() => storyInputRef.current?.click()}
                    >
                      +
                    </button>
                  </div>
                  {settings.customStatus && (
                    <div className={`xp-custom-status ${customStatusAnim}`}>Current status: {settings.customStatus}</div>
                  )}
                </div>
                <div className="xp-username">@{settings.username}</div>
                <div className="xp-status-row xp-status-stack">
                  <span className="xp-status-dot" style={{ backgroundColor: getStatusColor(settings.status) }} />
                  <span
                    className="xp-status"
                    style={{ color: getStatusColor(settings.status) }}
                    onClick={(e) => {
                      e.stopPropagation();
                      setStatusMenuOpen((prev) => !prev);
                    }}
                  >
                    {getStatusLabel(settings.status)}
                  </span>
                  <button
                    className="xp-status-toggle"
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setStatusMenuOpen((prev) => !prev);
                    }}
                  >
                    <span className="xp-status-arrow">&#9662;</span>
                  </button>
                  {statusMenuOpen && (
                    <div className="xp-status-menu" onClick={(e) => e.stopPropagation()}>
                      {STATUS_OPTIONS.map((s) => (
                        <button
                          key={s.value}
                          className="xp-status-option"
                          onClick={() => changeStatus(s.value)}
                        >
                          <span className="xp-status-dot" style={{ backgroundColor: s.color }} />
                          {s.label}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              <input
                ref={storyInputRef}
                type="file"
                accept="image/*,video/*"
                className="xp-hidden"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) uploadStory(file);
                  e.target.value = "";
                }}
              />
            </div>

            <div className="xp-sidebar-actions">
              <button className="xp-button" type="button" onClick={() => setView("chat")}>Inbox</button>
              <button className="xp-button" type="button" onClick={() => setView("settings")}>Settings</button>
            </div>

            <div className="xp-sidebar-scroll">
              <div className="xp-titlebar-search-wrap">
                <input
                  className="xp-titlebar-search"
                  placeholder="Search @user or group"
                  value={searchQuery}
                  onChange={(e) => handleSearchChange(e.target.value)}
                />
                {searchResults.length > 0 && (
                  <div className="xp-search-results">
                    {searchResults.map((r) => (
                      <button
                        key={`${r.type}:${r.id}`}
                        className="xp-search-item"
                        type="button"
                        onClick={() => {
                          if (r.type === "group") {
                            selectGroup(r.id);
                          } else {
                            selectDm(r.id);
                          }
                          setSearchQuery("");
                          setSearchResults([]);
                        }}
                      >
                        {r.type === "dm" ? (
                      <img
                        src={avatarUrl(r.avatar, r.sub?.slice(1))}
                        alt=""
                        onError={(e) => onAvatarError(e, r.sub?.slice(1))}
                      />
                        ) : (
                          <div className="xp-search-icon">#</div>
                        )}
                        <div>
                          <div className="xp-search-name">{r.label}</div>
                          <div className="xp-search-sub">{r.sub}</div>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <div className="xp-friends-block">
                <div className="xp-friends-header">
                  <div className="xp-users-title">Friends</div>
                  <div className="xp-friends-actions">
                    <button
                      className="xp-button xp-icon-button"
                      type="button"
                      onClick={() => {
                        setAddFriendOpen((prev) => !prev);
                        setShowNotifications(false);
                      }}
                      title="Add friend"
                    >
                      <img src={addIcon} alt="Add friend" />
                    </button>
                  <button
                    className="xp-button xp-icon-button"
                    type="button"
                    onClick={() => {
                      setShowNotifications((prev) => !prev);
                      setAddFriendOpen(false);
                      loadNotifications();
                    }}
                    title="Alerts"
                  >
                    <img src={bellIcon} alt="Alerts" />
                    {notifications.length > 0 && (
                      <span className="xp-badge">{notifications.length > 9 ? "9+" : notifications.length}</span>
                    )}
                  </button>
                </div>
              </div>
                <div className={`xp-friend-input ${addFriendOpen ? "open" : ""}`}>
                  <input
                    className="xp-input"
                    value={friendSearch}
                    onChange={(e) => setFriendSearch(e.target.value)}
                    placeholder="Add friend by username"
                    disabled={!addFriendOpen}
                  />
                  <button
                    className="xp-button"
                    type="button"
                    onClick={sendFriendRequest}
                    disabled={!addFriendOpen}
                  >
                    Send
                  </button>
                </div>
                {friendError && <div className="xp-error">{friendError}</div>}
                {friendSuccess && <div className="xp-success">{friendSuccess}</div>}
              </div>

              {showNotifications && (
                <div className="xp-notifications">
                  <div className="xp-notifications-header">
                    <div className="xp-users-title">Alerts</div>
                    <button className="xp-notifications-clear" type="button" onClick={clearNotifications}>
                      x
                    </button>
                  </div>
                  {notifications.length === 0 && <div className="xp-muted">No alerts</div>}
                  {notifications.map((n) => (
                    <div key={n.id} className="xp-notification">
                      {(() => {
                        const inviteExpired =
                          (n.type === "blackjack_invite" || n.type === "minigame_invite") &&
                          isGameInviteExpired(n);
                        return (
                          <>
                      <div className="xp-notification-text">
                        {renderNotificationText(n, { selectGroup, loadProfile })}
                      </div>
                      <div className="xp-notification-actions">
                        {n.context === "group" && n.group_id && n.type !== "group_invite" && (
                          <button className="xp-button" onClick={() => selectGroup(n.group_id)}>
                            #{n.group_name || "group"}
                          </button>
                        )}
                        {n.context === "dm" && n.from_user_id && (
                          <button
                            className="xp-button"
                            onClick={() => {
                              selectDm(n.from_user_id);
                              setShowNotifications(false);
                            }}
                          >
                            Open DM
                          </button>
                        )}
                        {n.type === "username_transfer" && n.transfer_id && (
                          <div className="xp-transfer-actions">
                            <button className="xp-button" onClick={() => acceptTransfer(n.transfer_id)}>Accept</button>
                            <button className="xp-button" onClick={() => denyTransfer(n.transfer_id)}>Decline</button>
                          </div>
                        )}
                        {n.type === "blackjack_invite" && n.ref_id && !inviteExpired && (
                          <div className="xp-transfer-actions">
                            <button
                              className="xp-button"
                              onClick={() => acceptBlackjackInvite(n.ref_id, n.id)}
                            >
                              Accept
                            </button>
                            <button
                              className="xp-button"
                              onClick={() => declineBlackjackInvite(n.ref_id, n.id)}
                            >
                              Decline
                            </button>
                          </div>
                        )}
                        {n.type === "blackjack_invite" && inviteExpired && (
                          <div className="xp-muted">Invitation expired</div>
                        )}
                        {n.type === "blackjack_claim" && n.ref_id && (
                          <button
                            className="xp-button"
                            onClick={() => {
                              openBlackjackMatch(n.ref_id);
                              setBlackjackClaimOpen(true);
                              apiFetch(`/api/notifications/${n.id}/read`, { method: "POST" }).catch(() => {});
                            }}
                          >
                            Claim
                          </button>
                        )}
                        {n.type === "minigame_invite" && n.ref_id && !inviteExpired && (
                          <div className="xp-transfer-actions">
                            <button
                              className="xp-button"
                              onClick={() => acceptMiniGameInvite(n.ref_id, n.id)}
                            >
                              Accept
                            </button>
                            <button
                              className="xp-button"
                              onClick={() => declineMiniGameInvite(n.ref_id, n.id)}
                            >
                              Decline
                            </button>
                          </div>
                        )}
                        {n.type === "minigame_invite" && inviteExpired && (
                          <div className="xp-muted">Invitation expired</div>
                        )}
                        {n.type === "minigame_claim" && n.ref_id && (
                          <button
                            className="xp-button"
                            onClick={() => {
                              openMiniGameMatch(n.ref_id);
                              setMiniGameClaimOpen(true);
                              apiFetch(`/api/notifications/${n.id}/read`, { method: "POST" }).catch(() => {});
                            }}
                          >
                            Claim
                          </button>
                        )}
                      </div>
                      <div className="xp-notification-time">{formatDateTime(n.created_at)}</div>
                          </>
                        );
                      })()}
                    </div>
                  ))}
                </div>
              )}

              {requests.length > 0 && (
                <div className="xp-requests">
                  <div className="xp-users-title">Requests</div>
                  {requests.map((r) => (
                    <div key={r.id} className="xp-request">
                      <img
                        src={avatarUrl(r.avatar, r.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, r.username)}
                      />
                      <div className="xp-request-info">
                        <div className="xp-user-name">@{r.username}</div>
                        <div className="xp-user-display">{r.display_name || r.username}</div>
                      </div>
                      <div className="xp-request-actions">
                        <button className="xp-button" onClick={() => acceptRequest(r.id)}>Accept</button>
                        <button className="xp-button" onClick={() => denyRequest(r.id)}>Deny</button>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              <div className="xp-users xp-scroll">
                  {friends.map((u) => (
                    <button
                      key={u.id}
                      className={`xp-user ${selectedChat?.type === "dm" && selectedChat.id === u.id ? "active" : ""}`}
                      data-unread={unreadChats[chatKey("dm", u.id)] ? "1" : "0"}
                      data-muted={isChatMutedValue(chatKey("dm", u.id)) ? "1" : "0"}
                      onClick={() => selectDm(u.id)}
                      onContextMenu={(e) => openProfileMenu(e, u.id)}
                    >
                    <div className="xp-story-wrap">
                      <button
                        type="button"
                        className={`xp-story-ring ${u.has_story ? "active" : ""} ${u.has_unviewed_story ? "" : "viewed"}`}
                        onClick={(e) => {
                          e.stopPropagation();
                          openStoryByUserId(u.id);
                        }}
                      >
                        <img
                          src={avatarUrl(u.avatar, u.username)}
                          alt=""
                          onError={(e) => onAvatarError(e, u.username)}
                        />
                      </button>
                    </div>
                    <div>
                      <div className="xp-user-name">
                        {nicknameMap[u.id] || u.display_name || u.username}
                      </div>
                      <div className="xp-user-status">
                        <span className="xp-status-dot" style={{ backgroundColor: getStatusColor(u.status || "offline") }} />
                        <span>{getStatusLabel(u.status || "offline")}</span>
                      </div>
                      {u.last_message && (
                        <div className="xp-user-preview">
                          {getLastMessagePreview(
                            u.last_message,
                            user?.id,
                            nicknameMap[u.id] || u.display_name || u.username
                          )}
                        </div>
                      )}
                      {(u.custom_status || customStatusById[u.id]) && (
                        <div className="xp-custom-status">{u.custom_status || customStatusById[u.id]}</div>
                      )}
                      {typingByChat[chatKey("dm", u.id)] && (
                        <div className="xp-user-typing">Typing…</div>
                      )}
                    </div>
                  </button>
                ))}
              </div>

              <div className="xp-users">
                <div className="xp-users-header">
                  <div className="xp-users-title">Groups</div>
                  <button className="xp-button" onClick={() => setGroupPickerOpen((prev) => !prev)}>New</button>
                </div>
                {groupPickerOpen && (
                  <div className="xp-group-picker">
                    <label className="xp-group-name-field">
                      Group name (optional)
                      <input
                        value={groupNameInput}
                        onChange={(e) => setGroupNameInput(e.target.value)}
                        placeholder="New group"
                      />
                    </label>
                    <div className="xp-group-picker-list">
                      {friends.map((f) => (
                        <label key={f.id} className="xp-group-picker-item">
                          <input
                            type="checkbox"
                            checked={!!groupSelected[f.id]}
                            onChange={() => toggleGroupSelect(f.id)}
                          />
                          <img
                            src={avatarUrl(f.avatar, f.username)}
                            alt=""
                            onError={(e) => onAvatarError(e, f.username)}
                          />
                          <span>@{f.username}</span>
                        </label>
                      ))}
                    </div>
                    <div className="xp-button-row">
                      <button className="xp-button" onClick={submitGroupCreate}>Create</button>
                      <button className="xp-button" onClick={() => setGroupPickerOpen(false)}>Cancel</button>
                    </div>
                  </div>
                )}
                  <div className="xp-group-list">
                    {groups.map((g) => (
                    <button
                        key={g.id}
                        className={`xp-group xp-user ${selectedChat?.type === "group" && selectedChat.id === g.id ? "active" : ""}`}
                        data-unread={unreadChats[chatKey("group", g.id)] ? "1" : "0"}
                        data-muted={isChatMutedValue(chatKey("group", g.id)) ? "1" : "0"}
                        onClick={() => selectGroup(g.id)}
                        onContextMenu={(e) => {
                          e.preventDefault();
                          setGroupContextMenu({ x: e.clientX, y: e.clientY, group: g });
                        }}
                      >
                      <img
                        src={g.avatar || defaultGroupAvatar()}
                        alt=""
                        onError={(e) => onAvatarError(e, g.name)}
                      />
                      <div>
                        <div className="xp-user-name">{g.name}</div>
                        <div className="xp-user-display">
                          {(g.members || []).filter((m) =>
                            ["online", "away", "busy"].includes(m.status)
                          ).length} online
                        </div>
                        {g.last_message && (
                          <div className="xp-user-preview">
                            {getLastMessagePreview(
                              g.last_message,
                              user?.id,
                              g.last_message.sender_display_name || g.last_message.sender_username || g.last_message.sender_name,
                              true
                            )}
                          </div>
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </aside>

          <section className="xp-chat">
            {view === "settings" ? (
              <div className="xp-settings-panel">
                <div className="xp-settings-header">
                  <button className="xp-button xp-settings-back" onClick={() => setView("chat")}>
                    Back
                  </button>
                  <span>Settings</span>
                </div>
                <div className="xp-settings-grid">
                  <div className="xp-settings-card about-full">
                    <div className="xp-settings-title">Profile</div>
                    <div className="xp-settings-avatar-row">
                      <button
                        type="button"
                        className="xp-settings-avatar"
                        onClick={() => avatarInputRef.current?.click()}
                      >
                        <img
                          src={avatarUrl(settings.avatar, settings.username)}
                          alt=""
                          onError={(e) => onAvatarError(e, settings.username)}
                        />
                        <span className="xp-settings-avatar-overlay">Change</span>
                      </button>
                      <button className="xp-button" onClick={() => setStatusEditOpen(true)}>
                        Set Status
                      </button>
                      <button className="xp-button" onClick={resetAvatar} disabled={avatarApplying}>
                        {avatarApplying ? "Resetting..." : "Reset Avatar"}
                      </button>
                    </div>
                    <div className="xp-settings-row">
                      <div className="xp-settings-label">Username</div>
                      <div className="xp-settings-value">@{settings.username}</div>
                      <button className="xp-button" onClick={() => setUsernameModalOpen(true)}>Edit</button>
                    </div>
                    <div className="xp-settings-row">
                      <div className="xp-settings-label">Display name</div>
                      <div className="xp-settings-value">{settings.displayName || settings.username}</div>
                      <button className="xp-button" onClick={() => setDisplayEditOpen((prev) => !prev)}>Edit</button>
                    </div>
                    <div className="xp-settings-row">
                      <div className="xp-settings-label">Email</div>
                      <div className="xp-settings-value">{settings.email || "Not set"}</div>
                      <button className="xp-button" onClick={() => setEmailEditOpen(true)}>Edit</button>
                    </div>
                    {settings.email && !settings.emailVerified && (
                      <div className="xp-warning">Email not verified</div>
                    )}
                    <div className="xp-settings-row">
                      <div className="xp-settings-label">Password</div>
                      <div className="xp-settings-value">********</div>
                      <button className="xp-button" onClick={() => setPasswordModalOpen(true)}>Change Password</button>
                    </div>
                    <input
                      ref={avatarInputRef}
                      type="file"
                      accept="image/*"
                      className="xp-hidden"
                      onChange={(e) => {
                        const file = e.target.files?.[0];
                        if (file) openAvatarEditor(file, { type: "user" });
                        e.target.value = "";
                      }}
                    />
                  </div>

                  <div className="xp-settings-card">
                    <div className="xp-settings-title">About me</div>
                    <textarea
                      className="xp-about-textarea"
                      value={aboutInput}
                      maxLength={MAX_BIO}
                      onChange={(e) => setAboutInput(e.target.value)}
                    />
                    <div className="xp-about-footer">
                      <div className="xp-about-count">{aboutInput.length}/{MAX_BIO}</div>
                      <div className="xp-about-actions">
                        <button className="xp-button" onClick={() => setShowBioEmoji((prev) => !prev)}>:)</button>
                        <button className={`xp-button ${bioSaving ? "is-saving" : ""}`} onClick={saveBio} disabled={bioSaving}>
                          {bioSaving ? "Saving..." : "Save"}
                        </button>
                      </div>
                    </div>
                    {showBioEmoji && (
                      <div className="xp-emoji-picker xp-emoji-picker-bio">
                        {EMOJI_LIST.map((emoji) => (
                          <button
                            key={emoji}
                            className="xp-emoji"
                            onClick={() => setAboutInput((prev) => `${prev}${emoji}`)}
                          >
                            {emoji}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="xp-settings-card">
                    <div className="xp-settings-title">Appearance</div>
                    <div className="xp-theme-grid">
                      {["classic", "olive", "custom", "kawaii"].map((t) => (
                        <button
                          key={t}
                          className={`xp-theme-card ${theme === t ? "active" : ""}`}
                          onClick={() => {
                            if (t === "custom") {
                              customThemeInputRef.current?.click();
                            } else {
                              setTheme(t);
                            }
                          }}
                        >
                          <div className={`xp-theme-preview theme-${t}`}>
                            <div className="xp-theme-titlebar" />
                            <div className="xp-theme-panel" />
                            <div className="xp-theme-panel small" />
                          </div>
                          <span>{t === "kawaii" ? "Cute" : t === "custom" ? "Custom" : t}</span>
                        </button>
                      ))}
                    </div>
                    <input
                      ref={customThemeInputRef}
                      type="file"
                      accept="image/*,video/*"
                      className="xp-hidden"
                      onChange={handleCustomThemeUpload}
                    />
                  </div>

                  <div className="xp-settings-card">
                    <div className="xp-settings-title">Connections</div>
                    <div className="xp-connection-row">
                      <div className="xp-connection-icons-container">
                        <div className="xp-connection-icons">
                          {CONNECTION_SERVICES.map((s) => (
                            <button
                              key={s.id}
                              className={`xp-connection-icon ${connectionService === s.id ? "active" : ""}`}
                              onClick={() => {
                                setConnectionService(s.id);
                                setConnectionModalOpen(true);
                              }}
                            >
                              <img src={s.icon} alt={s.label} />
                            </button>
                          ))}
                        </div>
                      </div>
                    </div>
                    <div className="xp-connection-list cards">
                      {connections.map((c) => (
                        <div key={c.id} className="xp-connection-card">
                          <div className="xp-connection-card-main">
                            <img src={CONNECTION_SERVICES.find((s) => s.id === c.service)?.icon} alt="" />
                            <div className="xp-connection-card-text">
                              <div className="xp-connection-handle">@{c.handle}</div>
                              <div className="xp-connection-service">{c.service}</div>
                            </div>
                          </div>
                          <div className="xp-connection-card-actions">
                            <label className={`xp-toggle ${c.visibility !== "hidden" ? "on" : ""}`}>
                              <input
                                type="checkbox"
                                checked={c.visibility !== "hidden"}
                                onChange={(e) => toggleConnectionVisibility(c.id, e.target.checked)}
                              />
                              <span />
                            </label>
                            <button className="xp-connection-remove" onClick={() => openRemoveConnection(c)}>x</button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div className="xp-settings-card">
                    <div className="xp-settings-title">Accounts</div>
                    <button className="xp-button" onClick={() => { loadUsernames(); setUsernameManagerOpen(true); }}>
                      Link User
                    </button>
                    {settings.aliases && settings.aliases.length > 0 && (
                      <div className="xp-settings-note">
                        Linked:{" "}
                        {settings.aliases.map((a, idx) => (
                          <span key={a}>
                            <button
                              type="button"
                              className="xp-alias-link"
                              onClick={() => loadProfileByAlias(a)}
                            >
                              @{a}
                            </button>
                            {idx < settings.aliases.length - 1 ? ", " : ""}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="xp-settings-card">
                    <div className="xp-settings-title">Log Out</div>
                    <button className="xp-button" onClick={handleLogout} disabled={logoutLoading}>
                      {logoutLoading ? "Logging out..." : "Log Out"}
                    </button>
                  </div>
                </div>
              </div>
            ) : (
              <>
                <div className="xp-chat-header">
                  {isMobile && mobileView === "chat" && (
                    <button className="xp-button xp-chat-back" type="button" onClick={backToList}>
                      Back
                    </button>
                  )}
                  {selectedChat?.type === "dm" && selectedFriend && (
                    <div className="xp-chat-head-info" onClick={() => loadProfile(selectedFriend.id)}>
                      <img
                        src={avatarUrl(selectedFriend.avatar, selectedFriend.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, selectedFriend.username)}
                      />
                      <div className="xp-chat-text">
                        <div className="xp-chat-name">@{selectedFriend.username}</div>
                        <div className="xp-chat-display">
                          {nicknameMap[selectedFriend.id] || selectedFriend.display_name || selectedFriend.username}
                        </div>
                        <div className="xp-chat-status">
                          <span
                            className="xp-status-dot"
                            style={{ backgroundColor: getStatusColor(selectedFriend.status || "offline") }}
                          />
                          <span>{getStatusLabel(selectedFriend.status || "offline")}</span>
                        </div>
                      </div>
                    </div>
                  )}
                  {selectedChat?.type === "group" && selectedGroup && (
                    <div
                      className="xp-chat-head-info"
                      onClick={() => {
                        if (isMobile) setGroupMembersOpen((prev) => !prev);
                      }}
                    >
                      <div>
                        <div className="xp-chat-name">{selectedGroup.name}</div>
                        <div className="xp-chat-display">{selectedGroup.members?.length || 0} members</div>
                      </div>
                    </div>
                  )}
                  <div className="xp-chat-header-actions">
                    {selectedChat?.type === "dm" && (
                      <>
                        <button
                          className="xp-button xp-icon-btn"
                          type="button"
                          onClick={() => setShowChatProfile((prev) => !prev)}
                          title={showChatProfile ? "Hide profile" : "Show profile"}
                        >
                          <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                            <circle cx="12" cy="8" r="4" fill="none" stroke="currentColor" strokeWidth="2" />
                            <path d="M4 20c1.8-4 6-6 8-6s6.2 2 8 6" fill="none" stroke="currentColor" strokeWidth="2" />
                          </svg>
                        </button>
                        <button
                          className={`xp-button xp-icon-btn ${canJoinActiveCall ? "xp-call-join" : ""}`}
                          type="button"
                          onClick={() => (canJoinActiveCall ? joinCall(selectedChat.id) : startCall(selectedChat.id))}
                          title={canJoinActiveCall ? "Join call" : "Call"}
                        >
                          <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                            <path d="M4 3h4l2 5-3 2c1.4 2.7 3.6 4.9 6.3 6.3l2-3 5 2v4c0 1-1 2-2 2C10 21 3 14 3 6c0-1 1-2 1-3z" fill="none" stroke="currentColor" strokeWidth="2" />
                          </svg>
                        </button>
                        <button className="xp-button" type="button" onClick={() => setShowChatMenu((prev) => !prev)}>
                          ...
                        </button>
                        {showChatMenu && (
                          <div className="xp-context-menu xp-chat-context">
                            <button className="xp-context-item" onClick={() => { deleteDM(selectedChat.id); setShowChatMenu(false); }}>Delete DM</button>
                            <button className="xp-context-item" onClick={() => { closeDM(selectedChat.id); setShowChatMenu(false); }}>Close DM</button>
                            <button
                              className="xp-context-item"
                              onClick={() => {
                                if (selectedChat?.type === "dm") {
                                  const target =
                                    friendsAll.find((f) => f.id === selectedChat.id) ||
                                    friends.find((f) => f.id === selectedChat.id) ||
                                    {};
                                  setClosingModals((prev) => ({ ...prev, removeFriend: false }));
                                  setRemoveFriendConfirm({
                                    id: selectedChat.id,
                                    username: target.username,
                                    display_name: target.display_name,
                                  });
                                }
                                setShowChatMenu(false);
                              }}
                            >
                              Remove Friend
                            </button>
                            <button className="xp-context-item" onClick={() => { setDmBgModal(selectedChat.id); setShowChatMenu(false); }}>Background</button>
                          </div>
                        )}
                      </>
                    )}
                    {selectedChat?.type === "group" && (
                      <>
                        <button
                          className={`xp-button xp-icon-btn ${
                            groupCall.groupId === selectedChat.id && groupCall.status !== "idle" ? "xp-call-join" : ""
                          }`}
                          type="button"
                          onClick={() =>
                            groupCall.groupId === selectedChat.id && groupCall.status !== "idle"
                              ? joinGroupCall(selectedChat.id)
                              : startGroupCall(selectedChat.id)
                          }
                          title={
                            groupCall.groupId === selectedChat.id && groupCall.status !== "idle"
                              ? "Join call"
                              : "Call"
                          }
                        >
                          <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                            <path d="M4 3h4l2 5-3 2c1.4 2.7 3.6 4.9 6.3 6.3l2-3 5 2v4c0 1-1 2-2 2C10 21 3 14 3 6c0-1 1-2 1-3z" fill="none" stroke="currentColor" strokeWidth="2" />
                          </svg>
                        </button>
                        <button
                          className="xp-button xp-icon-btn"
                          type="button"
                          onClick={() => {
                            setAddMembersModal(selectedChat.id);
                            setAddMembersSelected({});
                          }}
                        >
                          <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                            <circle cx="9" cy="8" r="4" fill="currentColor" />
                            <path d="M2.5 20c0-4 3.2-7.2 7.2-7.2h1.8" fill="none" stroke="currentColor" strokeWidth="2" />
                            <circle cx="18" cy="14" r="4" fill="none" stroke="currentColor" strokeWidth="2" />
                            <path d="M18 12.6v2.8M16.6 14h2.8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                          </svg>
                        </button>
                      </>
                    )}
                  </div>
                </div>
                <div
                  ref={chatBodyRef}
                  className={`xp-chat-body ${chatLoading ? "loading" : ""} ${selectedChat?.type === "dm" && selectedDmBackground ? "dm-custom" : ""} ${selectedChat?.type === "dm" && deletingDmId === selectedChat.id ? "dm-deleting" : ""}`}
                  style={
                    selectedChat?.type === "dm" && selectedDmBackground
                      ? {
                          "--dm-bg-color":
                            selectedDmBackground.type === "color" ? selectedDmBackground.value : "transparent",
                          "--dm-bg-image":
                            selectedDmBackground.type === "image"
                              ? `url(${resolveMediaUrl(selectedDmBackground.value)})`
                              : selectedDmBackground.type === "gradient"
                              ? selectedDmBackground.value
                              : "none",
                          "--dm-overlay": selectedDmBackground.overlay || getDmOverlay(selectedDmBackground),
                          "--dm-bg-size":
                            selectedDmBackground.type === "image"
                              ? selectedDmBackground.fit === "tile"
                                ? "auto"
                                : selectedDmBackground.fit === "fit"
                                ? "contain"
                                : "cover"
                              : "cover",
                          "--dm-bg-repeat":
                            selectedDmBackground.type === "image" && selectedDmBackground.fit === "tile"
                              ? "repeat"
                              : "no-repeat",
                        }
                      : undefined
                  }
                >
                  {chatLoading && (
                    <div className="xp-chat-loading">
                      <div>Loading conversation...</div>
                      <div className="xp-loading-bar"><span /></div>
                    </div>
                  )}
                  {messages.length === 0 && <div className="xp-muted">Select a chat to start chatting</div>}
                  {messages.map((msg, index) => {
                    const prev = messages[index - 1];
                    const sameSender = prev && prev.sender_id === msg.sender_id;
                    const gapMs = prev ? new Date(msg.created_at).getTime() - new Date(prev.created_at).getTime() : Infinity;
                    const showAvatar = !sameSender || gapMs > 2 * 60 * 1000;
                    return (
                    <React.Fragment key={msg.id}>
                      {firstUnreadIndex !== -1 && index === firstUnreadIndex && (
                        <div className="xp-unread-divider">
                          <span>New messages</span>
                        </div>
                      )}
                      <div
                        className={`xp-message ${msg.sender_id === user.id ? "outgoing" : ""} ${msg.is_system ? "system" : ""} ${showAvatar ? "" : "grouped"} ${deletingMessageIds[msg.id] ? "deleting" : ""} ${msg.pending ? "pending" : ""}`}
                        onContextMenu={(e) => openMessageMenu(e, msg)}
                        onTouchStart={(e) => handleMessageLongPressStart(e, msg)}
                        onTouchMove={handleMessageLongPressMove}
                        onTouchEnd={handleMessageLongPressEnd}
                        onTouchCancel={handleMessageLongPressEnd}
                      >
                  <div className="xp-message-row">
                        {!msg.is_system && (
                          <div className="xp-message-avatar">
                            {showAvatar && (
                              <button
                                type="button"
                                className="xp-message-avatar-btn"
                                onClick={() => loadProfile(msg.sender_id)}
                              >
                                <img
                                  src={
                                    msg.sender_id === user.id
                                      ? avatarUrl(settings.avatar, user.username)
                                      : selectedChat?.type === "dm"
                                      ? avatarUrl(selectedFriend?.avatar, selectedFriend?.username)
                                      : selectedGroup?.members?.find((m) => m.id === msg.sender_id)?.avatar ||
                                        defaultAvatar(selectedGroup?.members?.find((m) => m.id === msg.sender_id)?.username)
                                  }
                                  alt=""
                                  onError={(e) => {
                                    const fallbackName =
                                      msg.sender_id === user.id
                                        ? user.username
                                        : selectedChat?.type === "dm"
                                        ? selectedFriend?.username
                                        : selectedGroup?.members?.find((m) => m.id === msg.sender_id)?.username;
                                    onAvatarError(e, fallbackName);
                                  }}
                                />
                              </button>
                            )}
                          </div>
                        )}
                        <div className="xp-message-content">
                          {selectedChat?.type === "group" && showAvatar && !msg.is_system && msg.sender_id !== user.id && (
                            <button className="xp-message-name" type="button" onClick={() => loadProfile(msg.sender_id)}>
                              @{selectedGroup?.members?.find((m) => m.id === msg.sender_id)?.username || "user"}
                            </button>
                          )}
                          <div className={`xp-message-bubble ${msg.type === "audio" ? "audio" : ""}`}>
                            {(msg.forwarded_from_username || msg.forwarded_from_id) && (
                              <div className="xp-forwarded-label">
                                Forwarded · {msg.forwarded_from_display || msg.forwarded_from_username || getUserById(msg.forwarded_from_id)?.username || ""}
                              </div>
                            )}
                            {isImageType(msg.type) && (
                              <div className="xp-message-media">
                                <img src={resolveMediaUrl(msg.image_url)} alt="" />
                              </div>
                            )}
                            {isAudioType(msg.type) && (
                              <div className="xp-audio-shell">
                                <button className="xp-audio-play" onClick={() => toggleAudio(msg.id)}>
                                  {audioMeta[msg.id]?.playing ? (
                                    <svg viewBox="0 0 24 24" className="xp-audio-icon" aria-hidden="true">
                                      <path d="M7 5h4v14H7zM13 5h4v14h-4z" fill="currentColor" />
                                    </svg>
                                  ) : (
                                    <svg viewBox="0 0 24 24" className="xp-audio-icon" aria-hidden="true">
                                      <path d="M8 5l11 7-11 7z" fill="currentColor" />
                                    </svg>
                                  )}
                                </button>
                                <div
                                  className="xp-audio-track"
                                  onClick={(e) => {
                                    const el = document.getElementById(`xp-audio-${msg.id}`);
                                    if (!el) return;
                                    const rect = e.currentTarget.getBoundingClientRect();
                                    const pct = (e.clientX - rect.left) / rect.width;
                                    el.currentTime = pct * (el.duration || 0);
                                  }}
                                >
                                  <div
                                    className="xp-audio-fill"
                                    style={{
                                      width: `${Math.min(100, Math.max(0, (audioMeta[msg.id]?.progress || 0) * 100))}%`,
                                    }}
                                  />
                                </div>
                                <div className="xp-audio-time">
                                  {formatDuration(audioMeta[msg.id]?.current || 0)}
                                </div>
                                <audio
                                  id={`xp-audio-${msg.id}`}
                                  src={resolveMediaUrl(msg.audio_url)}
                                  preload="metadata"
                                  onLoadedMetadata={(e) =>
                                    updateAudioMeta(msg.id, { duration: e.currentTarget.duration || 0 })
                                  }
                                  onCanPlay={(e) =>
                                    updateAudioMeta(msg.id, { duration: e.currentTarget.duration || 0 })
                                  }
                                  onTimeUpdate={(e) =>
                                    updateAudioMeta(msg.id, {
                                      duration: e.currentTarget.duration || 0,
                                      current: e.currentTarget.currentTime || 0,
                                      progress: e.currentTarget.duration
                                        ? e.currentTarget.currentTime / e.currentTarget.duration
                                        : 0,
                                    })
                                  }
                                  onPlay={(e) => {
                                    updateAudioMeta(msg.id, { playing: true });
                                    startAudioTick(msg.id, e.currentTarget);
                                  }}
                                  onPause={() => {
                                    updateAudioMeta(msg.id, { playing: false });
                                    stopAudioTick(msg.id);
                                  }}
                                  onEnded={() => {
                                    updateAudioMeta(msg.id, { playing: false, progress: 0 });
                                    stopAudioTick(msg.id);
                                  }}
                                />
                              </div>
                            )}
                            {!msg.is_system && !isImageType(msg.type) && !isAudioType(msg.type) && (
                              <div className="xp-message-text">
                                {editingMessageId === msg.id ? (
                                  <input
                                    className="xp-message-edit-input"
                                    value={editingText}
                                    onChange={(e) => setEditingText(e.target.value)}
                                    onKeyDown={(e) => {
                                      if (e.key === "Escape") {
                                        e.stopPropagation();
                                        setEditingMessageId(null);
                                        setEditingText("");
                                      } else if (e.key === "Enter") {
                                        e.preventDefault();
                                        e.stopPropagation();
                                        editMessage(msg);
                                      }
                                    }}
                                    autoFocus
                                  />
                                ) : (
                                  <>
                                    {highlightMentions(msg.body || "", getMessageContextUsers())}
                                    {msg.edited_at && <span className="xp-message-edited"> (edited)</span>}
                                  </>
                                )}
                              </div>
                            )}
                            {msg.is_system ? (
                              <div className="xp-message-text">
                                {highlightMentions(msg.body || "", getMessageContextUsers(true))}
                                {msg.type === "call" && canJoinFromMessage && (
                                  <button
                                    className="xp-join-call"
                                    type="button"
                                    onClick={() =>
                                      incomingCall?.fromId === selectedChat.id
                                        ? acceptCall()
                                        : joinCall(selectedChat.id)
                                    }
                                  >
                                    Join call
                                  </button>
                                )}
                                {msg.type === "call" && canJoinGroupFromMessage && (
                                  <button
                                    className="xp-join-call"
                                    type="button"
                                    onClick={() => joinGroupCall(selectedChat.id)}
                                  >
                                    Join call
                                  </button>
                                )}
                              </div>
                            ) : null}
                          </div>
                          {(() => {
                            const safeReactions = (msg.reactions || []).filter(
                              (r) => r && r.emoji && r.count > 0
                            );
                            if (!safeReactions.length) return null;
                            return (
                              <div className="xp-reactions">
                                {safeReactions.map((r) => (
                                  <button
                                    key={`${r.emoji}-${r.count}`}
                                    className={`xp-reaction ${r.byMe ? "mine" : ""}`}
                                    onClick={() => handleReaction(msg, r.emoji)}
                                    title="Toggle reaction"
                                    type="button"
                                  >
                                    {r.emoji} {r.count}
                                  </button>
                                ))}
                              </div>
                            );
                          })()}
                          {shouldShowTimestamp(index, messages) && (
                            <div className="xp-message-time">{formatTime(msg.created_at)}</div>
                          )}
                          {selectedChat?.type === "dm" && msg.id === lastOutgoingId && (
                            <div className="xp-message-seen">
                              {getSeenLabel(msg, selectedFriend, user?.id)}
                            </div>
                          )}
                        </div>
                      </div>
                      {selectMode && msg.sender_id === user.id && (
                        <button
                          className={`xp-select-check ${selectedMessageIds[msg.id] ? "active" : ""}`}
                          type="button"
                          onClick={() => toggleSelectMessage(msg.id)}
                        />
                      )}
                    </div>
                    </React.Fragment>
                    );
                  })}
                {selectedChat?.type === "dm" && typingByChat[selectedKey] && (
                  <div className="xp-typing">
                    @{selectedFriend?.username || "user"} is typing
                    <span className="xp-typing-dots">...</span>
                  </div>
                )}
              </div>
              {selectMode && (
                <div className="xp-select-bar">
                  <span>{Object.keys(selectedMessageIds).filter((id) => selectedMessageIds[id]).length} selected</span>
                  <div className="xp-select-actions">
                    <button className="xp-button" onClick={deleteSelectedMessages}>Delete</button>
                    <button className="xp-button" onClick={() => { setSelectMode(false); setSelectedMessageIds({}); }}>Cancel</button>
                  </div>
                </div>
              )}
              {selectedChat && (
                <div className="xp-chat-input">
                    {imagePreview && (
                      <div className="xp-image-preview">
                        <img src={imagePreview.url} alt="" />
                        <div className="xp-image-actions">
                          <button
                            className="xp-button"
                            onClick={async () => {
                              await sendImage(imagePreview.file);
                            }}
                          >
                            Send
                          </button>
                          <button className="xp-button" onClick={clearImagePreview}>
                            Cancel
                          </button>
                        </div>
                      </div>
                    )}
                    {audioPreview && (
                      <div className="xp-audio-preview">
                        <audio src={audioPreview.url} controls />
                        <div className="xp-audio-actions">
                          <button
                            className="xp-button"
                            onClick={async () => {
                              await sendAudio(audioPreview.file);
                              clearAudioPreview();
                            }}
                          >
                            Send
                          </button>
                          <button className="xp-button" onClick={clearAudioPreview}>
                            Cancel
                          </button>
                        </div>
                      </div>
                    )}
                    <div className="xp-chat-input-row">
                      {isRecording && (
                        <div className="xp-recording-bar">
                          <div className="xp-recording-indicator">
                            <span className="xp-recording-dot" />
                            <span className="xp-recording-bars">
                              <span />
                              <span />
                              <span />
                            </span>
                            <span className="xp-recording-time">
                              {formatDuration(recordElapsed)}
                            </span>
                          </div>
                          <div
                            className="xp-recording-slide"
                            onPointerDown={handleRecordSlideDown}
                            onPointerMove={handleRecordSlideMove}
                            onPointerUp={handleRecordSlideUp}
                            onPointerCancel={handleRecordSlideUp}
                            style={{ transform: `translateX(${recordSlideOffset}px)` }}
                          >
                            Slide to cancel
                          </div>
                        </div>
                      )}
                      <div className="xp-chat-composer">
                      <input
                        ref={messageInputRef}
                        value={messageInput}
                        onChange={(e) => {
                          setMessageInput(e.target.value);
                          updateMentionMenu(e.target.value, e.target.selectionStart ?? e.target.value.length);
                          handleTyping();
                        }}
                        onPaste={handlePasteImage}
                        onKeyDown={(e) => {
                          if (mentionMenu?.users?.length) {
                            if (e.key === "ArrowDown") {
                              e.preventDefault();
                              setMentionIndex((prev) => (prev + 1) % mentionMenu.users.length);
                              return;
                            }
                            if (e.key === "ArrowUp") {
                              e.preventDefault();
                              setMentionIndex((prev) => (prev - 1 + mentionMenu.users.length) % mentionMenu.users.length);
                              return;
                            }
                            if (e.key === "Enter" || e.key === "Tab") {
                              e.preventDefault();
                              applyMention(mentionMenu.users[mentionIndex] || mentionMenu.users[0]);
                              return;
                            }
                            if (e.key === "Escape") {
                              e.preventDefault();
                              setMentionMenu(null);
                              setMentionIndex(0);
                              return;
                            }
                          }
                          if (e.key === "Enter" && !e.shiftKey) {
                            e.preventDefault();
                            if (imagePreview && !messageInput.trim()) {
                              sendImage(imagePreview.file);
                              return;
                            }
                            sendMessage();
                          }
                        }}
                        onClick={(e) => updateMentionMenu(e.target.value, e.target.selectionStart ?? e.target.value.length)}
                        onKeyUp={(e) => updateMentionMenu(e.currentTarget.value, e.currentTarget.selectionStart ?? e.currentTarget.value.length)}
                        placeholder="Type a message..."
                      />
                      {mentionMenu?.users?.length ? (
                        <div className="xp-mention-menu">
                          {mentionMenu.users.map((person, idx) => (
                            <button
                              key={person.id}
                              type="button"
                              className={`xp-mention-option ${idx === mentionIndex ? "active" : ""}`}
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={() => applyMention(person)}
                            >
                              <img
                                src={avatarUrl(person.avatar, person.username)}
                                alt=""
                                onError={(e) => onAvatarError(e, person.username)}
                              />
                              <div className="xp-mention-option-text">
                                <div className="xp-mention-option-name">@{person.username}</div>
                                <div className="xp-mention-option-sub">
                                  {person.display_name || (selectedChat?.type === "group" ? "Group member" : "Direct message")}
                                </div>
                              </div>
                            </button>
                          ))}
                        </div>
                      ) : null}
                      </div>
                      <div className="xp-emoji-wrap">
                        <button
                          className="xp-button"
                          type="button"
                          onClick={() => setShowEmojiPicker((prev) => !prev)}
                        >
                          :)
                        </button>
                        {showEmojiPicker && (
                          <div className="xp-emoji-picker">
                            {EMOJI_LIST.map((emoji) => (
                              <button
                                key={emoji}
                                className="xp-emoji"
                                onClick={() => setMessageInput((prev) => `${prev}${emoji}`)}
                              >
                                {emoji}
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                      <button
                        className="xp-button xp-icon-btn"
                        type="button"
                        onClick={() => fileInputRef.current?.click()}
                        title="Send image"
                      >
                        <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                          <rect x="3" y="5" width="18" height="14" rx="2" ry="2" fill="none" stroke="currentColor" strokeWidth="2" />
                          <circle cx="8" cy="10" r="2" fill="currentColor" />
                          <path d="M21 17l-5-5-4 4-2-2-5 5" fill="none" stroke="currentColor" strokeWidth="2" />
                        </svg>
                      </button>
                      <button
                        className="xp-button xp-icon-btn"
                        type="button"
                        onClick={() => (isRecording ? stopRecording() : startRecording())}
                        title="Voice message"
                      >
                        <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                          <rect x="9" y="3" width="6" height="12" rx="3" ry="3" fill="none" stroke="currentColor" strokeWidth="2" />
                          <path d="M5 11v1a7 7 0 0 0 14 0v-1" fill="none" stroke="currentColor" strokeWidth="2" />
                          <line x1="12" y1="19" x2="12" y2="22" stroke="currentColor" strokeWidth="2" />
                          <line x1="8" y1="22" x2="16" y2="22" stroke="currentColor" strokeWidth="2" />
                        </svg>
                      </button>
                      <button className="xp-button" type="button" onClick={sendMessage}>
                        Send
                      </button>
                      <input
                        ref={fileInputRef}
                        type="file"
                        accept="image/*"
                        className="xp-hidden"
                        onChange={(e) => {
                          const file = e.target.files?.[0];
                          if (file) sendImage(file);
                          e.target.value = "";
                        }}
                      />
                      <input
                        ref={audioInputRef}
                        type="file"
                        accept="audio/*"
                        className="xp-hidden"
                        onChange={(e) => {
                          const file = e.target.files?.[0];
                          if (file) setAudioPreviewFromFile(file);
                          e.target.value = "";
                        }}
                      />
                    </div>
                    {messageError && <div className="xp-error xp-chat-error">{messageError}</div>}
                  </div>
                )}
              </>
            )}
          </section>

          {view !== "settings" && selectedChat?.type === "dm" && selectedFriend && showChatProfile && (
            <aside className="xp-chat-profile-sidebar">
              <div className="xp-chat-profile-header">
                <div className="xp-chat-profile-title">Profile</div>
                <button className="xp-chat-profile-close" onClick={() => setShowChatProfile(false)}>
                  X
                </button>
              </div>
              <div className="xp-chat-profile-body">
                <div className="xp-chat-profile-card">
                  <div className="xp-chat-profile-avatar-row">
                    <div className="xp-chat-profile-avatar-wrap">
                      <img
                        className="xp-chat-profile-avatar"
                        src={avatarUrl(selectedFriend.avatar, selectedFriend.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, selectedFriend.username)}
                      />
                      <span
                        className="xp-status-bubble"
                        style={{ backgroundColor: getStatusColor(selectedFriend.status || "offline") }}
                        title={getStatusLabel(selectedFriend.status || "offline")}
                      />
                    </div>
                    {(selectedFriend.custom_status || customStatusById[selectedFriend.id]) && (
                      <div className="xp-chat-profile-bubble">
                        {selectedFriend.custom_status || customStatusById[selectedFriend.id]}
                      </div>
                    )}
                  </div>
                  <div className="xp-chat-profile-username">@{selectedFriend.username}</div>
                  <div className="xp-chat-profile-display">
                    {nicknameMap[selectedFriend.id] || selectedFriend.display_name || selectedFriend.username}
                  </div>
                </div>
                {selectedFriend.created_at && (
                  <div className="xp-chat-profile-card">
                    <div className="xp-chat-profile-label">Member Since</div>
                    <div className="xp-chat-profile-value">{formatDateShort(selectedFriend.created_at)}</div>
                  </div>
                )}
                {(() => {
                  const mutualGroups = groups.filter((g) =>
                    (g.members || []).some((m) => m.id === selectedFriend.id)
                  );
                  const mutualFriends = selectedFriend?.mutual_friends || [];
                  return (
                    <div className="xp-chat-profile-card">
                      <button
                        className="xp-chat-profile-row"
                        onClick={() => setShowMutualGroups((prev) => !prev)}
                      >
                        <span>Mutual Groups — {mutualGroups.length}</span>
                        <span className={`xp-chat-profile-caret ${showMutualGroups ? "open" : ""}`}>›</span>
                      </button>
                      {showMutualGroups && mutualGroups.length > 0 && (
                        <div className="xp-chat-profile-list">
                          {mutualGroups.map((g) => (
                            <div key={g.id} className="xp-chat-profile-item">
                              <img src={g.avatar || defaultGroupAvatar()} alt="" />
                              <span>{g.name}</span>
                            </div>
                          ))}
                        </div>
                      )}
                      <button
                        className="xp-chat-profile-row"
                        onClick={() => setShowMutualFriends((prev) => !prev)}
                      >
                        <span>Mutual Friends — {mutualFriends.length}</span>
                        <span className={`xp-chat-profile-caret ${showMutualFriends ? "open" : ""}`}>›</span>
                      </button>
                      {showMutualFriends && mutualFriends.length > 0 && (
                        <div className="xp-chat-profile-list">
                          {mutualFriends.map((f) => (
                            <div key={f.id || f.username} className="xp-chat-profile-item">
                              <img
                                src={avatarUrl(f.avatar, f.username)}
                                alt=""
                                onError={(e) => onAvatarError(e, f.username)}
                              />
                              <span>{f.display_name || f.username}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })()}
                {chatProfileConnections.length > 0 && (
                  <div className="xp-chat-profile-card">
                    <div className="xp-chat-profile-label">Connections</div>
                    <div
                      className={`xp-chat-profile-connections ${
                        chatProfileConnections.length > 5 ? "scrollable" : ""
                      }`}
                    >
                      {chatProfileConnections.map((c) => (
                        <a
                          key={c.id}
                          className="xp-chat-profile-conn"
                          href={c.url || "#"}
                          target="_blank"
                          rel="noreferrer"
                        >
                          <img
                            src={CONNECTION_SERVICES.find((s) => s.id === c.service)?.icon}
                            alt=""
                          />
                          <div className="xp-chat-profile-conn-text">
                            <div className="xp-chat-profile-conn-handle">{c.handle}</div>
                          </div>
                        </a>
                      ))}
                    </div>
                  </div>
                )}
                <button className="xp-chat-profile-footer" onClick={() => loadProfile(selectedFriend.id)}>
                  View Full Profile
                </button>
              </div>
            </aside>
          )}

          {view !== "settings" && selectedChat?.type === "group" && selectedGroup && (
            <aside className="xp-group-sidebar">
              <div className="xp-group-members-header">
                Members {selectedGroup.members?.length || 0}/10
              </div>
              <div className="xp-group-members">
                {(selectedGroup.members || []).map((m) => (
                  <button
                    key={m.id}
                    className="xp-group-member"
                    onClick={() => loadProfile(m.id)}
                    onContextMenu={(e) => openGroupMemberMenu(e, m.id)}
                  >
                    <img
                      src={avatarUrl(m.avatar, m.username)}
                      alt=""
                      onError={(e) => onAvatarError(e, m.username)}
                    />
                    <div>
                      <div className="xp-user-name">
                        {m.display_name || m.username}
                        {selectedGroup.owner_id === m.id && (
                          <span className="xp-owner-crown" aria-label="Group owner">
                            <svg viewBox="0 0 24 24" aria-hidden="true">
                              <path
                                d="M3 8l4 3 5-6 5 6 4-3v9H3V8z"
                                fill="currentColor"
                              />
                              <rect x="4" y="17" width="16" height="3" rx="1" fill="currentColor" />
                            </svg>
                          </span>
                        )}
                      </div>
                      {(m.custom_status || customStatusById[m.id]) && (
                        <div className="xp-custom-status">{m.custom_status || customStatusById[m.id]}</div>
                      )}
                    </div>
                  </button>
                ))}
              </div>
            </aside>
          )}
        </div>
      </div>
      )}

      {showMessageMenu && (
        <div
          className="xp-context-menu"
          style={{ left: showMessageMenu.x, top: showMessageMenu.y }}
          onClick={(e) => e.stopPropagation()}
        >
          <button
            className="xp-context-item"
            onClick={() => setReactionPickerFor(showMessageMenu.message.id)}
          >
            Add Reaction
          </button>
          {reactionPickerFor === showMessageMenu.message.id && (
            <div className="xp-reaction-picker">
              {REACTION_EMOJIS.map((emoji) => (
                <button
                  key={emoji}
                  className="xp-reaction-btn"
                  onClick={() => {
                    handleReaction(showMessageMenu.message, emoji);
                    setReactionPickerFor(null);
                    setShowMessageMenu(null);
                  }}
                >
                  {emoji}
                </button>
              ))}
            </div>
          )}
          <button
            className="xp-context-item"
            onClick={() => {
              const selectedIds = Object.keys(selectedMessageIds).filter((id) => selectedMessageIds[id]);
              const msgs =
                selectMode && selectedIds.length > 0
                  ? messages.filter((m) => selectedIds.includes(String(m.id)))
                  : [showMessageMenu.message];
              openForwardModal(msgs);
              setShowMessageMenu(null);
            }}
          >
            Forward
          </button>
          {showMessageMenu.message?.sender_id === user.id && (
            <button
              className="xp-context-item"
              onClick={() => {
                setEditingMessageId(showMessageMenu.message.id);
                setEditingText(showMessageMenu.message.body || "");
                setShowMessageMenu(null);
              }}
            >
              Edit
            </button>
          )}
          {showMessageMenu.message?.sender_id === user.id && (
            <button className="xp-context-item" onClick={() => { setSelectMode(true); setShowMessageMenu(null); }}>Select</button>
          )}
          {showMessageMenu.message?.sender_id === user.id && (
            <button
              className="xp-context-item danger"
              onClick={() => {
                const selectedIds = Object.keys(selectedMessageIds).filter((id) => selectedMessageIds[id]);
                if (selectMode && selectedIds.length > 0) {
                  deleteSelectedMessages();
                } else {
                  deleteMessage(showMessageMenu.message.id);
                }
                setShowMessageMenu(null);
              }}
            >
              Delete
            </button>
          )}
        </div>
      )}

      {showProfileMenu && (
        <div
          className="xp-context-menu"
          style={{ left: showProfileMenu.x, top: showProfileMenu.y }}
          onClick={(e) => e.stopPropagation()}
          onMouseLeave={() => {
            setFriendMuteMenu(null);
            setFriendGameMenu(null);
          }}
        >
          {(() => {
            const uid = Number(showProfileMenu.userId);
            const removed = removedFriendIds.includes(uid);
            const isPending = pendingFriendIds.includes(uid);
            const isFriend =
              !removed &&
              !isPending &&
              friendsAll.some((f) => Number(f.id) === uid);
            return (
              <>
                <button
                  className="xp-context-item"
                  onClick={() => {
                    loadProfile(showProfileMenu.userId);
                    setShowProfileMenu(null);
                  }}
                >
                  View profile
                </button>
                {isFriend ? (
                  <button
                    className="xp-context-item"
                    onClick={() => {
                      const target =
                        friendsAll.find((f) => f.id === showProfileMenu.userId) ||
                        friends.find((f) => f.id === showProfileMenu.userId) ||
                        {};
                      setClosingModals((prev) => ({ ...prev, removeFriend: false }));
                      setRemoveFriendConfirm({
                        id: showProfileMenu.userId,
                        username: target.username,
                        display_name: target.display_name,
                      });
                      setShowProfileMenu(null);
                    }}
                  >
                    Remove Friend
                  </button>
                ) : (
                  <button
                    className={`xp-context-item ${isPending ? "pending" : ""}`}
                    onClick={() => {
                      const target =
                        friendsAll.find((f) => f.id === showProfileMenu.userId) ||
                        friends.find((f) => f.id === showProfileMenu.userId) ||
                        allUsers.find((u) => u.id === showProfileMenu.userId) ||
                        {};
                      if (isPending) {
                        cancelFriendRequestFromProfile(target);
                      } else {
                        sendFriendRequestFromProfile(target);
                      }
                      setShowProfileMenu(null);
                    }}
                  >
                    {isPending ? "Cancel Request" : "Add Friend"}
                  </button>
                )}
              </>
            );
          })()}
          <button
            className="xp-context-item"
            onClick={() => {
              setNicknameModal({
                userId: showProfileMenu.userId,
                value: nicknameMap[showProfileMenu.userId] || "",
              });
              setShowProfileMenu(null);
            }}
          >
            Set nickname
          </button>
          <button
            className="xp-context-item"
            onClick={() => {
              closeDM(showProfileMenu.userId);
              setFriendMuteMenu(null);
              setFriendGameMenu(null);
              setShowProfileMenu(null);
            }}
          >
            Close DM
          </button>
          {nicknameMap[showProfileMenu.userId] && (
            <button
              className="xp-context-item"
              onClick={() => {
                saveNickname(showProfileMenu.userId, "");
                setShowProfileMenu(null);
              }}
            >
              Clear nickname
            </button>
          )}
          {(() => {
            const key = chatKey("dm", showProfileMenu.userId);
            const muted = isChatMutedValue(key);
            const display = nicknameMap[showProfileMenu.userId] ||
              friendsAll.find((f) => f.id === showProfileMenu.userId)?.display_name ||
              friends.find((f) => f.id === showProfileMenu.userId)?.display_name ||
              friendsAll.find((f) => f.id === showProfileMenu.userId)?.username ||
              friends.find((f) => f.id === showProfileMenu.userId)?.username ||
              "user";
            return (
              <div style={{ position: "relative" }}>
                <button
                  className="xp-context-item"
                  onClick={() => {
                    if (muted) {
                      setChatMute(key, null);
                      setFriendMuteMenu(null);
                      setShowProfileMenu(null);
                      return;
                    }
                    setFriendMuteMenu((prev) =>
                      prev?.userId === showProfileMenu.userId
                        ? null
                        : { userId: showProfileMenu.userId }
                    );
                  }}
                >
                  {muted ? `Unmute @${display}` : `Mute @${display}`}
                </button>
                {friendMuteMenu?.userId === showProfileMenu.userId && (
                  <div className="xp-context-submenu">
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 15 * 60 * 1000);
                        setFriendMuteMenu(null);
                        setShowProfileMenu(null);
                      }}
                    >
                      For 15 Minutes
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 60 * 60 * 1000);
                        setFriendMuteMenu(null);
                        setShowProfileMenu(null);
                      }}
                    >
                      For 1 Hour
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 3 * 60 * 60 * 1000);
                        setFriendMuteMenu(null);
                        setShowProfileMenu(null);
                      }}
                    >
                      For 3 Hours
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 8 * 60 * 60 * 1000);
                        setFriendMuteMenu(null);
                        setShowProfileMenu(null);
                      }}
                    >
                      For 8 Hours
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 24 * 60 * 60 * 1000);
                        setFriendMuteMenu(null);
                        setShowProfileMenu(null);
                      }}
                    >
                      For 24 Hours
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, "forever");
                        setFriendMuteMenu(null);
                        setShowProfileMenu(null);
                      }}
                    >
                      Until I turn it back on
                    </button>
                  </div>
                )}
              </div>
            );
          })()}
          {(() => {
            const uid = Number(showProfileMenu.userId);
            const removed = removedFriendIds.includes(uid);
            const isPending = pendingFriendIds.includes(uid);
            const isFriend =
              !removed &&
              !isPending &&
              friendsAll.some((f) => Number(f.id) === uid);
            if (!isFriend) return null;
            return (
              <div style={{ position: "relative" }}>
                <button
                  className="xp-context-item"
                  onClick={() => {
                    setFriendGameMenu((prev) =>
                      prev?.userId === showProfileMenu.userId ? null : { userId: showProfileMenu.userId }
                    );
                  }}
                >
                  Invite to Game
                </button>
                {friendGameMenu?.userId === showProfileMenu.userId && (
                  <div className="xp-context-submenu">
                    <button
                      className="xp-context-item"
                      onClick={() => openGameInviteFromProfile(showProfileMenu.userId, "blackjack")}
                    >
                      Blackjack
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => openGameInviteFromProfile(showProfileMenu.userId, "coinflip")}
                    >
                      Coinflip
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => openGameInviteFromProfile(showProfileMenu.userId, "bigbank")}
                    >
                      Big Bank Small Bank
                    </button>
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      )}

      {groupMemberMenu && selectedGroup && (
        <div
          className="xp-context-menu"
          style={{ left: groupMemberMenu.x, top: groupMemberMenu.y }}
        >
          <button className="xp-context-item" onClick={() => loadProfile(groupMemberMenu.memberId)}>
            View profile
          </button>
          {selectedGroup.owner_id === user.id && groupMemberMenu.memberId !== user.id && (
            <>
              <button
                className="xp-context-item"
                onClick={() => removeGroupMember(selectedGroup.id, groupMemberMenu.memberId)}
              >
                Remove from group
              </button>
              <button
                className="xp-context-item"
                onClick={() => transferGroupOwner(selectedGroup.id, groupMemberMenu.memberId)}
              >
                Make group owner
              </button>
            </>
          )}
        </div>
      )}

      {profileOpen && profileUser && (
        <div
          className={`xp-modal-overlay ${closingModals.profile ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("profile", () => setProfileOpen(false))}
        >
          <div className={`xp-modal xp-profile-modal-frame ${closingModals.profile ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Profile</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("profile", () => setProfileOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-profile-modal">
                <div className="xp-profile-modal-left">
                  <div className="xp-profile-modal-avatar">
                    {profileHasStory ? (
                      <div className="xp-story-wrap xp-profile-story">
                        <button
                          type="button"
                          className={`xp-story-ring active${profileHasUnviewed ? "" : " viewed"}`}
                          onClick={() => openStoryByUserId(profileUser.id)}
                        >
                          <img
                            src={avatarUrl(profileUser.avatar, profileUser.username)}
                            alt=""
                            onError={(e) => onAvatarError(e, profileUser.username)}
                          />
                        </button>
                      </div>
                    ) : (
                      <div className="xp-profile-avatar">
                        <img
                          src={avatarUrl(profileUser.avatar, profileUser.username)}
                          alt=""
                          onError={(e) => onAvatarError(e, profileUser.username)}
                        />
                      </div>
                    )}
                  </div>
                  {profileUser.bio && (
                    <div className="xp-profile-bio-left-wrap">
                      <div className="xp-private-title">About me</div>
                      <div className="xp-profile-bio xp-profile-bio-left">{profileUser.bio}</div>
                    </div>
                  )}
                  {profileConnections.length > 0 && (
                    <div className="xp-profile-connections-left">
                      <div className="xp-private-title">Connections</div>
                      <div className="xp-connection-list cards profile">
                        {profileConnections.map((c) => (
                          <a
                            key={c.id}
                            className="xp-connection-card"
                            href={c.url || "#"}
                            target="_blank"
                            rel="noreferrer"
                          >
                            <div className="xp-connection-card-main">
                              <img
                                className="xp-connection-icon"
                                src={CONNECTION_SERVICES.find((s) => s.id === c.service)?.icon}
                                alt=""
                              />
                              <div className="xp-connection-card-text">
                                <div className="xp-connection-handle">{c.handle}</div>
                                <div className="xp-connection-service">{c.service}</div>
                              </div>
                            </div>
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                <div className="xp-profile-modal-right">
                  <div className="xp-profile-identity">
                    <div className="xp-profile-name-row">
                      <div className="xp-user-name">@{profileUser.username}</div>
                      {profileUser.custom_status && (
                        <div className="xp-custom-status xp-inline-status">
                          {profileUser.custom_status}
                        </div>
                      )}
                    </div>
                    <div className="xp-user-display">{profileUser.display_name || profileUser.username}</div>
                    {profileUser.aliases && profileUser.aliases.length > 0 && (
                      <div className="xp-user-aliases">
                        also{" "}
                        {profileUser.aliases.map((a, idx) => (
                          <span key={a}>
                            <button
                              type="button"
                              className="xp-mention xp-mention-inline"
                              onClick={() => loadProfileByAlias(a)}
                            >
                              @{a}
                            </button>
                            {idx < profileUser.aliases.length - 1 ? ", " : ""}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="xp-profile-status-block">
                    <div className="xp-profile-status-row">
                      <span
                        className="xp-status-dot"
                        style={{ color: getStatusColor(profileUser.status || "offline") }}
                      />
                      <span>{getStatusLabel(profileUser.status || "offline")}</span>
                    </div>
                    {profileUser.last_seen && profileUser.status !== "online" && (
                      <div className="xp-profile-last-seen">
                        Last online: {formatDateTime(profileUser.last_seen)}
                      </div>
                    )}
                  </div>
                  {profileSong && (profileSong.title || profileSong.artist || profileSong.coverUrl || profileSong.audioUrl) && (
                    <div className="xp-profile-music-card">
                      <div className={`xp-profile-music-cover ${profileSong.coverUrl ? "" : "placeholder"}`}>
                        {profileSong.coverUrl ? (
                          <img
                            src={resolveMediaUrl(profileSong.coverUrl)}
                            alt=""
                            onError={(e) => {
                              e.currentTarget.style.display = "none";
                            }}
                          />
                        ) : (
                          <span>???</span>
                        )}
                      </div>
                      <div className="xp-profile-music-info">
                        <div className="xp-profile-music-title">
                          {profileSong.title || "Untitled track"}
                        </div>
                        <div className="xp-profile-music-artist">
                          {profileSong.artist || "Unknown artist"}
                        </div>
                        {profileSong.album && (
                          <div className="xp-profile-music-album">{profileSong.album}</div>
                        )}
                      </div>
                      <div className="xp-profile-music-actions">
                        <button
                          className={`xp-profile-music-button ${profileSongHasAudio ? "" : "disabled"}`}
                          onClick={() => toggleProfileSong(profileUser)}
                          disabled={!profileSongHasAudio}
                        >
                          {profileSongIsPlaying ? "Pause" : "Play"}
                        </button>
                      </div>
                    </div>
                  )}
                  <div className="xp-profile-divider" />
                  <div className="xp-private-note">
                    <div className="xp-private-title">Private note</div>
                    <textarea
                      placeholder="Only you can see this note..."
                      value={profileNote}
                      onChange={(e) => {
                        setProfileNote(e.target.value);
                        apiFetch(`/api/notes/${profileUser.id}`, {
                          method: "PATCH",
                          body: JSON.stringify({ note: e.target.value }),
                        }).catch(() => {});
                      }}
                    />
                  </div>
                  <div className="xp-profile-actions">
                    {profileUser.id !== user?.id && (() => {
                      const removed = removedFriendIds.includes(Number(profileUser.id));
                      const isPending = pendingFriendIds.includes(Number(profileUser.id));
                      const isFriend =
                        !removed &&
                        !isPending &&
                        (profileIsFriend ||
                          friendsAll.some((f) => Number(f.id) === Number(profileUser.id)));
                      return (
                        <>
                          <button
                            className="xp-button"
                            onClick={async () => {
                              await openDmFromProfile(profileUser);
                              setProfileOpen(false);
                            }}
                          >
                            Message
                          </button>
                          {isFriend ? (
                            <button
                              className="xp-button xp-icon-btn"
                              title="Remove Friend"
                              onClick={() => setRemoveFriendConfirm(profileUser)}
                              type="button"
                            >
                              <img src={removeIcon} alt="Remove friend" />
                            </button>
                          ) : (
                            <button
                              className={`xp-button xp-icon-btn ${isPending ? "pending" : ""}`}
                              title={pendingFriendIds.includes(Number(profileUser.id)) ? "Cancel Request" : "Add Friend"}
                              onClick={() =>
                                pendingFriendIds.includes(Number(profileUser.id))
                                  ? cancelFriendRequestFromProfile(profileUser)
                                  : sendFriendRequestFromProfile(profileUser)
                              }
                              type="button"
                            >
                              {pendingFriendIds.includes(Number(profileUser.id)) ? (
                                <img src={pendingIcon} alt="Pending request" />
                              ) : (
                                <img src={addIcon} alt="Add friend" />
                              )}
                            </button>
                          )}
                        </>
                      );
                    })()}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {removeFriendConfirm && (
        <div className={`xp-modal-overlay ${closingModals.removeFriend ? "closing" : ""}`} onClick={() => closeModalWithAnim("removeFriend", () => setRemoveFriendConfirm(null))}>
          <div className={`xp-modal xp-modal-wide ${closingModals.removeFriend ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Remove “{removeFriendConfirm.display_name || removeFriendConfirm.username}”</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("removeFriend", () => setRemoveFriendConfirm(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-muted">
                Are you sure you want to remove {removeFriendConfirm.display_name || removeFriendConfirm.username} from your friends?
              </div>
              <div className="xp-button-row" style={{ marginTop: 12 }}>
                <button className="xp-button" onClick={() => closeModalWithAnim("removeFriend", () => setRemoveFriendConfirm(null))}>Cancel</button>
                <button
                  className="xp-button danger"
                  onClick={() => {
                    removeFriend(removeFriendConfirm.id);
                    closeModalWithAnim("removeFriend", () => setRemoveFriendConfirm(null));
                  }}
                >
                  Remove Friend
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {leaveGroupConfirm && (
        <div className={`xp-modal-overlay ${closingModals.leaveGroup ? "closing" : ""}`} onClick={() => closeModalWithAnim("leaveGroup", () => setLeaveGroupConfirm(null))}>
          <div className={`xp-modal xp-modal-wide ${closingModals.leaveGroup ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Leave "{leaveGroupConfirm.name}"</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("leaveGroup", () => setLeaveGroupConfirm(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-muted">
                Are you sure you want to leave {leaveGroupConfirm.name}? You will need a new invite to rejoin.
              </div>
              <div className="xp-button-row" style={{ marginTop: 12 }}>
                <button className="xp-button" onClick={() => closeModalWithAnim("leaveGroup", () => setLeaveGroupConfirm(null))}>Cancel</button>
                <button
                  className="xp-button danger"
                  onClick={() => {
                    leaveGroup(leaveGroupConfirm.id);
                    closeModalWithAnim("leaveGroup", () => setLeaveGroupConfirm(null));
                  }}
                >
                  Leave Group
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {nicknameModal && (
        <div
          className={`xp-modal-overlay ${closingModals.nickname ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("nickname", () => setNicknameModal(null))}
        >
          <div className={`xp-modal xp-modal-small ${closingModals.nickname ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Set nickname</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("nickname", () => setNicknameModal(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Nickname
                <input
                  value={nicknameModal.value || ""}
                  onChange={(e) => setNicknameModal((prev) => ({ ...prev, value: e.target.value }))}
                  placeholder={
                    (() => {
                      const target =
                        friendsAll.find((f) => f.id === nicknameModal.userId) ||
                        friends.find((f) => f.id === nicknameModal.userId) ||
                        {};
                      return target.display_name || target.username || "nickname";
                    })()
                  }
                />
              </label>
              <div className="xp-button-row">
                <button className="xp-button" onClick={() => closeModalWithAnim("nickname", () => setNicknameModal(null))}>Cancel</button>
                <button
                  className={`xp-button ${savingFlags.nickname ? "is-saving" : ""}`}
                  onClick={() => {
                    saveNicknameWithDelay(nicknameModal.userId, (nicknameModal.value || "").trim());
                    setTimeout(() => {
                      closeModalWithAnim("nickname", () => setNicknameModal(null));
                    }, 320);
                  }}
                  disabled={savingFlags.nickname}
                >
                  {savingFlags.nickname ? "Saving..." : "Save"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}


      {connectionModalOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.connection ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("connection", () => setConnectionModalOpen(false))}
        >
          <div className={`xp-modal xp-modal-small ${closingModals.connection ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Link account</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("connection", () => setConnectionModalOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-connection-modal-head">
                <img className="xp-connection-icon" src={CONNECTION_SERVICES.find((s) => s.id === connectionService)?.icon} alt="" />
                <div className="xp-connection-modal-title">{connectionService}</div>
              </div>
              <label>
                {connectionService === "website" ? "Site" : "Handle"}
                <input
                  value={connectionHandle}
                  placeholder={connectionService === "website" ? "domain" : "@handle"}
                  onChange={(e) => setConnectionHandle(e.target.value)}
                />
              </label>
              <label>
                Link (optional)
                <input
                  value={connectionUrl}
                  placeholder={
                    connectionService === "spotify"
                      ? "https://open.spotify.com/track/..."
                      : ""
                  }
                  onChange={(e) => setConnectionUrl(e.target.value)}
                />
              </label>
              {connectionService === "spotify" && (
                <div className="xp-muted">
                  Paste a Spotify track link here. The profile music card will sync its title, artist, and cover from this link automatically.
                </div>
              )}
              {connectionError && <div className="xp-error">{connectionError}</div>}
              <div className="xp-button-row">
                <button className="xp-button" onClick={createConnection}>Add</button>
                <button className="xp-button" onClick={() => closeModalWithAnim("connection", () => setConnectionModalOpen(false))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {groupContextMenu && (
        <div
          className="xp-context-menu"
          style={{ top: groupContextMenu.y, left: groupContextMenu.x, position: "fixed", zIndex: 50 }}
          onClick={(e) => e.stopPropagation()}
          onMouseLeave={() => setGroupMuteMenu(null)}
        >
          {(() => {
            const key = chatKey("group", groupContextMenu.group.id);
            const muted = isChatMutedValue(key);
            return muted ? (
              <button
                className="xp-context-item"
                onClick={() => {
                  setChatMute(key, null);
                  setGroupContextMenu(null);
                }}
              >
                Unmute chat
              </button>
            ) : (
              <div style={{ position: "relative" }}>
                <button
                  className="xp-context-item"
                  onClick={() => {
                    setChatMute(key, "forever");
                    setGroupMuteMenu(null);
                    setGroupContextMenu(null);
                  }}
                  onMouseEnter={() =>
                    setGroupMuteMenu((prev) =>
                      prev?.groupId === groupContextMenu.group.id ? prev : { groupId: groupContextMenu.group.id }
                    )
                  }
                >
                  Mute
                </button>
                {groupMuteMenu?.groupId === groupContextMenu.group.id && (
                  <div className="xp-context-submenu">
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 15 * 60 * 1000);
                        setGroupMuteMenu(null);
                        setGroupContextMenu(null);
                      }}
                    >
                      For 15 Minutes
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 60 * 60 * 1000);
                        setGroupMuteMenu(null);
                        setGroupContextMenu(null);
                      }}
                    >
                      For 1 Hour
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 3 * 60 * 60 * 1000);
                        setGroupMuteMenu(null);
                        setGroupContextMenu(null);
                      }}
                    >
                      For 3 Hours
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 8 * 60 * 60 * 1000);
                        setGroupMuteMenu(null);
                        setGroupContextMenu(null);
                      }}
                    >
                      For 8 Hours
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, 24 * 60 * 60 * 1000);
                        setGroupMuteMenu(null);
                        setGroupContextMenu(null);
                      }}
                    >
                      For 24 Hours
                    </button>
                    <button
                      className="xp-context-item"
                      onClick={() => {
                        setChatMute(key, "forever");
                        setGroupMuteMenu(null);
                        setGroupContextMenu(null);
                      }}
                    >
                      Until I turn it back on
                    </button>
                  </div>
                )}
              </div>
            );
          })()}
          <button
            className="xp-context-item"
            onClick={() => {
              setEditGroupModal(groupContextMenu.group);
              setEditGroupName(groupContextMenu.group.name || "");
              setEditGroupAvatar(groupContextMenu.group.avatar || "");
              setGroupContextMenu(null);
            }}
          >
            Edit Group
          </button>
          <button
            className="xp-context-item danger"
            onClick={() => {
              setLeaveGroupConfirm({
                id: groupContextMenu.group.id,
                name: groupContextMenu.group.name || "this group",
              });
              setGroupContextMenu(null);
            }}
          >
            Leave Group
          </button>
        </div>
      )}

      {editGroupModal && (
        <div
          className={`xp-modal-overlay ${closingModals.editGroup ? "closing" : ""}`}
          onClick={requestCloseEditGroup}
        >
          <div className={`xp-modal xp-modal-small ${closingModals.editGroup ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Edit Group</span>
              <button className="xp-modal-close" onClick={requestCloseEditGroup}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-group-avatar-edit">
                <button
                  className="xp-group-avatar-button"
                  onClick={() => document.getElementById("xp-group-avatar-input")?.click()}
                >
                  <img src={editGroupAvatar || editGroupModal.avatar || defaultGroupAvatar()} alt="" />
                  <span className="xp-avatar-hover">Change</span>
                  <span className="xp-avatar-pencil" aria-hidden="true">
                    <svg viewBox="0 0 24 24">
                      <path
                        d="M3 17.25V21h3.75L17.8 9.95l-3.75-3.75L3 17.25Zm14.7-9.5 1.6-1.6a1 1 0 0 0 0-1.4l-1.9-1.9a1 1 0 0 0-1.4 0l-1.6 1.6 3.3 3.3Z"
                        fill="currentColor"
                      />
                    </svg>
                  </span>
                </button>
                <input
                  id="xp-group-avatar-input"
                  type="file"
                  accept="image/*"
                  className="xp-hidden"
                  onChange={(e) => {
                    const file = e.target.files?.[0];
                    if (!file) return;
                    openAvatarEditor(file, { type: "group", groupId: editGroupModal?.id || null });
                  }}
                />
              </div>
              <label>
                Group name
                <input value={editGroupName} onChange={(e) => setEditGroupName(e.target.value)} />
              </label>
              <div className="xp-button-row">
                <button
                  className="xp-button"
                  onClick={() => {
                    updateGroup(editGroupModal.id, editGroupName, editGroupAvatar);
                    setEditGroupModal(null);
                  }}
                >
                  Save
                </button>
                <button className="xp-button" onClick={requestCloseEditGroup}>
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {editGroupConfirmOpen && (
        <div className={`xp-modal-overlay ${closingModals.editGroupConfirm ? "closing" : ""}`} onClick={() => closeModalWithAnim("editGroupConfirm", () => setEditGroupConfirmOpen(false))}>
          <div className={`xp-modal xp-modal-small ${closingModals.editGroupConfirm ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Discard unsaved changes?</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("editGroupConfirm", () => setEditGroupConfirmOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div>You have unsaved changes. Are you sure you want to discard them?</div>
              <div className="xp-button-row">
                <button className="xp-button" onClick={() => closeModalWithAnim("editGroupConfirm", () => setEditGroupConfirmOpen(false))}>Keep Editing</button>
                <button className="xp-button" onClick={() => {
                  setEditGroupConfirmOpen(false);
                  closeModalWithAnim("editGroup", () => setEditGroupModal(null));
                }}>Discard</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {addMembersModal && (
        <div className={`xp-modal-overlay ${closingModals.addMembers ? "closing" : ""}`} onClick={() => closeModalWithAnim("addMembers", () => setAddMembersModal(null))}>
          <div className={`xp-modal xp-modal-small ${closingModals.addMembers ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Add Members</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("addMembers", () => setAddMembersModal(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-group-picker-list xp-forward-grid">
                {friends
                  .filter((f) => !selectedGroup?.members?.some((m) => m.id === f.id))
                  .map((f) => (
                    <button
                      key={f.id}
                      type="button"
                      className={`xp-forward-item ${addMembersSelected[f.id] ? "active" : ""}`}
                      onClick={() => setAddMembersSelected((prev) => ({ ...prev, [f.id]: !prev[f.id] }))}
                    >
                      <img
                        src={avatarUrl(f.avatar, f.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, f.username)}
                      />
                      <div>
                        <div className="xp-forward-name">@{f.username}</div>
                        <div className="xp-forward-sub">{f.display_name || f.username}</div>
                      </div>
                    </button>
                  ))}
              </div>
              <div className="xp-muted">
                {(selectedGroup?.members?.length || 0) + Object.values(addMembersSelected).filter(Boolean).length}/10
              </div>
              <div className="xp-button-row">
                <button
                  className="xp-button"
                  onClick={submitAddMembers}
                  disabled={(selectedGroup?.members?.length || 0) + Object.values(addMembersSelected).filter(Boolean).length > 10}
                >
                  Add
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("addMembers", () => setAddMembersModal(null))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {emailEditOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.email ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("email", () => setEmailEditOpen(false))}
        >
          <div className={`xp-modal xp-modal-small ${closingModals.email ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Change email</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("email", () => setEmailEditOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Email
                <input
                  type="email"
                  value={settings.email}
                  onChange={(e) => setSettings((prev) => ({ ...prev, email: e.target.value }))}
                />
              </label>
              <label>
                Password
                <input
                  type="password"
                  value={emailPassword}
                  onChange={(e) => setEmailPassword(e.target.value)}
                />
              </label>
              <div className="xp-button-row">
                <button className={`xp-button ${savingFlags.email ? "is-saving" : ""}`} onClick={saveEmail} disabled={savingFlags.email}>
                  {savingFlags.email ? "Saving..." : "Save"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("email", () => setEmailEditOpen(false))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {statusEditOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.status ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("status", () => setStatusEditOpen(false))}
        >
          <div className={`xp-modal xp-modal-small ${closingModals.status ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Set Status</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("status", () => setStatusEditOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Custom status
                <input
                  value={customStatusInput}
                  maxLength={MAX_CUSTOM_STATUS}
                  onChange={(e) => setCustomStatusInput(e.target.value)}
                />
              </label>
              <div className="xp-button-row">
                <button className={`xp-button ${statusSaving ? "is-saving" : ""}`} onClick={saveCustomStatus} disabled={statusSaving}>
                  {statusSaving ? "Saving..." : "Save"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("status", () => setStatusEditOpen(false))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {removeConnectionId && (
        <div
          className={`xp-modal-overlay ${closingModals.removeConnection ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("removeConnection", () => setRemoveConnectionId(null))}
        >
          <div className={`xp-modal ${closingModals.removeConnection ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>
                Unlink {connections.find((c) => c.id === removeConnectionId)?.service || "connection"}
              </span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("removeConnection", () => setRemoveConnectionId(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div>
                You will no longer have this connection linked to your profile.
              </div>
              <div className="xp-button-row">
                <button className="xp-button" onClick={() => removeConnection(removeConnectionId)}>Unlink</button>
                <button className="xp-button" onClick={() => closeModalWithAnim("removeConnection", () => setRemoveConnectionId(null))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {usernameManagerOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.usernameManager ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("usernameManager", () => setUsernameManagerOpen(false))}
        >
          <div className={`xp-modal xp-modal-wide ${closingModals.usernameManager ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Usernames</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("usernameManager", () => setUsernameManagerOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body xp-username-manager">
              <div className={`xp-username-primary ${primarySwitching ? "switching" : ""}`}>Primary: @{settings.username}</div>
              <div className="xp-alias-list xp-alias-scroll">
                {(aliasData?.aliases || []).map((a) => (
                  <div key={a.username} className="xp-alias-row">
                    <div className="xp-alias-name">@{a.username}</div>
                    <div className="xp-alias-actions">
                      <button
                        className="xp-button xp-alias-menu-button"
                        onClick={(e) => {
                          const rect = e.currentTarget.getBoundingClientRect();
                          setAliasMenuOpen(
                            aliasMenuOpen?.username === a.username
                              ? null
                              : { username: a.username, x: rect.right + 6, y: rect.top }
                          );
                        }}
                      >
                        ...
                      </button>
                    </div>
                  </div>
                ))}
              </div>
              {aliasMenuOpen && (
                <div
                  className="xp-alias-menu-float"
                  style={{ left: aliasMenuOpen.x, top: aliasMenuOpen.y }}
                  onClick={(e) => e.stopPropagation()}
                >
                  <button className="xp-button" onClick={() => { setPrimaryModal({ username: aliasMenuOpen.username }); setPrimaryError(""); setPrimaryPassword(""); setAliasMenuOpen(null); }}>Set primary</button>
                  <button className="xp-button" onClick={() => { setTransferModal({ username: aliasMenuOpen.username }); setAliasMenuOpen(null); }}>Transfer</button>
                  <button className="xp-button" onClick={() => { setRemoveAliasModal({ username: aliasMenuOpen.username }); setAliasMenuOpen(null); }}>Remove</button>
                </div>
              )}
              {aliasData?.incoming?.length > 0 && (
                <div className="xp-transfer-list">
                  {aliasData.incoming.map((t) => (
                    <div key={t.id} className="xp-transfer-row">
                      <div>@{t.from_username} wants to transfer @{t.username}</div>
                      <div className="xp-transfer-actions">
                        <button className="xp-button" onClick={() => acceptTransfer(t.id)}>Accept</button>
                        <button className="xp-button" onClick={() => denyTransfer(t.id)}>Decline</button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              <div className="xp-alias-create">
                <div className="xp-settings-label">Claim alias</div>
                <div className="xp-alias-fields">
                  <input
                    value={aliasCheck.username}
                    placeholder="username"
                    onChange={(e) => setAliasCheck({ username: e.target.value, available: null, error: "" })}
                    minLength={USERNAME_MIN}
                    maxLength={USERNAME_MAX}
                  />
                  <input
                    type="password"
                    value={aliasPassword}
                    placeholder="Current password"
                    onChange={(e) => setAliasPassword(e.target.value)}
                  />
                </div>
                <div className="xp-button-row">
                  <button className="xp-button" onClick={checkAliasAvailability} disabled={aliasClaiming}>Check</button>
                  <button className="xp-button" onClick={claimAlias} disabled={aliasCheck.available === false || aliasClaiming}>
                    {aliasClaiming ? "Claiming..." : "Claim"}
                  </button>
                </div>
                {aliasCheck.available === true && <div className="xp-success">Available</div>}
                {aliasCheck.available === false && <div className="xp-error">Username already taken</div>}
                {aliasCheck.error && <div className="xp-error">{aliasCheck.error}</div>}
              </div>
            </div>
          </div>
        </div>
      )}

      {usernameModalOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.username ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("username", () => setUsernameModalOpen(false))}
        >
          <div className={`xp-modal ${closingModals.username ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Change username</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("username", () => setUsernameModalOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                New username
                <input
                  value={newUsername}
                  onChange={(e) => setNewUsername(e.target.value)}
                  minLength={USERNAME_MIN}
                  maxLength={USERNAME_MAX}
                />
              </label>
              <label>
                Current password
                <input type="password" value={usernamePassword} onChange={(e) => setUsernamePassword(e.target.value)} />
              </label>
              <div className="xp-button-row">
                <button className={`xp-button ${savingFlags.username ? "is-saving" : ""}`} onClick={saveUsername} disabled={savingFlags.username}>
                  {savingFlags.username ? "Saving..." : "Done"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("username", () => setUsernameModalOpen(false))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {passwordModalOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.password ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("password", () => setPasswordModalOpen(false))}
        >
          <div className={`xp-modal ${closingModals.password ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Change password</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("password", () => setPasswordModalOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Current password
                <input type="password" value={currentPassword} onChange={(e) => setCurrentPassword(e.target.value)} />
              </label>
              <label>
                New password
                <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} />
              </label>
              {passwordError && <div className="xp-error">{passwordError}</div>}
              <div className="xp-button-row">
                <button className={`xp-button ${savingFlags.password ? "is-saving" : ""}`} onClick={savePassword} disabled={savingFlags.password}>
                  {savingFlags.password ? "Saving..." : "Save"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("password", () => setPasswordModalOpen(false))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {primaryModal && (
        <div className="xp-modal-overlay" onClick={() => setPrimaryModal(null)}>
          <div className="xp-modal" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Set primary username</span>
              <button className="xp-modal-close" onClick={() => setPrimaryModal(null)}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-muted">Set @{primaryModal.username} as your primary username.</div>
              <label>
                Current password
                <input
                  type="password"
                  value={primaryPassword}
                  onChange={(e) => setPrimaryPassword(e.target.value)}
                />
              </label>
              {primaryError && <div className="xp-error">{primaryError}</div>}
              <div className="xp-button-row">
                <button className="xp-button" onClick={() => setPrimaryModal(null)}>Cancel</button>
                <button className="xp-button" onClick={() => setPrimaryAlias(primaryModal.username)} disabled={primarySwitching}>
                  {primarySwitching ? "Setting..." : "Set primary"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {displayEditOpen && (
        <div
          className={`xp-modal-overlay ${closingModals.display ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("display", () => setDisplayEditOpen(false))}
        >
          <div className={`xp-modal ${closingModals.display ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Change display name</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("display", () => setDisplayEditOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Display name
                <input
                  value={settings.displayName}
                  onChange={(e) => setSettings((prev) => ({ ...prev, displayName: e.target.value }))}
                />
              </label>
              <div className="xp-button-row">
                <button className={`xp-button ${savingFlags.displayName ? "is-saving" : ""}`} onClick={saveDisplayName} disabled={savingFlags.displayName}>
                  {savingFlags.displayName ? "Saving..." : "Save"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("display", () => setDisplayEditOpen(false))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {dmBgModal && (
        <div className="xp-modal-overlay" onClick={() => setDmBgModal(null)}>
            <div
              className="xp-modal xp-dm-bg-modal"
              onClick={(e) => {
                e.stopPropagation();
              }}
            >
            <div className="xp-modal-title">
              <span>Chat background</span>
              <button className="xp-modal-close" onClick={() => setDmBgModal(null)}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-dm-bg-layout">
                <div className="xp-dm-bg-controls">
                  <div className="xp-dm-bg-mode">
                    <label className={`xp-radio ${dmBgMode === "color" ? "active" : ""}`}>
                      <input type="radio" checked={dmBgMode === "color"} onChange={() => updateDmBgMode("color")} />
                      <span>Color</span>
                    </label>
                    <label className={`xp-radio ${dmBgMode === "gradient" ? "active" : ""}`}>
                      <input type="radio" checked={dmBgMode === "gradient"} onChange={() => updateDmBgMode("gradient")} />
                      <span>Gradient</span>
                    </label>
                    <label className={`xp-radio ${dmBgMode === "image" ? "active" : ""}`}>
                      <input type="radio" checked={dmBgMode === "image"} onChange={() => updateDmBgMode("image")} />
                      <span>Image</span>
                    </label>
                  </div>

                  <div className={`xp-dm-bg-section ${dmBgMode !== "color" ? "inactive" : ""}`}>
                    <div className="xp-dm-bg-label">Solid color</div>
                    <div className="xp-dm-bg-row">
                      <input
                        type="color"
                        value={dmBgColor}
                        onChange={(e) => handleDmBgColorChange(e.target.value)}
                        disabled={dmBgMode !== "color"}
                      />
                      <div className="xp-dm-bg-swatch" style={{ background: dmBgColor }} />
                    </div>
                    <div className="xp-dm-bg-swatches">
                      {dmBgRecent.map((c) => (
                        <button
                          key={c}
                          type="button"
                          className="xp-dm-bg-swatch-btn"
                          style={{ background: c }}
                          onClick={() => handleDmBgColorChange(c)}
                          disabled={dmBgMode !== "color"}
                        />
                      ))}
                    </div>
                  </div>

                  <div className={`xp-dm-bg-section ${dmBgMode !== "gradient" ? "inactive" : ""}`}>
                    <div className="xp-dm-bg-label">Gradient</div>
                    <div className="xp-dm-bg-row">
                      <div className="xp-dm-bg-select">
                        <button
                          type="button"
                          className="xp-dm-bg-select-btn"
                          disabled={dmBgMode !== "gradient"}
                          onClick={() => setDmBgPresetOpen((v) => !v)}
                        >
                          {dmBgPreset === "soft" ? "Soft" : dmBgPreset === "dusk" ? "Dusk" : "Moss"}
                        </button>
                        {dmBgPresetOpen && dmBgMode === "gradient" && (
                          <div className="xp-dm-bg-select-menu">
                            {["soft", "dusk", "moss"].map((opt) => (
                              <button
                                key={opt}
                                type="button"
                                className={`xp-dm-bg-select-item ${dmBgPreset === opt ? "active" : ""}`}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setDmBgPreset(opt);
                                  const presets = {
                                    soft: "linear-gradient(135deg,#f7f7ff,#e8f0ff)",
                                    dusk: "linear-gradient(135deg,#f5d0e8,#c9d5ff)",
                                    moss: "linear-gradient(135deg,#e8f5e4,#d3e7d1)",
                                  };
                                  const next = presets[opt] || dmBgGradient;
                                  handleDmBgGradientChange(next);
                                  setDmBgPresetOpen(false);
                                }}
                              >
                                {opt === "soft" ? "Soft" : opt === "dusk" ? "Dusk" : "Moss"}
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                      <div className="xp-dm-bg-select">
                        <button
                          type="button"
                          className="xp-dm-bg-select-btn"
                          disabled={dmBgMode !== "gradient"}
                          onClick={() => setDmBgDirectionOpen((v) => !v)}
                        >
                          {dmBgDirection === "horizontal" ? "Horizontal" : "Vertical"}
                        </button>
                        {dmBgDirectionOpen && dmBgMode === "gradient" && (
                          <div className="xp-dm-bg-select-menu">
                            {["horizontal", "vertical"].map((opt) => (
                              <button
                                key={opt}
                                type="button"
                                className={`xp-dm-bg-select-item ${dmBgDirection === opt ? "active" : ""}`}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setDmBgDirection(opt);
                                  const angle = opt === "horizontal" ? "90deg" : "180deg";
                                  const parts = String(dmBgGradient || "").split(",");
                                  if (parts.length > 1) {
                                    parts[0] = `linear-gradient(${angle}`;
                                    handleDmBgGradientChange(parts.join(","));
                                  } else {
                                    handleDmBgGradientChange(`linear-gradient(${angle}, #f7f7ff, #e8f0ff)`);
                                  }
                                  setDmBgDirectionOpen(false);
                                }}
                              >
                                {opt === "horizontal" ? "Horizontal" : "Vertical"}
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                    <input
                      className="xp-input"
                      value={dmBgGradient}
                      onChange={(e) => handleDmBgGradientChange(e.target.value)}
                      placeholder="linear-gradient(...)"
                      disabled={dmBgMode !== "gradient"}
                    />
                  </div>

                  <div className={`xp-dm-bg-section ${dmBgMode !== "image" ? "inactive" : ""}`}>
                    <div className="xp-dm-bg-label">Image</div>
                    <label className="xp-dm-bg-upload">
                      <input
                        type="file"
                        accept="image/*"
                        onChange={(e) => handleDmBgImageChange(e.target.files?.[0])}
                        disabled={dmBgMode !== "image"}
                      />
                      <span>Choose image</span>
                    </label>
                    <div className="xp-dm-bg-row">
                      <div className="xp-dm-bg-select">
                        <button
                          type="button"
                          className="xp-dm-bg-select-btn"
                          disabled={dmBgMode !== "image"}
                          onClick={() => setDmBgFitOpen((v) => !v)}
                        >
                          {dmBgFit === "fill" ? "Fill" : dmBgFit === "fit" ? "Fit" : "Tile"}
                        </button>
                        {dmBgFitOpen && dmBgMode === "image" && (
                          <div className="xp-dm-bg-select-menu">
                            {["fill", "fit", "tile"].map((opt) => (
                              <button
                                key={opt}
                                type="button"
                                className={`xp-dm-bg-select-item ${dmBgFit === opt ? "active" : ""}`}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setDmBgFit(opt);
                                  const current = dmBackgrounds[dmBgModal];
                                  if (current?.type === "image") {
                                    applyDmBackground({ ...current, fit: opt });
                                  }
                                  setDmBgFitOpen(false);
                                }}
                              >
                                {opt === "fill" ? "Fill" : opt === "fit" ? "Fit" : "Tile"}
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                      <label className="xp-dm-bg-opacity">
                        Opacity
                        <input
                          type="range"
                          min="0.35"
                          max="0.85"
                          step="0.05"
                          value={dmBgOpacity}
                          onChange={(e) => {
                            const v = Number(e.target.value);
                            setDmBgOpacity(v);
                            const current = dmBackgrounds[dmBgModal];
                            if (current?.type === "image" || current?.type === "gradient") {
                              const overlay = current.overlay
                                ? current.overlay.replace(/rgba\\(([^)]+)\\)/, (m) => {
                                    const parts = m.replace("rgba(", "").replace(")", "").split(",");
                                    return `rgba(${parts[0]}, ${parts[1]}, ${parts[2]}, ${v})`;
                                  })
                                : `rgba(255, 255, 255, ${v})`;
                              applyDmBackground({ ...current, overlay, fit: dmBgFit });
                            }
                          }}
                          disabled={dmBgMode !== "image" && dmBgMode !== "gradient"}
                        />
                      </label>
                    </div>
                  </div>
                </div>

              </div>
            </div>
          </div>
        </div>
      )}

      {forwardModal && (
        <div className="xp-modal-overlay" onClick={() => setForwardModal(null)}>
          <div className="xp-modal xp-modal-wide" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Forward to</span>
              <button className="xp-modal-close" onClick={() => setForwardModal(null)}>x</button>
            </div>
            <div className="xp-modal-body">
              <label className="xp-forward-note">
                Add a message (optional)
                <input
                  className="xp-input"
                  value={forwardNote}
                  maxLength={300}
                  onChange={(e) => setForwardNote(e.target.value)}
                  placeholder="Write a short note..."
                />
              </label>
              <div className="xp-forward-list">
                <div className="xp-forward-section">Direct messages</div>
                <div className="xp-forward-grid">
                  {friends.map((f) => (
                    <button
                      key={`dm-${f.id}`}
                      className={`xp-forward-item ${forwardTarget?.type === "dm" && forwardTarget.id === f.id ? "active" : ""}`}
                      onClick={() => setForwardTarget({ type: "dm", id: f.id })}
                    >
                      <img
                        src={avatarUrl(f.avatar, f.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, f.username)}
                      />
                      <div>
                        <div className="xp-forward-name">@{f.username}</div>
                        <div className="xp-forward-sub">{f.display_name || f.username}</div>
                      </div>
                    </button>
                  ))}
                </div>
                <div className="xp-forward-section">Groups</div>
                <div className="xp-forward-grid">
                  {groups.map((g) => (
                    <button
                      key={`group-${g.id}`}
                      className={`xp-forward-item ${forwardTarget?.type === "group" && forwardTarget.id === g.id ? "active" : ""}`}
                      onClick={() => setForwardTarget({ type: "group", id: g.id })}
                    >
                      <img
                        src={avatarUrl(g.avatar, g.name)}
                        alt=""
                        onError={(e) => onAvatarError(e, g.name)}
                      />
                      <div>
                        <div className="xp-forward-name">{g.name}</div>
                        <div className="xp-forward-sub">{(g.members || []).length} members</div>
                      </div>
                    </button>
                  ))}
                </div>
              </div>
              <div className="xp-button-row">
                <button className="xp-button" onClick={() => setForwardModal(null)}>Cancel</button>
                <button className="xp-button" onClick={sendForward} disabled={!forwardTarget}>
                  Forward
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {avatarEditOpen && (
        <div className={`xp-modal-overlay ${closingModals.avatar ? "closing" : ""}`} onClick={() => closeModalWithAnim("avatar", () => setAvatarEditOpen(false))}>
          <div className={`xp-modal xp-avatar-modal ${closingModals.avatar ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Edit Image</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("avatar", () => setAvatarEditOpen(false))}>x</button>
            </div>
            <div className="xp-modal-body">
              <div
                className="xp-avatar-crop"
                onPointerDown={onAvatarPointerDown}
                onPointerMove={onAvatarPointerMove}
                onPointerUp={onAvatarPointerUp}
                onPointerCancel={onAvatarPointerUp}
                onWheel={onAvatarWheel}
              >
                {avatarEditSrc && (
                  <img
                    src={avatarEditSrc}
                    alt=""
                    style={{
                      width: `${avatarEditSize.w * avatarEditSize.base}px`,
                      height: `${avatarEditSize.h * avatarEditSize.base}px`,
                      transform: `translate(${avatarOffset.x}px, ${avatarOffset.y}px) scale(${avatarZoom}) rotate(${avatarRotate}deg)`,
                    }}
                    draggable={false}
                  />
                )}
              </div>
              <div className="xp-avatar-controls">
                <label>
                  Zoom
                  <input type="range" min="1" max="3" step="0.01" value={avatarZoom} onChange={(e) => setAvatarZoom(Number(e.target.value))} />
                </label>
                <label>
                  Rotate
                  <input type="range" min="0" max="360" step="1" value={avatarRotate} onChange={(e) => setAvatarRotate(Number(e.target.value))} />
                </label>
              </div>
              <div className="xp-button-row">
                <button className="xp-button" onClick={() => closeModalWithAnim("avatar", () => setAvatarEditOpen(false))}>Cancel</button>
                <button className={`xp-button ${avatarApplying ? "is-saving" : ""}`} onClick={applyAvatarCrop} disabled={avatarApplying}>
                  {avatarApplying ? "Saving..." : "Apply"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {transferModal && (
        <div
          className={`xp-modal-overlay ${closingModals.transfer ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("transfer", () => setTransferModal(null))}
        >
          <div className={`xp-modal ${closingModals.transfer ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Transfer @{transferModal.username}</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("transfer", () => setTransferModal(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Recipient username
                <input
                  value={transferModal.to || ""}
                  onChange={(e) => setTransferModal((prev) => ({ ...prev, to: e.target.value }))}
                  minLength={USERNAME_MIN}
                  maxLength={USERNAME_MAX}
                />
              </label>
              <label>
                Your password
                <input
                  type="password"
                  value={transferModal.password || ""}
                  onChange={(e) => setTransferModal((prev) => ({ ...prev, password: e.target.value }))}
                />
              </label>
              {transferError && <div className="xp-error">{transferError}</div>}
              <div className="xp-button-row">
                <button
                  className="xp-button"
                  onClick={() => {
                    transferAlias(transferModal.username, transferModal.to || "", transferModal.password || "");
                  }}
                  disabled={aliasTransfering}
                >
                  {aliasTransfering ? "Transferring..." : "Transfer"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("transfer", () => setTransferModal(null))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {removeAliasModal && (
        <div
          className={`xp-modal-overlay ${closingModals.removeAlias ? "closing" : ""}`}
          onClick={() => closeModalWithAnim("removeAlias", () => setRemoveAliasModal(null))}
        >
          <div className={`xp-modal ${closingModals.removeAlias ? "closing" : ""}`} onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Remove @{removeAliasModal.username}</span>
              <button className="xp-modal-close" onClick={() => closeModalWithAnim("removeAlias", () => setRemoveAliasModal(null))}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Your password
                <input
                  type="password"
                  value={removeAliasModal.password || ""}
                  onChange={(e) => setRemoveAliasModal((prev) => ({ ...prev, password: e.target.value }))}
                />
              </label>
              <div className="xp-button-row">
                <button
                  className="xp-button"
                  onClick={() => {
                    removeAlias(removeAliasModal.username, removeAliasModal.password || "");
                    setTimeout(() => setRemoveAliasModal(null), 280);
                  }}
                  disabled={aliasRemoving}
                >
                  {aliasRemoving ? "Removing..." : "Remove"}
                </button>
                <button className="xp-button" onClick={() => closeModalWithAnim("removeAlias", () => setRemoveAliasModal(null))}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {storyViewer.open && (
        <div
          className={`xp-modal-overlay story-overlay ${storyClosing ? "story-closing" : ""}`}
          onClick={closeStory}
        >
          <div
            className={`xp-story-modal ${storyPaused ? "paused" : ""} ${storyClosing ? "story-closing" : ""}`}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="xp-story-title">
              <span>@{stories[storyViewer.userIndex]?.user?.username}'s story</span>
              <div className="xp-story-meta">
                {timeAgo(stories[storyViewer.userIndex]?.stories?.[storyViewer.storyIndex]?.created_at)}
              </div>
              <div className="xp-story-actions">
                {stories[storyViewer.userIndex]?.user?.id === user.id && (
                  <button
                    className="xp-button xp-story-viewers"
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleStoryViewers();
                    }}
                    title="Viewers"
                  >
                    <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                      <path d="M12 5c5 0 9 4 10 7-1 3-5 7-10 7S3 15 2 12c1-3 5-7 10-7Zm0 3a4 4 0 1 0 0 8 4 4 0 0 0 0-8Z" fill="currentColor" />
                    </svg>
                  </button>
                )}
                <button
                  className="xp-button xp-story-toggle"
                  onClick={(e) => {
                    e.stopPropagation();
                    setStoryPaused((prev) => !prev);
                  }}
                  title={storyPaused ? "Play" : "Pause"}
                >
                  {storyPaused ? (
                    <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                      <polygon points="8,5 19,12 8,19" fill="currentColor" />
                    </svg>
                  ) : (
                    <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                      <rect x="6" y="5" width="4" height="14" fill="currentColor" />
                      <rect x="14" y="5" width="4" height="14" fill="currentColor" />
                    </svg>
                  )}
                </button>
                {stories[storyViewer.userIndex]?.user?.id === user.id && (
                  <button
                    className="xp-button xp-story-trash"
                    onClick={(e) => {
                      e.stopPropagation();
                      setStoryDeleteConfirm(true);
                    }}
                    title="Delete story"
                  >
                    <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                      <path
                        d="M9 3h6l1 2h4v2H4V5h4l1-2Zm1 7h2v8h-2v-8Zm4 0h2v8h-2v-8ZM7 7h10l-1 12H8L7 7Z"
                        fill="currentColor"
                      />
                    </svg>
                  </button>
                )}
                <button className="xp-button" onClick={(e) => { e.stopPropagation(); closeStory(); }}>x</button>
              </div>
            </div>
            <div className="xp-story-progress">
              {(stories[storyViewer.userIndex]?.stories || []).map((story, idx) => {
                const duration = story.type === "video" ? STORY_VIDEO_MS : STORY_IMAGE_MS;
                if (idx < storyViewer.storyIndex) {
                  return (
                    <div key={story.id} className="xp-story-bar">
                      <span style={{ transform: "scaleX(1)" }} />
                    </div>
                  );
                }
                if (idx > storyViewer.storyIndex) {
                  return (
                    <div key={story.id} className="xp-story-bar">
                      <span style={{ transform: "scaleX(0)" }} />
                    </div>
                  );
                }
                return (
                  <div key={story.id} className="xp-story-bar">
                    <span
                      key={`${story.id}-${storyViewer.storyIndex}`}
                      style={{
                        transform: "scaleX(0)",
                        animation: `xp-story-fill ${duration}ms linear forwards`,
                        animationPlayState: storyPaused ? "paused" : "running",
                      }}
                      onAnimationEnd={() => {
                        if (!storyPaused && idx === storyViewer.storyIndex) {
                          advanceStory();
                        }
                      }}
                    />
                  </div>
                );
              })}
            </div>
            <div className="xp-story-body">
              <div className="xp-story-media-wrap">
                {(() => {
                  const current = stories[storyViewer.userIndex]?.stories?.[storyViewer.storyIndex];
                  if (!current) return null;
                  const src = resolveMediaUrl(current.url || current.media_url || current.mediaUrl || "");
                  if (current.type === "video") {
                    return (
                      <video
                        ref={storyVideoRef}
                        className="xp-story-media"
                        src={src}
                        autoPlay
                        loop
                        muted
                        playsInline
                      />
                    );
                  }
                  return <img className="xp-story-media" src={src} alt="" />;
                })()}
                <div className="xp-story-tapzones">
                  <button className="xp-story-tap left" onClick={() => {
                    setStoryPaused(false);
                    setStoryViewer((prev) => {
                      if (prev.storyIndex > 0) return { ...prev, storyIndex: prev.storyIndex - 1 };
                      if (prev.userIndex > 0) {
                        const prevList = stories[prev.userIndex - 1]?.stories || [];
                        return { ...prev, userIndex: prev.userIndex - 1, storyIndex: Math.max(0, prevList.length - 1) };
                      }
                      return prev;
                    });
                  }} />
                  <button className="xp-story-tap right" onClick={() => {
                    setStoryPaused(false);
                    advanceStory();
                  }} />
                </div>
              </div>
              {(stories[storyViewer.userIndex]?.stories || []).length > 1 && (
                <div className="xp-story-desktop-nav">
                  <button
                    className="xp-button"
                    onClick={() => {
                      setStoryPaused(false);
                      setStoryViewer((prev) => {
                        if (prev.storyIndex > 0) return { ...prev, storyIndex: prev.storyIndex - 1 };
                        if (prev.userIndex > 0) {
                          const prevList = stories[prev.userIndex - 1]?.stories || [];
                          return { ...prev, userIndex: prev.userIndex - 1, storyIndex: Math.max(0, prevList.length - 1) };
                        }
                        return prev;
                      });
                    }}
                  >
                    Prev
                  </button>
                  <button className="xp-button" onClick={() => { setStoryPaused(false); advanceStory(); }}>
                    Next
                  </button>
                </div>
              )}
            </div>
            {storyViewersOpen && (
              <div
                className="xp-modal-overlay story-viewers-overlay"
                onClick={() => setStoryViewersOpen(false)}
              >
                <div className="xp-modal xp-story-viewers-modal" onClick={(e) => e.stopPropagation()}>
                  <div className="xp-modal-title">
                    <span>Viewed by</span>
                    <button className="xp-modal-close" onClick={() => setStoryViewersOpen(false)}>
                      x
                    </button>
                  </div>
                  <div className="xp-modal-body xp-story-viewers-list">
                    {storyViewers.length === 0 && <div className="xp-muted">No views yet</div>}
                    {storyViewers.map((v) => (
                      <div key={v.id} className="xp-story-viewer">
                        <img
                          src={avatarUrl(v.avatar, v.username)}
                          alt=""
                          onError={(e) => onAvatarError(e, v.username)}
                        />
                        <div>
                          <div className="xp-user-name">@{v.username}</div>
                          <div className="xp-user-display">{v.display_name || v.username}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
            {storyDeleteConfirm && (
              <div className="xp-story-confirm-overlay" onClick={() => setStoryDeleteConfirm(false)}>
                <div className="xp-story-confirm" onClick={(e) => e.stopPropagation()}>
                  <div className="xp-story-confirm-title">Delete this story?</div>
                  <div className="xp-story-confirm-actions">
                    <button
                      className="xp-button danger"
                      onClick={() => {
                        const current = stories[storyViewer.userIndex]?.stories?.[storyViewer.storyIndex];
                        if (current) deleteStory(current.id);
                        setStoryDeleteConfirm(false);
                      }}
                    >
                      Delete
                    </button>
                    <button className="xp-button" onClick={() => setStoryDeleteConfirm(false)}>
                      Cancel
                    </button>
                  </div>
                </div>
              </div>
            )}
            {storyMenuOpen && (
              <div className="xp-story-menu">
                <button className="xp-button" onClick={() => setStoryMenuOpen(false)}>
                  Close
                </button>
              </div>
            )}
          </div>
        </div>
      )}

      {incomingCall && (
        <div className="xp-modal-overlay xp-call-overlay">
          <div className="xp-modal xp-call-modal" style={{ transform: `translate(${callWindowPos.x}px, ${callWindowPos.y}px)` }}>
            <div className="xp-modal-title xp-call-drag" onMouseDown={startCallDrag}>
              <span>Incoming call</span>
              <button className="xp-modal-close" onClick={declineCall}>x</button>
            </div>
            <div className="xp-modal-body">
              {(() => {
                const other =
                  friendsAll.find((f) => f.id === incomingCall.fromId) ||
                  friends.find((f) => f.id === incomingCall.fromId) ||
                  manualDmUsers.find((f) => f.id === incomingCall.fromId) ||
                  null;
                return (
                  <div className="xp-call-user">
                    <div className="xp-call-avatar ringing">
                      <img
                        src={avatarUrl(other?.avatar, other?.username || "U")}
                        alt=""
                        onError={(e) => onAvatarError(e, other?.username || "U")}
                      />
                    </div>
                    <div>
                      <div className="xp-call-name">@{other?.username || "user"}</div>
                      <div className="xp-call-sub">{other?.display_name || other?.username || ""}</div>
                    </div>
                  </div>
                );
              })()}
              <div className="xp-call-duration">Ringing...</div>
              <div className="xp-call-actions">
                <button className="xp-button" onClick={acceptCall}>Accept</button>
                <button className="xp-button" onClick={declineCall}>Decline</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {callState.status === "calling" && (
        <div className="xp-modal-overlay xp-call-overlay">
          <div className="xp-modal xp-call-modal" onClick={(e) => e.stopPropagation()} style={{ transform: `translate(${callWindowPos.x}px, ${callWindowPos.y}px)` }}>
            <div className="xp-modal-title xp-call-drag" onMouseDown={startCallDrag}>
              <span>Calling...</span>
              <button className="xp-modal-close" onClick={cancelOutgoingCall}>x</button>
            </div>
            <div className="xp-modal-body">
              {(() => {
                const other =
                  friendsAll.find((f) => f.id === callState.withUserId) ||
                  friends.find((f) => f.id === callState.withUserId) ||
                  manualDmUsers.find((f) => f.id === callState.withUserId) ||
                  null;
                return (
                  <div className="xp-call-user">
                    <div className="xp-call-avatar ringing">
                      <img
                        src={avatarUrl(other?.avatar, other?.username || "U")}
                        alt=""
                        onError={(e) => onAvatarError(e, other?.username || "U")}
                      />
                    </div>
                    <div>
                      <div className="xp-call-name">@{other?.username || "user"}</div>
                      <div className="xp-call-sub">{other?.display_name || other?.username || ""}</div>
                    </div>
                  </div>
                );
              })()}
              <div className="xp-call-duration">Ringing...</div>
              <div className="xp-call-actions">
                <button className="xp-button" onClick={cancelOutgoingCall}>Cancel</button>
              </div>
            </div>
          </div>
        </div>
      )}

            {(callState.status === "in-call" || callState.status === "reconnecting") && (
        <div className="xp-modal-overlay xp-call-overlay">
          <div className="xp-modal xp-call-modal" onClick={(e) => e.stopPropagation()} style={{ transform: `translate(${callWindowPos.x}px, ${callWindowPos.y}px)` }}>
          <div className="xp-modal-title xp-call-drag" onMouseDown={startCallDrag}>
            <span>{callState.status === "reconnecting" ? "Reconnecting..." : "In call"}</span>
          </div>
          <div className="xp-modal-body">
            {(() => {
              const other =
                friendsAll.find((f) => f.id === callState.withUserId) ||
                friends.find((f) => f.id === callState.withUserId) ||
                manualDmUsers.find((f) => f.id === callState.withUserId) ||
                null;
              const otherAudioStatus = callAudioStates[callState.withUserId] || null;
              const otherStatusType = otherAudioStatus?.deafened ? "deafened" : otherAudioStatus?.muted ? "muted" : null;
              const solo = callState.status === "active" && !remotePresent;
              return (
                <div className="xp-call-user">
                  <div className={`xp-call-avatar ${speakingRemote ? "speaking" : ""} ${solo ? "solo" : ""}`}>
                    <img
                      src={avatarUrl(other?.avatar, other?.username || "U")}
                      alt=""
                      onError={(e) => onAvatarError(e, other?.username || "U")}
                    />
                    {otherStatusType && (
                      <div className={`xp-call-avatar-status ${otherStatusType}`} title={otherStatusType === "deafened" ? "Deafened" : "Muted"}>
                        {getAudioStatusIcon(otherStatusType)}
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="xp-call-name">@{other?.username || "user"}</div>
                    <div className="xp-call-sub">
                      {solo ? "Waiting to rejoin..." : other?.display_name || other?.username || ""}
                    </div>
                  </div>
                </div>
              );
            })()}
            <div className="xp-call-duration-row">
              <div className="xp-call-duration">
                {callState.status === "reconnecting" ? "Reconnecting..." : formatDuration(callDuration)}
              </div>
              <div className={`xp-call-quality ${callQuality.level}`}>
                <span className="xp-call-quality-dot" />
                {callQuality.label}
              </div>
              {voiceEffect !== "none" && (
                <div className="xp-call-effect">Effects: {voiceEffect}</div>
              )}
            </div>
            {screenShareError && <div className="xp-error xp-call-error">{screenShareError}</div>}
            <div className="xp-call-actions">
              <div className="xp-call-mic">
                <button className={`xp-button xp-icon-btn ${callState.muted ? "muted" : ""}`} onClick={toggleMute} title={callState.muted ? "Unmute" : "Mute"}>
                  <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                    <path d="M12 3a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V6a3 3 0 0 0-3-3z" fill="none" stroke="currentColor" strokeWidth="2" />
                    <path d="M5 11a7 7 0 0 0 14 0" fill="none" stroke="currentColor" strokeWidth="2" />
                    <path d="M12 18v3" fill="none" stroke="currentColor" strokeWidth="2" />
                  </svg>
                </button>
                <button
                  className={`xp-button xp-icon-btn ${callState.deafened ? "muted" : ""}`}
                  onClick={toggleDeafen}
                  title={callState.deafened ? "Undeafen" : "Deafen"}
                >
                  {getAudioStatusIcon("deafened")}
                </button>
                <button
                  className="xp-button xp-icon-btn"
                  type="button"
                  title="Call settings"
                  onClick={() => setCallSettingsOpen((p) => !p)}
                >
                  <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                    <path d="M6 9l6 6 6-6" fill="none" stroke="currentColor" strokeWidth="2" />
                  </svg>
                </button>
              </div>
              <button className="xp-button" onClick={dmShareActive ? stopDmScreenShare : startDmScreenShare}>
                {dmShareActive ? "Stop Share" : "Share Screen"}
              </button>
              <button className="xp-button" onClick={() => setBlackjackPanelOpen(true)}>
                Activities
              </button>
              <button className="xp-button" onClick={endCall}>End</button>
            </div>
            {callSettingsOpen && (
              <div className="xp-call-settings">
                <div className="xp-call-select">
                  <div className="xp-call-select-label">Input device</div>
                  <button
                    type="button"
                    className="xp-call-select-btn"
                    onClick={() => {
                      setCallInputMenuOpen((p) => !p);
                      setCallOutputMenuOpen(false);
                    }}
                  >
                    {callDevices.inputs.find((d) => d.deviceId === callSettings.inputId)?.label || "Default"}
                    <span className="xp-caret">▾</span>
                  </button>
                  {callInputMenuOpen && (
                    <div className="xp-call-select-menu">
                      <button
                        className={`xp-call-select-item ${callSettings.inputId === "" ? "active" : ""}`}
                        onClick={() => {
                          setCallSettings((s) => ({ ...s, inputId: "" }));
                          applyInputDevice("");
                          setCallInputMenuOpen(false);
                        }}
                      >
                        Default
                      </button>
                      {callDevices.inputs.map((d) => (
                        <button
                          key={d.deviceId}
                          className={`xp-call-select-item ${callSettings.inputId === d.deviceId ? "active" : ""}`}
                          onClick={() => {
                            setCallSettings((s) => ({ ...s, inputId: d.deviceId }));
                            applyInputDevice(d.deviceId);
                            setCallInputMenuOpen(false);
                          }}
                        >
                          {d.label || "Microphone"}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <div className="xp-call-select">
                  <div className="xp-call-select-label">Output device</div>
                  <button
                    type="button"
                    className="xp-call-select-btn"
                    onClick={() => {
                      setCallOutputMenuOpen((p) => !p);
                      setCallInputMenuOpen(false);
                    }}
                  >
                    {callDevices.outputs.find((d) => d.deviceId === callSettings.outputId)?.label || "Default"}
                    <span className="xp-caret">▾</span>
                  </button>
                  {callOutputMenuOpen && (
                    <div className="xp-call-select-menu">
                      <button
                        className={`xp-call-select-item ${callSettings.outputId === "" ? "active" : ""}`}
                        onClick={() => {
                          setCallSettings((s) => ({ ...s, outputId: "" }));
                          applyOutputDevice("");
                          setCallOutputMenuOpen(false);
                        }}
                      >
                        Default
                      </button>
                      {callDevices.outputs.map((d) => (
                        <button
                          key={d.deviceId}
                          className={`xp-call-select-item ${callSettings.outputId === d.deviceId ? "active" : ""}`}
                          onClick={() => {
                            setCallSettings((s) => ({ ...s, outputId: d.deviceId }));
                            applyOutputDevice(d.deviceId);
                            setCallOutputMenuOpen(false);
                          }}
                        >
                          {d.label || "Speaker"}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <button
                  className="xp-button"
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setCallSettingsOpen(false);
                    setVoicePanelOpen(true);
                  }}
                >
                  Voice Settings
                </button>
              </div>
            )}
          </div>
          </div>
        </div>
      )}
      {groupCallVisible && (groupCall.status === "ringing" || groupCall.status === "in-call") && (
        <div className="xp-modal-overlay xp-call-overlay">
          <div className="xp-modal xp-call-modal" onClick={(e) => e.stopPropagation()}>
          <div className="xp-modal-title">
            <span>
              {groups.find((g) => g.id === groupCall.groupId)?.name || "Group Call"}
            </span>
          </div>
          <div className="xp-modal-body">
            {groupCall.status === "ringing" && (
              <div className="xp-call-ringing">Ringing…</div>
            )}
            <div className="xp-call-avatar-grid">
              {(((selectedGroup && selectedGroup.id === groupCall.groupId)
                ? selectedGroup.members
                : groups.find((g) => g.id === groupCall.groupId)?.members) || [])
                .slice(0, 10)
                .map((m) => {
                  const memberAudioStatus = groupCallAudioStates[m.id] || null;
                  const memberStatusType = memberAudioStatus?.deafened ? "deafened" : memberAudioStatus?.muted ? "muted" : null;
                  return (
                    <div
                      key={m.id}
                      className={`xp-call-avatar ${groupCall.status === "ringing" ? "ringing" : ""} ${
                        groupSpeakingMap[m.id] ? "speaking" : ""
                      }`}
                    >
                      <img
                        src={avatarUrl(m.avatar, m.username)}
                        alt=""
                        onError={(e) => onAvatarError(e, m.username)}
                      />
                      {memberStatusType && (
                        <div className={`xp-call-avatar-status ${memberStatusType}`} title={memberStatusType === "deafened" ? "Deafened" : "Muted"}>
                          {getAudioStatusIcon(memberStatusType)}
                        </div>
                      )}
                      <div className="xp-call-avatar-name">@{m.username}</div>
                    </div>
                  );
                })}
            </div>
            <div className="xp-call-duration">{formatDuration(groupCallDuration)}</div>
            {screenShareError && <div className="xp-error xp-call-error">{screenShareError}</div>}
            {groupCall.status === "ringing" ? (
              <div className="xp-call-actions">
                <button className="xp-button" onClick={() => joinGroupCall(groupCall.groupId)}>Join</button>
                <button className="xp-button" onClick={() => setGroupCallVisible(false)}>Ignore</button>
              </div>
            ) : (
              <div className="xp-call-actions">
                <div className="xp-call-mic">
                  <button
                    className={`xp-button xp-icon-btn ${callState.muted ? "muted" : ""}`}
                    onClick={toggleMute}
                    title={callState.muted ? "Unmute" : "Mute"}
                  >
                    <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                      <path d="M12 3a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V6a3 3 0 0 0-3-3z" fill="none" stroke="currentColor" strokeWidth="2" />
                      <path d="M5 11a7 7 0 0 0 14 0" fill="none" stroke="currentColor" strokeWidth="2" />
                      <path d="M12 18v3" fill="none" stroke="currentColor" strokeWidth="2" />
                    </svg>
                  </button>
                  <button
                    className={`xp-button xp-icon-btn ${callState.deafened ? "muted" : ""}`}
                    onClick={toggleDeafen}
                    title={callState.deafened ? "Undeafen" : "Deafen"}
                  >
                    {getAudioStatusIcon("deafened")}
                  </button>
                </div>
                <button className="xp-button" onClick={startGroupScreenShare}>Share Screen</button>
                <button className="xp-button xp-icon-btn" type="button" onClick={() => setCallSettingsOpen((p) => !p)}>
                  <svg viewBox="0 0 24 24" className="xp-icon" aria-hidden="true">
                    <path d="M6 9l6 6 6-6" fill="none" stroke="currentColor" strokeWidth="2" />
                  </svg>
                </button>
                <button className="xp-button" onClick={leaveGroupCall}>Leave</button>
              </div>
            )}
            {callSettingsOpen && (
              <div className="xp-call-settings">
                <div className="xp-call-select">
                  <div className="xp-call-select-label">Input device</div>
                  <button
                    type="button"
                    className="xp-call-select-btn"
                    onClick={() => {
                      setCallInputMenuOpen((p) => !p);
                      setCallOutputMenuOpen(false);
                    }}
                  >
                    {callDevices.inputs.find((d) => d.deviceId === callSettings.inputId)?.label || "Default"}
                    <span className="xp-caret">▾</span>
                  </button>
                  {callInputMenuOpen && (
                    <div className="xp-call-select-menu">
                      <button
                        className={`xp-call-select-item ${callSettings.inputId === "" ? "active" : ""}`}
                        onClick={() => {
                          setCallSettings((s) => ({ ...s, inputId: "" }));
                          applyInputDevice("");
                          setCallInputMenuOpen(false);
                        }}
                      >
                        Default
                      </button>
                      {callDevices.inputs.map((d) => (
                        <button
                          key={d.deviceId}
                          className={`xp-call-select-item ${callSettings.inputId === d.deviceId ? "active" : ""}`}
                          onClick={() => {
                            setCallSettings((s) => ({ ...s, inputId: d.deviceId }));
                            applyInputDevice(d.deviceId);
                            setCallInputMenuOpen(false);
                          }}
                        >
                          {d.label || "Microphone"}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <div className="xp-call-select">
                  <div className="xp-call-select-label">Output device</div>
                  <button
                    type="button"
                    className="xp-call-select-btn"
                    onClick={() => {
                      setCallOutputMenuOpen((p) => !p);
                      setCallInputMenuOpen(false);
                    }}
                  >
                    {callDevices.outputs.find((d) => d.deviceId === callSettings.outputId)?.label || "Default"}
                    <span className="xp-caret">▾</span>
                  </button>
                  {callOutputMenuOpen && (
                    <div className="xp-call-select-menu">
                      <button
                        className={`xp-call-select-item ${callSettings.outputId === "" ? "active" : ""}`}
                        onClick={() => {
                          setCallSettings((s) => ({ ...s, outputId: "" }));
                          applyOutputDevice("");
                          setCallOutputMenuOpen(false);
                        }}
                      >
                        Default
                      </button>
                      {callDevices.outputs.map((d) => (
                        <button
                          key={d.deviceId}
                          className={`xp-call-select-item ${callSettings.outputId === d.deviceId ? "active" : ""}`}
                          onClick={() => {
                            setCallSettings((s) => ({ ...s, outputId: d.deviceId }));
                            applyOutputDevice(d.deviceId);
                            setCallOutputMenuOpen(false);
                          }}
                        >
                          {d.label || "Speaker"}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <label>
                  Mic volume
                  <input
                    type="range"
                    min="0"
                    max="1.5"
                    step="0.01"
                    value={callSettings.micVolume}
                    onChange={(e) => {
                      const v = Number(e.target.value);
                      setCallSettings((s) => ({ ...s, micVolume: v }));
                      if (micGainNodeRef.current) micGainNodeRef.current.gain.value = v;
                    }}
                  />
                </label>
                <label>
                  Speaker volume
                  <input
                    type="range"
                    min="0"
                    max="1"
                    step="0.01"
                    value={callSettings.speakerVolume}
                    onChange={(e) => setCallSettings((s) => ({ ...s, speakerVolume: Number(e.target.value) }))}
                  />
                </label>
                <button
                  className="xp-button"
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setCallSettingsOpen(false);
                    setVoicePanelOpen(true);
                  }}
                >
                  Voice Settings
                </button>
              </div>
            )}
          </div>
          </div>
        </div>
      )}

      {groupMembersOpen && selectedGroup && (
        <div className="xp-modal-overlay xp-group-members-overlay" onClick={() => setGroupMembersOpen(false)}>
          <div
            className="xp-group-members-sheet"
            onClick={(e) => e.stopPropagation()}
            onTouchStart={(e) => {
              const touch = e.touches?.[0];
              if (!touch) return;
              mobileTouchRef.current = { x: touch.clientX, y: touch.clientY, active: true };
            }}
            onTouchMove={(e) => {
              if (!mobileTouchRef.current.active) return;
              const touch = e.touches?.[0];
              if (!touch) return;
              const dy = touch.clientY - mobileTouchRef.current.y;
              if (dy > 80) {
                mobileTouchRef.current.active = false;
                setGroupMembersOpen(false);
              }
            }}
            onTouchEnd={() => {
              mobileTouchRef.current.active = false;
            }}
          >
            <div className="xp-group-members-sheet-handle" />
            <div className="xp-group-members-header">
              Members {selectedGroup.members?.length || 0}/10
            </div>
            <div className="xp-group-members">
              {(selectedGroup.members || []).map((m) => (
                <button
                  key={m.id}
                  className="xp-group-member"
                  onClick={() => loadProfile(m.id)}
                >
                  <img
                    src={avatarUrl(m.avatar, m.username)}
                    alt=""
                    onError={(e) => onAvatarError(e, m.username)}
                  />
                  <div>
                    <div className="xp-user-name">
                      {m.display_name || m.username}
                      {selectedGroup.owner_id === m.id && (
                        <span className="xp-owner-crown" aria-label="Group owner">
                          <svg viewBox="0 0 24 24" aria-hidden="true">
                            <path d="M3 8l4 3 5-6 5 6 4-3v9H3V8z" fill="currentColor" />
                            <rect x="4" y="17" width="16" height="3" rx="1" fill="currentColor" />
                          </svg>
                        </span>
                      )}
                    </div>
                    {(m.custom_status || customStatusById[m.id]) && (
                      <div className="xp-custom-status">{m.custom_status || customStatusById[m.id]}</div>
                    )}
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>
      )}

      {screenShareWindow.open && (
        <div className="xp-call-window">
          <div className="xp-call-window-title">
            {screenShareWindow.label || "Screen Share"}
            <div className="xp-call-window-actions">
              <button className="xp-button" onClick={() => setScreenShareWindow((prev) => ({ ...prev, open: false }))}>x</button>
            </div>
          </div>
          <div className="xp-call-window-body">
            {groupLocalScreenRef.current && (
              <StreamVideo stream={groupLocalScreenRef.current} muted />
            )}
            {dmLocalScreenRef.current && (
              <StreamVideo stream={dmLocalScreenRef.current} muted />
            )}
            {screenShareEntries.map((entry) => (
              <div key={entry.id} className="xp-share-tile">
                <div className="xp-share-label">{entry.name}</div>
                <StreamVideo stream={entry.stream} muted />
              </div>
            ))}
          </div>
        </div>
      )}

      {voicePanelOpen && (
        <div className="xp-modal-overlay xp-call-overlay" onClick={() => setVoicePanelOpen(false)}>
          <div className="xp-modal xp-modal-wide" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Voice Settings</span>
              <button className="xp-modal-close" onClick={() => setVoicePanelOpen(false)}>x</button>
            </div>
            <div className="xp-modal-body">
              <label>
                Input volume
                <input
                  type="range"
                  min="0"
                  max="1.5"
                  step="0.01"
                  value={callSettings.micVolume}
                  onChange={(e) => {
                    const v = Number(e.target.value);
                    setCallSettings((s) => ({ ...s, micVolume: v }));
                    if (micGainNodeRef.current) micGainNodeRef.current.gain.value = v;
                  }}
                />
              </label>
              <label>
                Output volume
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.01"
                  value={callSettings.speakerVolume}
                  onChange={(e) => setCallSettings((s) => ({ ...s, speakerVolume: Number(e.target.value) }))}
                />
              </label>
              <div className="xp-call-mic-test">
                <button className="xp-button" onClick={toggleMicTest}>
                  {micTestOn ? "Stop Test" : "Test Mic"}
                </button>
                <div className="xp-meter">
                  <div className="xp-meter-fill" style={{ width: `${Math.round(micTestLevel * 100)}%` }} />
                </div>
              </div>
              <div className="xp-call-sensitivity">
                <div className="xp-call-sens-label">
                  Input sensitivity
                  <span className="xp-muted">
                    {inputSensitivity < 0.34 ? " Low" : inputSensitivity < 0.67 ? " Medium" : " High"}
                  </span>
                </div>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.01"
                  value={inputSensitivity}
                  onChange={(e) => setInputSensitivity(Number(e.target.value))}
                />
              </div>
              <div className="xp-call-ptt-block">
                <div className="xp-call-ptt-row">
                  <div className="xp-call-ptt-label">Echo cancellation</div>
                  <label className={`xp-toggle ${echoCancel ? "on" : ""}`}>
                    <input type="checkbox" checked={echoCancel} onChange={(e) => setEchoCancel(e.target.checked)} />
                    <span />
                  </label>
                </div>
                <div className="xp-call-ptt-row">
                  <div className="xp-call-ptt-label">Push to Talk</div>
                  <label className={`xp-toggle ${pushToTalk ? "on" : ""}`}>
                    <input type="checkbox" checked={pushToTalk} onChange={(e) => setPushToTalk(e.target.checked)} />
                    <span />
                  </label>
                </div>
                {pushToTalk && (
                  <div className="xp-call-ptt-bind">
                    <div>
                      <div className="xp-call-ptt-title">Push to Talk Keybind</div>
                    </div>
                    <div className="xp-call-ptt-actions">
                      <div className="xp-call-ptt-key">
                        {pttKeybind ? pttKeybind : ""}
                      </div>
                      <button className="xp-button" onClick={() => setPttListening(true)}>
                        {pttListening ? "Press any key..." : "Edit Keybind"}
                      </button>
                    </div>
                  </div>
                )}
              </div>
              <div className="xp-call-ring">
                <div className="xp-call-ring-title">Ringtone</div>
                <div className="xp-call-ring-row">
                  <button className="xp-button" type="button" onClick={() => ringtoneInputRef.current?.click()}>
                    Upload MP3
                  </button>
                  {settings.ringtoneUrl && (
                    <button className="xp-button" type="button" onClick={() => {
                      apiFetch("/api/settings", { method: "PATCH", body: JSON.stringify({ ringtone: null }) }).catch(() => {});
                      setSettings((prev) => ({ ...prev, ringtoneUrl: "" }));
                    }}>
                      Clear
                    </button>
                  )}
                  {!settings.ringtoneUrl && (
                    <span className="xp-muted">Default</span>
                  )}
                </div>
                <input
                  ref={ringtoneInputRef}
                  type="file"
                  accept="audio/mpeg,audio/mp3"
                  style={{ display: "none" }}
                  onChange={(e) => {
                    uploadRingtone(e.target.files?.[0]);
                    e.target.value = "";
                  }}
                />
                {ringtoneError && <div className="xp-error">{ringtoneError}</div>}
              </div>
                <div className="xp-call-effects">
                  <div className="xp-call-effects-title">Voice effects</div>
                  <div className="xp-call-effects-row">
                    {["none", "pitch", "robot", "warm"].map((fx) => (
                    <button
                      key={fx}
                      className={`xp-button ${voiceEffect === fx ? "active" : ""}`}
                      onClick={() => setVoiceEffect(fx)}
                    >
                      {fx === "none"
                        ? "Off"
                        : fx === "pitch"
                        ? "Pitch"
                        : fx === "robot"
                        ? "Robot"
                        : "Warm"}
                    </button>
                  ))}
                  </div>
                  {voiceEffect === "pitch" && (
                    <label className="xp-call-effect-slider">
                      Pitch
                      <span className="xp-call-effect-value">{pitchShift.toFixed(2)}</span>
                      <input
                        type="range"
                        min="-10"
                        max="10"
                        step="0.1"
                        value={pitchShift}
                        onChange={(e) => {
                          const next = Math.max(-10, Math.min(10, Number(e.target.value)));
                          setPitchShift(next);
                        }}
                      />
                    </label>
                  )}
                  <div className="xp-call-eq">
                  <div className="xp-call-eq-row">
                    <label>Low</label>
                    <input type="range" min="-8" max="8" step="1" value={eqLow} onChange={(e) => setEqLow(Number(e.target.value))} />
                  </div>
                  <div className="xp-call-eq-row">
                    <label>Mid</label>
                    <input type="range" min="-8" max="8" step="1" value={eqMid} onChange={(e) => setEqMid(Number(e.target.value))} />
                  </div>
                  <div className="xp-call-eq-row">
                    <label>High</label>
                    <input type="range" min="-8" max="8" step="1" value={eqHigh} onChange={(e) => setEqHigh(Number(e.target.value))} />
                  </div>
                  <button className="xp-button" onClick={() => { setEqLow(0); setEqMid(0); setEqHigh(0); }}>
                    Reset EQ
                  </button>
                </div>
              </div>
</div>
          </div>
        </div>
      )}

      {blackjackPanelOpen && (
        <div className="xp-modal-overlay xp-call-overlay" onClick={() => setBlackjackPanelOpen(false)}>
          <div className="xp-modal xp-blackjack-modal" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Blackjack Activity</span>
              <button className="xp-modal-close" onClick={() => setBlackjackPanelOpen(false)}>x</button>
            </div>
            <div className="xp-modal-body">
              {!blackjackMatch && (
                <div className="xp-blackjack-setup">
                  <div className="xp-blackjack-setup-card">
                    <div className="xp-blackjack-title">Invite a friend</div>
                    <div className="xp-blackjack-sub">
                      Create a 1v1 table inside your call. Set how much each player risks, then choose the token and network.
                    </div>
                  <div className="xp-blackjack-form">
                      <label>
                        Friend
                        <select
                          value={String(blackjackInvite.targetId || blackjackTargetId || "")}
                          onChange={(e) =>
                            setBlackjackInvite((prev) => ({ ...prev, targetId: e.target.value }))
                          }
                        >
                          <option value="">Select a friend</option>
                          {friends.map((friend) => (
                            <option key={friend.id} value={friend.id}>
                              @{friend.username}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label>
                        Wager
                        <input
                          type="number"
                          min="0"
                          step="0.01"
                          inputMode="decimal"
                          placeholder="Example: 10"
                          value={blackjackInvite.wager}
                          onChange={(e) =>
                            setBlackjackInvite((prev) => ({ ...prev, wager: e.target.value }))
                          }
                        />
                        <small>Each player deposits this same amount.</small>
                      </label>
                      <label>
                        Token
                        <select
                          value={blackjackInvite.token}
                          onChange={(e) =>
                            setBlackjackInvite((prev) => ({ ...prev, token: e.target.value }))
                          }
                        >
                          {BLACKJACK_TOKENS.map((t) => (
                            <option key={t} value={t}>
                              {t}
                            </option>
                          ))}
                        </select>
                        <small>What both players will deposit and win in.</small>
                      </label>
                      <label>
                        Chain
                        <select
                          value={blackjackInvite.chain}
                          onChange={(e) =>
                            setBlackjackInvite((prev) => ({ ...prev, chain: e.target.value }))
                          }
                        >
                          {BLACKJACK_CHAINS.map((c) => (
                            <option key={c.id} value={c.id}>
                              {c.label}
                            </option>
                          ))}
                        </select>
                        <small>{formatGameTokenLabel(blackjackInvite.token)} on {formatGameChainLabel(blackjackInvite.chain)}.</small>
                      </label>
                    </div>
                    <button
                      className="xp-button"
                      onClick={() => sendBlackjackInvite(blackjackTargetId)}
                      disabled={!blackjackTargetId}
                    >
                      Invite {blackjackTargetUser?.username ? `@${blackjackTargetUser.username}` : "Friend"}
                    </button>
                    {blackjackError && <div className="xp-error">{blackjackError}</div>}
                  </div>
                </div>
              )}

              {blackjackMatch && (
                <div className="xp-blackjack-room">
                  {(blackjackMatch.status === "active" || blackjackMatch.status === "ended" || blackjackMatch.status === "settled") &&
                    renderGameAudioStrip("casino")}
                  <div className="xp-blackjack-header">
                    <div>
                      <div className="xp-blackjack-title">Pot</div>
                      <div className="xp-blackjack-pot">
                        {blackjackPot.toFixed(2)} {blackjackMatch.token}
                      </div>
                    </div>
                    <div className="xp-blackjack-meta">
                      <span>{blackjackMatch.chain}</span>
                      <span>{blackjackMatch.status}</span>
                    </div>
                  </div>

                  {blackjackMatch.status === "deposit" && (
                    <div className="xp-blackjack-deposit">
                      <div className="xp-blackjack-deposit-info">
                        <div className="xp-blackjack-title">Deposit to Join</div>
                        <div className="xp-blackjack-sub">
                          Send exactly {Number(blackjackMatch.wager_amount).toFixed(2)} {blackjackMatch.token} to the match escrow.
                        </div>
                        <div className="xp-blackjack-address">
                          {blackjackMatch.escrow_address || "Generating address..."}
                        </div>
                        {blackjackMatch.escrow_address && (
                          <button
                            className="xp-button"
                            type="button"
                            onClick={() => copyEscrowAddress(blackjackMatch.escrow_address, setBlackjackUiMessage)}
                          >
                            Copy Address
                          </button>
                        )}
                        <div className="xp-blackjack-timer">
                          Time left: {formatDuration(Math.ceil(blackjackDepositRemaining / 1000))}
                        </div>
                        <div className="xp-blackjack-sub">
                          Network: {BLACKJACK_CHAIN_LABELS[blackjackMatch.chain] || blackjackMatch.chain}
                        </div>
                        {selectedWalletOption && (
                          <div className="xp-blackjack-sub">
                            Wallet: {selectedWalletOption.label}
                            {blackjackWallet ? ` · ${blackjackWallet.slice(0, 6)}...${blackjackWallet.slice(-4)}` : ""}
                          </div>
                        )}
                        <div className="xp-blackjack-deposit-list">
                          {blackjackPlayers.map((p) => {
                            const info = resolveUserById(p.user_id, { user, friends, friendsAll, manualDmUsers, allUsers });
                            return (
                              <div key={p.user_id} className="xp-blackjack-deposit-row">
                                <img
                                  src={avatarUrl(info?.avatar, info?.username || "U")}
                                  alt=""
                                  onError={(e) => onAvatarError(e, info?.username || "U")}
                                />
                                <div>
                                  <div className="xp-blackjack-deposit-name">@{info?.username || "user"}</div>
                                  <div className="xp-blackjack-deposit-wallet">
                                    {p.wallet_address ? `${p.wallet_address.slice(0, 6)}...${p.wallet_address.slice(-4)}` : "Wallet not linked"}
                                  </div>
                                </div>
                                <div className={`xp-blackjack-deposit-status ${p.status}`}>{p.status}</div>
                              </div>
                            );
                          })}
                        </div>
                        <div className="xp-blackjack-deposit-actions">
                          {CHAIN_MODE === "evm" ? (
                            <>
                              <button
                                className="xp-button"
                                onClick={() => openWalletPicker("connect")}
                                disabled={blackjackActionBusy}
                              >
                                {selectedWalletOption
                                  ? `${selectedWalletOption.label}${blackjackWalletRegistered ? " Connected" : ""}`
                                  : "Choose Wallet"}
                              </button>
                              <button
                                className="xp-button"
                                onClick={depositBlackjackOnchain}
                                disabled={blackjackActionBusy || !blackjackEscrowReady || !selectedWalletOption}
                              >
                                Approve + Deposit
                              </button>
                              <button
                                className="xp-button"
                                onClick={syncBlackjackMatch}
                                disabled={blackjackActionBusy}
                              >
                                Refresh
                              </button>
                            </>
                          ) : (
                            <button
                              className="xp-button"
                              onClick={simulateBlackjackDeposit}
                              disabled={blackjackActionBusy}
                            >
                              Mark Deposited (dev)
                            </button>
                          )}
                        </div>
                        {CHAIN_MODE === "evm" && !blackjackEscrowReady && (
                          <div className="xp-muted">Waiting for every player to connect a wallet before deposits open.</div>
                        )}
                        {CHAIN_MODE === "evm" && selectedWalletOption && blackjackMatch.chain && (
                          <div className="xp-muted">
                            Deposit with {selectedWalletOption.label} on {BLACKJACK_CHAIN_LABELS[blackjackMatch.chain] || blackjackMatch.chain}.
                          </div>
                        )}
                        {blackjackUiMessage && <div className="xp-success">{blackjackUiMessage}</div>}
                      </div>
                      <div className="xp-blackjack-qr">
                        {blackjackMatch.escrow_address && (
                          <img
                            src={`https://api.qrserver.com/v1/create-qr-code/?size=160x160&data=${encodeURIComponent(
                              blackjackMatch.escrow_address
                            )}`}
                            alt="Deposit QR"
                          />
                        )}
                      </div>
                    </div>
                  )}

                  {(blackjackMatch.status === "active" || blackjackMatch.status === "ended" || blackjackMatch.status === "settled") && (
                    <div className="xp-blackjack-table">
                      <div className="xp-blackjack-dealer">
                        <div className="xp-blackjack-name">Dealer</div>
                        <div className="xp-blackjack-cards">
                          {(blackjackState?.dealer?.cards || []).map((card, idx) =>
                            renderBjCard(card, idx, blackjackState?.dealer?.status !== "reveal" && idx === 1)
                          )}
                        </div>
                      </div>
                      <div className="xp-blackjack-players">
                        {blackjackPlayers.map((p) => {
                          const info = resolveUserById(p.user_id, { user, friends, friendsAll, manualDmUsers, allUsers });
                          const pState = blackjackState?.players?.[p.user_id];
                          return (
                            <div
                              key={p.user_id}
                              className={`xp-blackjack-player ${
                                blackjackState?.currentPlayerId === p.user_id ? "active" : ""
                              }`}
                            >
                              <div className="xp-blackjack-player-header">
                                <img
                                  src={avatarUrl(info?.avatar, info?.username || "U")}
                                  alt=""
                                  onError={(e) => onAvatarError(e, info?.username || "U")}
                                />
                                <div>
                                  <div className="xp-blackjack-name">@{info?.username || "user"}</div>
                                  <div className="xp-blackjack-sub">
                                    Bet: {Number(blackjackMatch.wager_amount).toFixed(2)} {blackjackMatch.token}
                                  </div>
                                </div>
                              </div>
                              <div className="xp-blackjack-cards">
                                {(pState?.hands || []).map((hand, handIdx) => (
                                  <div key={`${p.user_id}-hand-${handIdx}`} className="xp-blackjack-hand">
                                    <div className="xp-blackjack-hand-label">
                                      Hand {handIdx + 1} · {hand.status}
                                    </div>
                                    <div className="xp-blackjack-hand-cards">
                                      {hand.cards.map((card, idx) => renderBjCard(card, idx))}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          );
                        })}
                      </div>

                      {blackjackMatch.status === "active" && (
                        <div className="xp-blackjack-actions">
                          <div className="xp-blackjack-turn">
                            {blackjackIsMyTurn ? "Your turn" : "Waiting..."}
                          </div>
                          <div className="xp-blackjack-buttons">
                            <button
                              className="xp-button"
                              disabled={!blackjackIsMyTurn || blackjackActionBusy}
                              onClick={() => blackjackAction("hit")}
                            >
                              Hit
                            </button>
                            <button
                              className="xp-button"
                              disabled={!blackjackIsMyTurn || blackjackActionBusy}
                              onClick={() => blackjackAction("stand")}
                            >
                              Stand
                            </button>
                            <button
                              className="xp-button"
                              disabled={
                                !blackjackIsMyTurn ||
                                blackjackActionBusy ||
                                (blackjackState?.players?.[user?.id]?.hands?.[
                                  blackjackState?.players?.[user?.id]?.activeHand || 0
                                ]?.cards?.length || 0) !== 2
                              }
                              onClick={() => blackjackAction("double")}
                            >
                              Double
                            </button>
                            <button
                              className="xp-button"
                              disabled={!blackjackIsMyTurn || blackjackActionBusy}
                              onClick={() => blackjackAction("split")}
                            >
                              Split
                            </button>
                          </div>
                        </div>
                      )}

                      {blackjackMatch.status === "ended" && (
                        <div className="xp-blackjack-results">
                          {blackjackMatch.winner_id === user?.id ? (
                            <>
                              <div className="xp-blackjack-win">You won the match.</div>
                              <div className="xp-button-row">
                                <button className="xp-button" onClick={() => setBlackjackClaimOpen(true)}>
                                  Claim Winnings
                                </button>
                                <button className="xp-button" onClick={prepareBlackjackRematch}>
                                  Rematch
                                </button>
                              </div>
                            </>
                          ) : (
                            <>
                              <div className="xp-blackjack-lose">You lost. Better luck next round.</div>
                              <button className="xp-button" onClick={prepareBlackjackRematch}>
                                Rematch
                              </button>
                            </>
                          )}
                        </div>
                      )}

                      {blackjackMatch.status === "settled" && (
                        <div className="xp-blackjack-results">
                          <div className="xp-blackjack-win">
                            Settlement complete {blackjackMatch.settlement_tx ? `(${blackjackMatch.settlement_tx})` : ""}
                          </div>
                          <button className="xp-button" onClick={prepareBlackjackRematch}>
                            Rematch
                          </button>
                        </div>
                      )}
                    </div>
                  )}

                  {blackjackError && <div className="xp-error">{blackjackError}</div>}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {miniGameHubOpen && (
        <div className="xp-modal-overlay xp-call-overlay" onClick={() => setMiniGameHubOpen(false)}>
          <div className="xp-modal xp-minigame-modal" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>{miniGameGameLabel}</span>
              <button className="xp-modal-close" onClick={() => setMiniGameHubOpen(false)}>x</button>
            </div>
            <div className="xp-modal-body">
              {!miniGameMatch && (
                <div className="xp-minigame-setup">
                  <div className="xp-minigame-card">
                    <div className="xp-minigame-title">Invite to {miniGameGameLabel}</div>
                    <div className="xp-minigame-sub">
                      {miniGameSelectedType === "coinflip"
                        ? "Set the wager, send the invite, then both players match the same deposit before the flip."
                        : "Set the opening wager, send the invite, then both players secretly lock their amount before the round resolves."}
                    </div>
                    <div className="xp-minigame-form">
                      <label>
                        Friend
                        <select
                          value={String(miniGameInvite.targetId || miniGameTargetId || "")}
                          onChange={(e) =>
                            setMiniGameInvite((prev) => ({ ...prev, targetId: e.target.value }))
                          }
                        >
                          <option value="">Select a friend</option>
                          {friends.map((friend) => (
                            <option key={friend.id} value={friend.id}>
                              @{friend.username}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label>
                        Wager
                        <input
                          type="number"
                          min="0"
                          step="0.01"
                          inputMode="decimal"
                          placeholder="Example: 10"
                          value={miniGameInvite.wager}
                          onChange={(e) =>
                            setMiniGameInvite((prev) => ({ ...prev, wager: e.target.value }))
                          }
                        />
                        <small>
                          {miniGameSelectedType === "coinflip"
                            ? "Both players match this exact amount."
                            : "This is the starting amount. Players can deposit more when the round locks."}
                        </small>
                      </label>
                      <label>
                        Token
                        <select
                          value={miniGameInvite.token}
                          onChange={(e) =>
                            setMiniGameInvite((prev) => ({ ...prev, token: e.target.value }))
                          }
                        >
                          {BLACKJACK_TOKENS.map((t) => (
                            <option key={t} value={t}>
                              {t}
                            </option>
                          ))}
                        </select>
                        <small>What the pot will be held and paid out in.</small>
                      </label>
                      <label>
                        Chain
                        <select
                          value={miniGameInvite.chain}
                          onChange={(e) =>
                            setMiniGameInvite((prev) => ({ ...prev, chain: e.target.value }))
                          }
                        >
                          {BLACKJACK_CHAINS.map((c) => (
                            <option key={c.id} value={c.id}>
                              {c.label}
                            </option>
                          ))}
                        </select>
                        <small>{formatGameTokenLabel(miniGameInvite.token)} on {formatGameChainLabel(miniGameInvite.chain)}.</small>
                      </label>
                    </div>
                    <button
                      className="xp-button"
                      onClick={() => sendMiniGameInvite(miniGameTargetId)}
                      disabled={!miniGameTargetId || miniGameBusy}
                    >
                      Invite {miniGameTargetId ? `@${resolveUserById(miniGameTargetId, { user, friends, friendsAll, manualDmUsers, allUsers })?.username || "Friend"}` : "Friend"}
                    </button>
                    {miniGameError && <div className="xp-error">{miniGameError}</div>}
                  </div>
                </div>
              )}

              {miniGameMatch && (
                <div className="xp-minigame-room">
                  {(miniGameMatch.status === "active" ||
                    miniGameMatch.status === "ended" ||
                    miniGameMatch.status === "settled") &&
                    renderGameAudioStrip(miniGameMatch?.game_type === "coinflip" ? "coinflip" : "casino")}
                  <div className="xp-minigame-header">
                    <div>
                      <div className="xp-minigame-title">{miniGameGameLabel}</div>
                      <div className="xp-minigame-sub">
                        {miniGameMatch.game_type === "coinflip"
                          ? "Match the wager, flip the coin, take the pot."
                          : "Secretly lock your amount. Highest deposit takes everything."}
                      </div>
                    </div>
                    <div className="xp-minigame-pot">
                      Pot: {miniGamePot.toFixed(2)} {miniGameMatch.token}
                    </div>
                  </div>

                  <div className="xp-minigame-players">
                    {miniGamePlayers.map((player) => {
                      const info = resolveUserById(player.user_id, { user, friends, friendsAll, manualDmUsers, allUsers });
                      return (
                        <div key={player.user_id} className={`xp-minigame-seat ${miniGameMatch.winner_id === player.user_id ? "winner" : ""}`}>
                          <img
                            src={avatarUrl(info?.avatar, info?.username || "U")}
                            alt=""
                            onError={(e) => onAvatarError(e, info?.username || "U")}
                          />
                          <div className="xp-minigame-seat-meta">
                            <div className="xp-minigame-seat-name">@{info?.username || "user"}</div>
                            <div className="xp-minigame-seat-status">{player.status}</div>
                            {miniGameMatch.game_type === "coinflip" && (
                              <div className="xp-minigame-seat-side">
                                {miniGameAssignments[player.user_id]
                                  ? miniGameAssignments[player.user_id].toUpperCase()
                                  : "Waiting side"}
                              </div>
                            )}
                          </div>
                          <div className="xp-minigame-seat-amount">
                            {player.status === "deposited" || miniGameMatch.status === "ended" || miniGameMatch.status === "settled"
                              ? `${Number(player.deposited_amount || player.deposit_amount || 0).toFixed(2)} ${miniGameMatch.token}`
                              : miniGameMatch.game_type === "coinflip"
                                ? `${Number(miniGameMatch.wager_amount).toFixed(2)} ${miniGameMatch.token}`
                                : "Hidden"}
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  {miniGameMatch.status === "invited" && (
                    <div className="xp-minigame-stage">
                      <div className="xp-minigame-banner">
                        Invite sent. Waiting for @{resolveUserById(miniGameOpponent?.user_id, { user, friends, friendsAll, manualDmUsers, allUsers })?.username || "friend"} to accept.
                      </div>
                    </div>
                  )}

                  {miniGameMatch.status === "deposit" && (
                    <div className="xp-minigame-stage">
                      <div className="xp-minigame-countdown">
                        Round starts in {formatDuration(Math.ceil(miniGameCountdownRemaining / 1000))}
                      </div>
                      <div className="xp-minigame-sub">
                        {miniGameMatch.game_type === "coinflip"
                          ? `Lock exactly ${Number(miniGameMatch.wager_amount).toFixed(2)} ${miniGameMatch.token}.`
                          : "Choose your hidden deposit. Highest amount wins the combined pot."}
                      </div>
                      {selectedWalletOption && (
                        <div className="xp-minigame-sub">
                          Wallet: {selectedWalletOption.label}
                          {miniGameWalletRegistered
                            ? ` · ${miniGameWalletRegistered.slice(0, 6)}...${miniGameWalletRegistered.slice(-4)}`
                            : ""}
                        </div>
                      )}
                      {miniGameMatch.escrow_address && CHAIN_MODE === "evm" && (
                        <>
                          <div className="xp-blackjack-address">{miniGameMatch.escrow_address}</div>
                          <button
                            className="xp-button"
                            type="button"
                            onClick={() => copyEscrowAddress(miniGameMatch.escrow_address, setMiniGameUiMessage)}
                          >
                            Copy Address
                          </button>
                        </>
                      )}
                      <div className="xp-minigame-action-row">
                        <label>
                          {miniGameMatch.game_type === "coinflip" ? "Required amount" : "Your deposit"}
                          <input
                            value={miniGameMatch.game_type === "coinflip" ? String(miniGameMatch.wager_amount) : miniGameDepositAmount}
                            onChange={(e) => setMiniGameDepositAmount(e.target.value)}
                            disabled={miniGameMatch.game_type === "coinflip"}
                          />
                        </label>
                        {CHAIN_MODE === "evm" ? (
                          <>
                            <button className="xp-button" onClick={() => openWalletPicker("connect")} disabled={miniGameBusy}>
                              {selectedWalletOption ? selectedWalletOption.label : "Choose Wallet"}
                            </button>
                            <button
                              className="xp-button"
                              onClick={depositMiniGameOnchain}
                              disabled={miniGameBusy || !selectedWalletOption || !miniGameEscrowReady}
                            >
                              Approve + Deposit
                            </button>
                            <button className="xp-button" onClick={syncMiniGameMatch} disabled={miniGameBusy}>
                              Refresh
                            </button>
                          </>
                        ) : (
                          <button className="xp-button" onClick={depositMiniGame} disabled={miniGameBusy}>
                            Lock Wager
                          </button>
                        )}
                      </div>
                      {CHAIN_MODE === "evm" && !miniGameEscrowReady && (
                        <div className="xp-muted">Waiting for every player to register a wallet before deposits open.</div>
                      )}
                      {CHAIN_MODE === "evm" && selectedWalletOption && miniGameMatch.chain && (
                        <div className="xp-muted">
                          Deposit with {selectedWalletOption.label} on {BLACKJACK_CHAIN_LABELS[miniGameMatch.chain] || miniGameMatch.chain}.
                        </div>
                      )}
                      {miniGameUiMessage && <div className="xp-success">{miniGameUiMessage}</div>}
                    </div>
                  )}

                  {miniGameMatch.status === "declined" && (
                    <div className="xp-minigame-stage">
                      <div className="xp-minigame-loss">Invite declined. No funds were locked.</div>
                    </div>
                  )}

                  {miniGameMatch.status === "ended" && (
                    <div className="xp-minigame-stage">
                      {miniGameMatch.game_type === "coinflip" ? (
                        <div className={`xp-coinflip-stage ${miniGameMatch.state?.result || ""}`}>
                          <div className="xp-coinflip-coin">
                            <span>{(miniGameMatch.state?.result || "?").slice(0, 1).toUpperCase()}</span>
                          </div>
                          <div className="xp-minigame-sub">
                            Coin landed on {(miniGameMatch.state?.result || "unknown").toUpperCase()}.
                          </div>
                        </div>
                      ) : (
                        <div className="xp-bigbank-stage">
                          <div className="xp-bigbank-bars">
                            {miniGamePlayers.map((player) => {
                              const info = resolveUserById(player.user_id, { user, friends, friendsAll, manualDmUsers, allUsers });
                              const amount = Number(player.deposited_amount || player.deposit_amount || 0);
                              const max = Math.max(...miniGamePlayers.map((p) => Number(p.deposited_amount || p.deposit_amount || 0)), 1);
                              return (
                                <div key={player.user_id} className="xp-bigbank-bar-row">
                                  <span>@{info?.username || "user"}</span>
                                  <div className="xp-bigbank-bar-track">
                                    <div
                                      className="xp-bigbank-bar-fill"
                                      style={{ width: `${Math.max(12, (amount / max) * 100)}%` }}
                                    />
                                  </div>
                                  <strong>{amount.toFixed(2)}</strong>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}
                      <div className={miniGameWinner?.id === user?.id ? "xp-minigame-win" : "xp-minigame-loss"}>
                        {miniGameWinner?.id === user?.id
                          ? `You won ${miniGamePot.toFixed(2)} ${miniGameMatch.token}.`
                          : `@${miniGameWinner?.username || "winner"} won ${miniGamePot.toFixed(2)} ${miniGameMatch.token}.`}
                      </div>
                      {miniGameWinner?.id === user?.id && miniGameMatch.status === "ended" && (
                        <div className="xp-button-row">
                          <button className="xp-button" onClick={() => setMiniGameClaimOpen(true)}>
                            Claim Winnings
                          </button>
                          <button className="xp-button" onClick={prepareMiniGameRematch}>
                            Rematch
                          </button>
                        </div>
                      )}
                      {miniGameWinner?.id !== user?.id && (
                        <button className="xp-button" onClick={prepareMiniGameRematch}>
                          Rematch
                        </button>
                      )}
                    </div>
                  )}

                  {miniGameMatch.status === "refunded" && (
                    <div className="xp-minigame-stage">
                      <div className="xp-minigame-loss">
                        Big Bank Small Bank tied on the top deposit. The round was refunded.
                      </div>
                      {miniGameMatch.settlement_tx && (
                        <div className="xp-muted">Refund tx: {miniGameMatch.settlement_tx}</div>
                      )}
                      <button className="xp-button" onClick={prepareMiniGameRematch}>
                        Rematch
                      </button>
                    </div>
                  )}

                  {miniGameMatch.status === "settled" && (
                    <div className="xp-minigame-stage">
                      <div className="xp-minigame-win">
                        Settlement complete {miniGameMatch.settlement_tx ? `(${miniGameMatch.settlement_tx})` : ""}
                      </div>
                      <button className="xp-button" onClick={prepareMiniGameRematch}>
                        Rematch
                      </button>
                    </div>
                  )}

                  {miniGameError && <div className="xp-error">{miniGameError}</div>}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {miniGameClaimOpen && (
        <div className="xp-modal-overlay xp-call-overlay" onClick={() => setMiniGameClaimOpen(false)}>
          <div className="xp-modal xp-blackjack-claim" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Claim Mini Game Winnings</span>
              <button className="xp-modal-close" onClick={() => setMiniGameClaimOpen(false)}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-blackjack-claim-wallet-row">
                <button className="xp-button" onClick={() => openWalletPicker("claim")} disabled={miniGameBusy}>
                  {selectedWalletOption ? `Wallet: ${selectedWalletOption.label}` : "Choose Wallet"}
                </button>
                {miniGameClaimAddress && miniGameClaimAddress.startsWith("0x") && (
                  <div className="xp-muted">
                    {miniGameClaimAddress.slice(0, 6)}...{miniGameClaimAddress.slice(-4)}
                  </div>
                )}
              </div>
              <label>
                Wallet address
                <input
                  value={miniGameClaimAddress}
                  onChange={(e) => setMiniGameClaimAddress(e.target.value)}
                  placeholder="0x..."
                />
              </label>
              <div className="xp-button-row">
                <button className="xp-button" onClick={claimMiniGameWinnings} disabled={miniGameBusy}>
                  Claim
                </button>
                <button className="xp-button" onClick={() => setMiniGameClaimOpen(false)}>
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {blackjackClaimOpen && (
        <div className="xp-modal-overlay xp-call-overlay" onClick={() => setBlackjackClaimOpen(false)}>
          <div className="xp-modal xp-blackjack-claim" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Claim Winnings</span>
              <button className="xp-modal-close" onClick={() => setBlackjackClaimOpen(false)}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-blackjack-claim-wallet-row">
                <button className="xp-button" onClick={() => openWalletPicker("claim")} disabled={blackjackActionBusy}>
                  {selectedWalletOption ? `Wallet: ${selectedWalletOption.label}` : "Choose Wallet"}
                </button>
                {blackjackWallet && (
                  <div className="xp-muted">
                    {blackjackWallet.slice(0, 6)}...{blackjackWallet.slice(-4)}
                  </div>
                )}
              </div>
              <label>
                Wallet address
                <input
                  value={blackjackClaimAddress}
                  onChange={(e) => setBlackjackClaimAddress(e.target.value)}
                  placeholder="0x..."
                />
              </label>
              <div className="xp-button-row">
                <button className="xp-button" onClick={claimBlackjackWinnings} disabled={blackjackActionBusy}>
                  Claim
                </button>
                <button className="xp-button" onClick={() => setBlackjackClaimOpen(false)}>
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {walletPickerOpen && (
        <div className="xp-modal-overlay xp-call-overlay" onClick={() => setWalletPickerOpen(false)}>
          <div className="xp-modal xp-wallet-modal" onClick={(e) => e.stopPropagation()}>
            <div className="xp-modal-title">
              <span>Choose Wallet</span>
              <button className="xp-modal-close" onClick={() => setWalletPickerOpen(false)}>x</button>
            </div>
            <div className="xp-modal-body">
              <div className="xp-wallet-picker">
                {walletOptions.length === 0 && (
                  <div className="xp-muted">No supported wallet was detected. Install an EVM wallet or configure WalletConnect.</div>
                )}
                {walletOptions.map((option) => (
                  <button
                    key={option.id}
                    className={`xp-wallet-option ${blackjackWalletProviderId === option.id ? "active" : ""}`}
                    onClick={() => chooseBlackjackWallet(option)}
                  >
                    <div className="xp-wallet-option-title">{option.label}</div>
                    <div className="xp-wallet-option-sub">
                      {option.type === "walletconnect" ? "Mobile and desktop wallets" : "Browser extension / injected wallet"}
                    </div>
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      <audio ref={gameMusicAudioRef} className="xp-hidden" aria-hidden="true" />
      <audio ref={coinflipSfxAudioRef} className="xp-hidden" aria-hidden="true" />

      {groupCall.participants.map((pid) => (
        <audio key={pid} id={`xp-group-audio-${pid}`} />
      ))}

      <audio id="xp-remote-audio" />
      <audio ref={micTestAudioRef} />
      <audio ref={profileSongAudioRef} />
    </div>
  );
}
