import fs from "fs";
import path from "path";
import initSqlJs from "sql.js";
import { fileURLToPath } from "url";
import os from "os";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const legacyDbPath = path.join(__dirname, "db", "chat.sqlite");
const volumePath =
  process.env.RAILWAY_VOLUME_MOUNT_PATH ||
  process.env.RAILWAY_VOLUME_PATH ||
  process.env.VOLUME_PATH ||
  process.env.RAILWAY_VOLUME ||
  "";
const dbBase =
  process.env.DB_DIR ||
  volumePath ||
  (process.env.LOCALAPPDATA
    ? path.join(process.env.LOCALAPPDATA, "xp-chat")
    : process.env.APPDATA
    ? path.join(process.env.APPDATA, "xp-chat")
    : path.join(os.homedir(), ".xp-chat"));
const dbPath = process.env.DB_PATH || path.join(dbBase, "chat.sqlite");
const dbBackupPath = `${dbPath}.bak`;

let db;

export async function initDb() {
  const SQL = await initSqlJs({
    locateFile: (file) =>
      path.join(__dirname, "node_modules", "sql.js", "dist", file),
  });

  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  if (!fs.existsSync(dbPath) && fs.existsSync(legacyDbPath)) {
    fs.copyFileSync(legacyDbPath, dbPath);
  }

  if (fs.existsSync(dbPath)) {
    const data = fs.readFileSync(dbPath);
    db = new SQL.Database(data);
  } else if (fs.existsSync(dbBackupPath)) {
    const data = fs.readFileSync(dbBackupPath);
    db = new SQL.Database(data);
  } else {
    db = new SQL.Database();
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      display_name TEXT,
      custom_status TEXT,
      bio TEXT,
      ringtone_url TEXT,
      song_title TEXT,
      song_artist TEXT,
      song_album TEXT,
      song_cover_url TEXT,
      song_audio_url TEXT,
      song_source TEXT,
      song_source_url TEXT,
      song_updated_at TEXT,
      password_hash TEXT NOT NULL,
      email TEXT,
      email_verified INTEGER NOT NULL DEFAULT 0,
      avatar TEXT,
      status TEXT NOT NULL DEFAULT 'online',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      last_seen TEXT
    );

    CREATE TABLE IF NOT EXISTS username_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      old_username TEXT NOT NULL,
      changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sender_id INTEGER NOT NULL,
      recipient_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT 'text',
      image_url TEXT,
      audio_url TEXT,
      forwarded_from_id INTEGER,
      forwarded_from_username TEXT,
      forwarded_from_display TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      edited_at TEXT,
      deleted_at TEXT
    );

    CREATE TABLE IF NOT EXISTS friend_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      requester_id INTEGER NOT NULL,
      recipient_id INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS friendships (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user1_id INTEGER NOT NULL,
      user2_id INTEGER NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS groups (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      owner_id INTEGER NOT NULL,
      avatar TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS group_members (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      group_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      role TEXT NOT NULL DEFAULT 'member',
      joined_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS group_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      group_id INTEGER NOT NULL,
      sender_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT 'text',
      image_url TEXT,
      audio_url TEXT,
      forwarded_from_id INTEGER,
      forwarded_from_username TEXT,
      forwarded_from_display TEXT,
      is_system INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      edited_at TEXT,
      deleted_at TEXT
    );

    CREATE TABLE IF NOT EXISTS dm_reads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      other_id INTEGER NOT NULL,
      last_read_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS group_reads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      group_id INTEGER NOT NULL,
      last_read_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      type TEXT NOT NULL,
      message TEXT NOT NULL,
      from_user_id INTEGER,
      group_id INTEGER,
      context TEXT,
      ref_id INTEGER,
      payload TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      read_at TEXT
    );

    CREATE TABLE IF NOT EXISTS message_reactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      message_type TEXT NOT NULL,
      message_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      emoji TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(message_type, message_id, user_id, emoji)
    );

    CREATE TABLE IF NOT EXISTS private_notes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      owner_id INTEGER NOT NULL,
      target_id INTEGER NOT NULL,
      note TEXT,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(owner_id, target_id)
    );

    CREATE TABLE IF NOT EXISTS stories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      media_url TEXT NOT NULL,
      media_type TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      expires_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS story_views (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      story_id INTEGER NOT NULL,
      viewer_id INTEGER NOT NULL,
      viewed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(story_id, viewer_id)
    );

    CREATE TABLE IF NOT EXISTS username_aliases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      username TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS username_actions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      action TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS username_transfers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL,
      from_user_id INTEGER NOT NULL,
      to_user_id INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      expires_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS connections (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      service TEXT NOT NULL,
      handle TEXT NOT NULL,
      url TEXT,
      visibility TEXT NOT NULL DEFAULT 'public',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
  `);

  const columns = db.exec("PRAGMA table_info(users)")[0]?.values || [];
  const hasDisplayName = columns.some((col) => col[1] === "display_name");
  if (!hasDisplayName) {
    db.exec("ALTER TABLE users ADD COLUMN display_name TEXT");
  }
  const hasCustomStatus = columns.some((col) => col[1] === "custom_status");
  if (!hasCustomStatus) {
    db.exec("ALTER TABLE users ADD COLUMN custom_status TEXT");
  }
  const hasBio = columns.some((col) => col[1] === "bio");
  if (!hasBio) {
    db.exec("ALTER TABLE users ADD COLUMN bio TEXT");
  }
  const hasRingtone = columns.some((col) => col[1] === "ringtone_url");
  if (!hasRingtone) {
    db.exec("ALTER TABLE users ADD COLUMN ringtone_url TEXT");
  }
  const hasSongTitle = columns.some((col) => col[1] === "song_title");
  if (!hasSongTitle) {
    db.exec("ALTER TABLE users ADD COLUMN song_title TEXT");
  }
  const hasSongArtist = columns.some((col) => col[1] === "song_artist");
  if (!hasSongArtist) {
    db.exec("ALTER TABLE users ADD COLUMN song_artist TEXT");
  }
  const hasSongAlbum = columns.some((col) => col[1] === "song_album");
  if (!hasSongAlbum) {
    db.exec("ALTER TABLE users ADD COLUMN song_album TEXT");
  }
  const hasSongCoverUrl = columns.some((col) => col[1] === "song_cover_url");
  if (!hasSongCoverUrl) {
    db.exec("ALTER TABLE users ADD COLUMN song_cover_url TEXT");
  }
  const hasSongAudioUrl = columns.some((col) => col[1] === "song_audio_url");
  if (!hasSongAudioUrl) {
    db.exec("ALTER TABLE users ADD COLUMN song_audio_url TEXT");
  }
  const hasSongSource = columns.some((col) => col[1] === "song_source");
  if (!hasSongSource) {
    db.exec("ALTER TABLE users ADD COLUMN song_source TEXT");
  }
  const hasSongSourceUrl = columns.some((col) => col[1] === "song_source_url");
  if (!hasSongSourceUrl) {
    db.exec("ALTER TABLE users ADD COLUMN song_source_url TEXT");
  }
  const hasSongUpdatedAt = columns.some((col) => col[1] === "song_updated_at");
  if (!hasSongUpdatedAt) {
    db.exec("ALTER TABLE users ADD COLUMN song_updated_at TEXT");
  }
  const hasEmailVerified = columns.some((col) => col[1] === "email_verified");
  if (!hasEmailVerified) {
    db.exec("ALTER TABLE users ADD COLUMN email_verified INTEGER NOT NULL DEFAULT 0");
  }

  const notifColumns = db.exec("PRAGMA table_info(notifications)")[0]?.values || [];
  const hasNotifFrom = notifColumns.some((col) => col[1] === "from_user_id");
  if (!hasNotifFrom) {
    db.exec("ALTER TABLE notifications ADD COLUMN from_user_id INTEGER");
  }
  const hasNotifGroup = notifColumns.some((col) => col[1] === "group_id");
  if (!hasNotifGroup) {
    db.exec("ALTER TABLE notifications ADD COLUMN group_id INTEGER");
  }
  const hasNotifContext = notifColumns.some((col) => col[1] === "context");
  if (!hasNotifContext) {
    db.exec("ALTER TABLE notifications ADD COLUMN context TEXT");
  }
  const hasNotifRef = notifColumns.some((col) => col[1] === "ref_id");
  if (!hasNotifRef) {
    db.exec("ALTER TABLE notifications ADD COLUMN ref_id INTEGER");
  }
  const hasNotifPayload = notifColumns.some((col) => col[1] === "payload");
  if (!hasNotifPayload) {
    db.exec("ALTER TABLE notifications ADD COLUMN payload TEXT");
  }

  const blackjackColumns =
    db.exec("PRAGMA table_info(blackjack_matches)")[0]?.values || [];
  const hasBlackjackTable = blackjackColumns.length > 0;
  if (hasBlackjackTable) {
    const hasTokenAddress = blackjackColumns.some((col) => col[1] === "token_address");
    if (!hasTokenAddress) {
      db.exec("ALTER TABLE blackjack_matches ADD COLUMN token_address TEXT");
    }
    const hasFactoryAddress = blackjackColumns.some((col) => col[1] === "escrow_factory_address");
    if (!hasFactoryAddress) {
      db.exec("ALTER TABLE blackjack_matches ADD COLUMN escrow_factory_address TEXT");
    }
    const hasEscrowMatchId = blackjackColumns.some((col) => col[1] === "escrow_match_id");
    if (!hasEscrowMatchId) {
      db.exec("ALTER TABLE blackjack_matches ADD COLUMN escrow_match_id TEXT");
    }
    const hasInviteDeadline = blackjackColumns.some((col) => col[1] === "invite_deadline");
    if (!hasInviteDeadline) {
      db.exec("ALTER TABLE blackjack_matches ADD COLUMN invite_deadline TEXT");
    }
  }

  const miniGameColumns =
    db.exec("PRAGMA table_info(mini_game_matches)")[0]?.values || [];
  const hasMiniGameTable = miniGameColumns.length > 0;
  if (hasMiniGameTable) {
    const hasTokenAddress = miniGameColumns.some((col) => col[1] === "token_address");
    if (!hasTokenAddress) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN token_address TEXT");
    }
    const hasFactoryAddress = miniGameColumns.some((col) => col[1] === "escrow_factory_address");
    if (!hasFactoryAddress) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN escrow_factory_address TEXT");
    }
    const hasEscrowMatchId = miniGameColumns.some((col) => col[1] === "escrow_match_id");
    if (!hasEscrowMatchId) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN escrow_match_id TEXT");
    }
    const hasEscrowAddress = miniGameColumns.some((col) => col[1] === "escrow_address");
    if (!hasEscrowAddress) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN escrow_address TEXT");
    }
    const hasDepositDeadline = miniGameColumns.some((col) => col[1] === "deposit_deadline");
    if (!hasDepositDeadline) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN deposit_deadline TEXT");
    }
    const hasClaimAddress = miniGameColumns.some((col) => col[1] === "claim_address");
    if (!hasClaimAddress) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN claim_address TEXT");
    }
    const hasInviteDeadline = miniGameColumns.some((col) => col[1] === "invite_deadline");
    if (!hasInviteDeadline) {
      db.exec("ALTER TABLE mini_game_matches ADD COLUMN invite_deadline TEXT");
    }
  }

  const connectionColumns =
    db.exec("PRAGMA table_info(connections)")[0]?.values || [];
  const hasConnections = connectionColumns.length > 0;
  if (!hasConnections) {
    db.exec(
      "CREATE TABLE IF NOT EXISTS connections (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, service TEXT NOT NULL, handle TEXT NOT NULL, url TEXT, visibility TEXT NOT NULL DEFAULT 'public', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    );
  }

  const messageColumns = db.exec("PRAGMA table_info(messages)")[0]?.values || [];
  const hasMessageType = messageColumns.some((col) => col[1] === "type");
  if (!hasMessageType) {
    db.exec("ALTER TABLE messages ADD COLUMN type TEXT NOT NULL DEFAULT 'text'");
  }
  const hasMessageImage = messageColumns.some((col) => col[1] === "image_url");
  if (!hasMessageImage) {
    db.exec("ALTER TABLE messages ADD COLUMN image_url TEXT");
  }
  const hasMessageAudio = messageColumns.some((col) => col[1] === "audio_url");
  if (!hasMessageAudio) {
    db.exec("ALTER TABLE messages ADD COLUMN audio_url TEXT");
  }
  const hasForwardedFromId = messageColumns.some((col) => col[1] === "forwarded_from_id");
  if (!hasForwardedFromId) {
    db.exec("ALTER TABLE messages ADD COLUMN forwarded_from_id INTEGER");
  }
  const hasForwardedFromUsername = messageColumns.some((col) => col[1] === "forwarded_from_username");
  if (!hasForwardedFromUsername) {
    db.exec("ALTER TABLE messages ADD COLUMN forwarded_from_username TEXT");
  }
  const hasForwardedFromDisplay = messageColumns.some((col) => col[1] === "forwarded_from_display");
  if (!hasForwardedFromDisplay) {
    db.exec("ALTER TABLE messages ADD COLUMN forwarded_from_display TEXT");
  }

  const groupColumns = db.exec("PRAGMA table_info(group_messages)")[0]?.values || [];
  const hasSystem = groupColumns.some((col) => col[1] === "is_system");
  if (!hasSystem) {
    db.exec("ALTER TABLE group_messages ADD COLUMN is_system INTEGER NOT NULL DEFAULT 0");
  }
  const hasGroupType = groupColumns.some((col) => col[1] === "type");
  if (!hasGroupType) {
    db.exec("ALTER TABLE group_messages ADD COLUMN type TEXT NOT NULL DEFAULT 'text'");
  }
  const hasGroupImage = groupColumns.some((col) => col[1] === "image_url");
  if (!hasGroupImage) {
    db.exec("ALTER TABLE group_messages ADD COLUMN image_url TEXT");
  }
  const hasGroupAudio = groupColumns.some((col) => col[1] === "audio_url");
  if (!hasGroupAudio) {
    db.exec("ALTER TABLE group_messages ADD COLUMN audio_url TEXT");
  }
  const hasGroupForwardedFromId = groupColumns.some((col) => col[1] === "forwarded_from_id");
  if (!hasGroupForwardedFromId) {
    db.exec("ALTER TABLE group_messages ADD COLUMN forwarded_from_id INTEGER");
  }
  const hasGroupForwardedFromUsername = groupColumns.some((col) => col[1] === "forwarded_from_username");
  if (!hasGroupForwardedFromUsername) {
    db.exec("ALTER TABLE group_messages ADD COLUMN forwarded_from_username TEXT");
  }
  const hasGroupForwardedFromDisplay = groupColumns.some((col) => col[1] === "forwarded_from_display");
  if (!hasGroupForwardedFromDisplay) {
    db.exec("ALTER TABLE group_messages ADD COLUMN forwarded_from_display TEXT");
  }

  db.exec(
    "CREATE TABLE IF NOT EXISTS message_reactions (id INTEGER PRIMARY KEY AUTOINCREMENT, message_type TEXT NOT NULL, message_id INTEGER NOT NULL, user_id INTEGER NOT NULL, emoji TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(message_type, message_id, user_id, emoji))"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS private_notes (id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL, target_id INTEGER NOT NULL, note TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(owner_id, target_id))"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS stories (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, media_url TEXT NOT NULL, media_type TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, expires_at TEXT NOT NULL)"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS story_views (id INTEGER PRIMARY KEY AUTOINCREMENT, story_id INTEGER NOT NULL, viewer_id INTEGER NOT NULL, viewed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(story_id, viewer_id))"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS username_aliases (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, username TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS username_actions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, action TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS username_transfers (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, from_user_id INTEGER NOT NULL, to_user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, expires_at TEXT NOT NULL)"
  );
  db.exec(
    "CREATE TABLE IF NOT EXISTS connections (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, service TEXT NOT NULL, handle TEXT NOT NULL, url TEXT, visibility TEXT NOT NULL DEFAULT 'public', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
  );

  db.exec(
    "CREATE TABLE IF NOT EXISTS notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, type TEXT NOT NULL, message TEXT NOT NULL, from_user_id INTEGER, group_id INTEGER, context TEXT, ref_id INTEGER, payload TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, read_at TEXT)"
  );

  db.exec(`
    CREATE TABLE IF NOT EXISTS blackjack_matches (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      inviter_id INTEGER NOT NULL,
      status TEXT NOT NULL,
      chain TEXT NOT NULL,
      token TEXT NOT NULL,
      token_address TEXT,
      wager_amount REAL NOT NULL,
      escrow_factory_address TEXT,
      escrow_match_id TEXT,
      escrow_address TEXT,
      invite_deadline TEXT,
      deposit_deadline TEXT,
      players_json TEXT NOT NULL,
      state_json TEXT,
      winner_id INTEGER,
      settlement_tx TEXT,
      claim_address TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT
    );
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS mini_game_matches (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      game_type TEXT NOT NULL,
      inviter_id INTEGER NOT NULL,
      status TEXT NOT NULL,
      chain TEXT NOT NULL,
      token TEXT NOT NULL,
      token_address TEXT,
      wager_amount REAL NOT NULL,
      escrow_factory_address TEXT,
      escrow_match_id TEXT,
      escrow_address TEXT,
      invite_deadline TEXT,
      deposit_deadline TEXT,
      players_json TEXT NOT NULL,
      state_json TEXT,
      winner_id INTEGER,
      settlement_tx TEXT,
      claim_address TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT
    );
  `);

  persistDb();
  return db;
}

export function persistDb() {
  if (!db) return;
  const data = db.export();
  if (fs.existsSync(dbPath)) {
    try {
      fs.copyFileSync(dbPath, dbBackupPath);
    } catch {
      // best-effort backup
    }
  }
  fs.writeFileSync(dbPath, Buffer.from(data));
}

export function getDb() {
  if (!db) {
    throw new Error("Database not initialized");
  }
  return db;
}
