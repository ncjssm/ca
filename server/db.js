import fs from "fs";
import path from "path";
import initSqlJs from "sql.js";
import { fileURLToPath } from "url";
import os from "os";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const legacyDbPath = path.join(__dirname, "db", "chat.sqlite");
const dbBase =
  process.env.DB_DIR ||
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
    "CREATE TABLE IF NOT EXISTS notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, type TEXT NOT NULL, message TEXT NOT NULL, from_user_id INTEGER, group_id INTEGER, context TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, read_at TEXT)"
  );

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
