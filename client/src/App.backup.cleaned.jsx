import React, { useEffect, useRef, useState } from "react";
import { io } from "socket.io-client";
import addIcon from "./assets/add.svg";
import bellIcon from "./assets/bell.svg";

const API_URL = "http://localhost:3001";

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

async function apiFetch(path, options = {}) {
  const res = await fetch(`${API_URL}${path}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.error || "Request failed");
  }
  return res.json();
}

function chatKey(type, id) {
  return `${type}:${id}`;
}

export default function App() {
  const [user, setUser] = useState(null);
  const [friends, setFriends] = useState([]);
  const [selectedChat, setSelectedChat] = useState(null);
  const [messagesByChat, setMessagesByChat] = useState({});
  const [view, setView] = useState("chat");
  const [authMode, setAuthMode] = useState("login");
  const [authError, setAuthError] = useState("");
  const [form, setForm] = useState({ username: "", password: "", email: "" });
  const [messageInput, setMessageInput] = useState("");
  const [friendSearch, setFriendSearch] = useState("");
  const [friendError, setFriendError] = useState("");
  const [friendSuccess, setFriendSuccess] = useState("");
  const [addFriendOpen, setAddFriendOpen] = useState(false);
  const [notifications, setNotifications] = useState([]);
  const [showNotifications, setShowNotifications] = useState(false);
  const socketRef = useRef(null);

  useEffect(() => {
    apiFetch("/api/me")
      .then((data) => data?.user && setUser(data.user))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!user) return;
    loadSidebar();
    loadNotifications();
    const socket = io(API_URL, { withCredentials: true });
    socketRef.current = socket;
    socket.on("dm:new", (message) => {
      const key = chatKey(
        "dm",
        message.sender_id === user.id ? message.recipient_id : message.sender_id
      );
      setMessagesByChat((prev) => ({
        ...prev,
        [key]: [...(prev[key] || []), message],
      }));
      loadSidebar();
    });
    socket.on("notify:new", loadNotifications);
    return () => socket.disconnect();
  }, [user]);

  async function loadSidebar() {
    const data = await apiFetch("/api/chats");
    setFriends(data.dms || []);
  }

  async function loadNotifications() {
    const data = await apiFetch("/api/notifications");
    setNotifications(data.notifications || []);
  }

  async function handleAuth(event) {
    event.preventDefault();
    setAuthError("");
    try {
      if (authMode === "register") {
        await apiFetch("/api/register", {
          method: "POST",
          body: JSON.stringify(form),
        });
      } else {
        await apiFetch("/api/login", {
          method: "POST",
          body: JSON.stringify(form),
        });
      }
      const data = await apiFetch("/api/me");
      setUser(data.user);
      setForm({ username: "", password: "", email: "" });
    } catch (err) {
      setAuthError(err.message);
    }
  }

  async function selectDm(userId) {
    setSelectedChat({ type: "dm", id: userId });
    const key = chatKey("dm", userId);
    if (!messagesByChat[key]) {
      const data = await apiFetch(`/api/messages/${userId}`);
      setMessagesByChat((prev) => ({ ...prev, [key]: data.messages || [] }));
    }
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
    } catch (err) {
      setFriendError(err.message);
    }
  }

  async function sendMessage() {
    if (!selectedChat || !socketRef.current) return;
    const trimmed = messageInput.trim();
    if (!trimmed) return;
    socketRef.current.emit("dm:send", { toId: selectedChat.id, body: trimmed });
    setMessageInput("");
  }

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
                <button className="xp-button" type="submit">
                  {authMode === "login" ? "Log In" : "Create Account"}
                </button>
                <button
                  className="xp-button secondary"
                  type="button"
                  onClick={() => setAuthMode(authMode === "login" ? "register" : "login")}
                >
                  {authMode === "login" ? "Register" : "Back to Login"}
                </button>
              </div>
            </form>
          </div>
          <div className="xp-statusbar">Welcome</div>
        </div>
      </div>
    );
  }

  const selectedKey = selectedChat && chatKey(selectedChat.type, selectedChat.id);
  const messages = selectedKey ? messagesByChat[selectedKey] || [] : [];
  const selectedFriend =
    selectedChat?.type === "dm"
      ? friends.find((f) => f.id === selectedChat.id)
      : null;

  return (
    <div className="xp-desktop">
      <div className="xp-window app-window">
        <div className="xp-titlebar">
          <span>Tarot Club</span>
        </div>
        <div className="xp-window-body app-body">
          <aside className="xp-sidebar">
            <div className="xp-friends-block">
              <div className="xp-friends-header">
                <div className="xp-users-title">Friends</div>
                <div className="xp-friends-actions">
                  <button
                    className="xp-button xp-icon-button"
                    type="button"
                    onClick={() => setAddFriendOpen((prev) => !prev)}
                    title="Add friend"
                  >
                    <img src={addIcon} alt="Add friend" />
                  </button>
                  <button
                    className="xp-button xp-icon-button"
                    type="button"
                    onClick={() => setShowNotifications((prev) => !prev)}
                    title="Alerts"
                  >
                    <img src={bellIcon} alt="Alerts" />
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
                </div>
                {notifications.length === 0 && <div className="xp-muted">No alerts</div>}
                {notifications.map((n) => (
                  <div key={n.id} className="xp-notification">
                    <div className="xp-notification-text">{n.message}</div>
                    <div className="xp-notification-time">{n.created_at}</div>
                  </div>
                ))}
              </div>
            )}
            <div className="xp-users xp-scroll">
              {friends.map((u) => (
                <button
                  key={u.id}
                  className={`xp-user ${
                    selectedChat?.type === "dm" && selectedChat.id === u.id ? "active" : ""
                  }`}
                  onClick={() => selectDm(u.id)}
                >
                  <img src={u.avatar || defaultAvatar(u.username)} alt="" />
                  <div>
                    <div className="xp-user-name">@{u.username}</div>
                    <div className="xp-user-display">{u.display_name || u.username}</div>
                  </div>
                </button>
              ))}
            </div>
          </aside>
          <section className="xp-chat">
            <div className="xp-chat-header">
              {selectedFriend && (
                <div className="xp-chat-head-info">
                  <div className="xp-chat-name">@{selectedFriend.username}</div>
                  <div className="xp-chat-display">
                    {selectedFriend.display_name || selectedFriend.username}
                  </div>
                </div>
              )}
            </div>
            <div className="xp-chat-body">
              {selectedChat ? (
                messages.map((msg) => (
                  <div key={msg.id} className={`xp-message ${msg.sender_id === user.id ? "self" : ""}`}>
                    <div className="xp-message-avatar">
                      <img
                        src={
                          msg.sender_id === user.id
                            ? user.avatar || defaultAvatar(user.username)
                            : selectedFriend?.avatar || defaultAvatar(selectedFriend?.username)
                        }
                        alt=""
                      />
                    </div>
                    <div className="xp-message-content">
                      <div className="xp-message-bubble">{msg.body}</div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="xp-muted">Select a friend to start chatting</div>
              )}
            </div>
            {selectedChat && (
              <div className="xp-chat-input">
                <input
                  value={messageInput}
                  onChange={(e) => setMessageInput(e.target.value)}
                  placeholder="Type a message..."
                />
                <button className="xp-button" onClick={sendMessage}>
                  Send
                </button>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}