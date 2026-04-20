require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { Pool } = require("pg");
const cron = require("node-cron");
const { randomUUID } = require("crypto");
const { v2: cloudinary } = require("cloudinary");

const PORT = Number(process.env.PORT || 3000);
const FRONT_ORIGIN = process.env.FRONT_ORIGIN || "*";

/** localhost ↔ 127.0.0.1 포트만 같으면 같은 출처로 허용 (Live Server 주소 혼용 대응) */
function corsOrigin(origin, callback) {
  const raw = String(FRONT_ORIGIN || "").trim();
  if (!raw || raw === "*") {
    callback(null, true);
    return;
  }
  const list = raw.split(",").map((s) => s.trim()).filter(Boolean);
  if (!origin) {
    callback(null, true);
    return;
  }
  if (list.includes(origin)) {
    callback(null, true);
    return;
  }
  try {
    const u = new URL(origin);
    const port = u.port || (u.protocol === "https:" ? "443" : "80");
    for (const entry of list) {
      const e = new URL(entry);
      const ep = e.port || (e.protocol === "https:" ? "443" : "80");
      if (port !== ep) continue;
      if (u.hostname === e.hostname) {
        callback(null, true);
        return;
      }
      const a = u.hostname;
      const b = e.hostname;
      if (
        (a === "localhost" && b === "127.0.0.1") ||
        (a === "127.0.0.1" && b === "localhost")
      ) {
        callback(null, true);
        return;
      }
    }
  } catch {
    callback(null, false);
    return;
  }
  callback(null, false);
}

const app = express();
app.use(cors({ origin: corsOrigin, credentials: true }));
app.use(express.json({ limit: "1mb" }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: corsOrigin, methods: ["GET", "POST"], credentials: true },
});

const hasDb = Boolean(process.env.DATABASE_URL);
const pool = hasDb
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : false,
    })
  : null;

const hasCloudinary = Boolean(
  process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET
);

if (hasCloudinary) {
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET,
    secure: true,
  });
}

const state = {
  waitingQueue: [],
  socketUuid: new Map(),
  socketRoom: new Map(),
  rooms: new Map(),
  activeSocketsByUuid: new Map(),
  totalRoomsCreated: 0,
  memory: {
    uploads: [],
    messages: [],
    rooms: [],
    dauByDate: new Map(),
  },
};

function nowIso() {
  return new Date().toISOString();
}

function utcDateKey(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function addActiveSocket(uuid, socketId) {
  if (!state.activeSocketsByUuid.has(uuid)) {
    state.activeSocketsByUuid.set(uuid, new Set());
  }
  state.activeSocketsByUuid.get(uuid).add(socketId);
}

function removeActiveSocket(uuid, socketId) {
  const set = state.activeSocketsByUuid.get(uuid);
  if (!set) return;
  set.delete(socketId);
  if (set.size === 0) {
    state.activeSocketsByUuid.delete(uuid);
  }
}

function queueContainsUuid(uuid) {
  return state.waitingQueue.some((entry) => entry.uuid === uuid);
}

function removeFromQueueBySocketId(socketId) {
  state.waitingQueue = state.waitingQueue.filter((entry) => entry.socketId !== socketId);
}

function removeFromQueueByUuid(uuid) {
  state.waitingQueue = state.waitingQueue.filter((entry) => entry.uuid !== uuid);
}

async function recordDau(uuid) {
  const day = utcDateKey();
  if (!uuid) return;

  if (!hasDb) {
    if (!state.memory.dauByDate.has(day)) {
      state.memory.dauByDate.set(day, new Set());
    }
    state.memory.dauByDate.get(day).add(uuid);
    return;
  }

  await pool.query(
    `
      INSERT INTO daily_active_users (day, uuid, first_seen_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (day, uuid) DO NOTHING
    `,
    [day, uuid]
  );
}

async function saveRoom(room) {
  // MVP 정책: 채팅방은 서버 메모리에서만 관리하고 DB에 저장하지 않는다.
  void room;
  return;
}

async function markRoomEnded(roomId) {
  // MVP 정책: 채팅방 종료 정보도 영속 저장하지 않는다.
  void roomId;
  return;
}

async function saveMessage({ roomId, senderUuid, text, type = "text", mediaUrl = null, mediaPublicId = null }) {
  // MVP 정책: 메시지는 DB/메모리에 저장하지 않고 실시간 전달만 처리한다.
  void roomId;
  void senderUuid;
  void text;
  void type;
  void mediaUrl;
  void mediaPublicId;
  return;
}

async function saveUpload({ uuid, url, publicId, resourceType, bytes }) {
  if (!hasDb) {
    state.memory.uploads.push({
      id: randomUUID(),
      uuid,
      url,
      public_id: publicId,
      resource_type: resourceType,
      bytes: bytes || null,
      created_at: nowIso(),
    });
    return;
  }

  await pool.query(
    `
      INSERT INTO uploaded_media (uuid, url, public_id, resource_type, bytes, created_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
    `,
    [uuid, url, publicId, resourceType, bytes || null]
  );
}

function getPartnerSocket(room, socketId) {
  return room.users.find((user) => user.socketId !== socketId);
}

async function endChatForSocket(socket, reason = "상대방이 채팅을 종료했습니다.") {
  const roomId = state.socketRoom.get(socket.id);
  if (!roomId) return;

  const room = state.rooms.get(roomId);
  if (!room) return;

  const me = room.users.find((u) => u.socketId === socket.id);
  const partner = getPartnerSocket(room, socket.id);

  state.socketRoom.delete(socket.id);
  if (partner) {
    state.socketRoom.delete(partner.socketId);
  }
  state.rooms.delete(roomId);

  if (me?.uuid) removeFromQueueByUuid(me.uuid);
  if (partner?.uuid) removeFromQueueByUuid(partner.uuid);

  if (partner) {
    const partnerSocket = io.sockets.sockets.get(partner.socketId);
    if (partnerSocket) {
      partnerSocket.leave(roomId);
      partnerSocket.emit("partner_left", { roomId, reason });
      enqueueForMatch(partnerSocket, partner.uuid);
    }
  }

  socket.leave(roomId);
  socket.emit("chat_ended", { roomId, reason: "채팅이 종료되었습니다. 다시 매칭합니다." });

  if (me?.uuid) enqueueForMatch(socket, me.uuid);
  await markRoomEnded(roomId);
  tryMatchUsers();
}

function enqueueForMatch(socket, uuid) {
  if (!socket || !uuid) return;
  if (state.socketRoom.has(socket.id)) return;
  if (queueContainsUuid(uuid)) {
    socket.emit("queued", { queued: true, reason: "같은 UUID는 이미 대기열에 있습니다." });
    return;
  }

  removeFromQueueBySocketId(socket.id);
  state.waitingQueue.push({ socketId: socket.id, uuid, queuedAt: Date.now() });
  socket.emit("queued", { queued: true, size: state.waitingQueue.length });
}

async function createRoom(userA, userB) {
  const roomId = randomUUID();
  const room = {
    id: roomId,
    users: [userA, userB],
    createdAt: Date.now(),
  };

  state.rooms.set(roomId, room);
  state.socketRoom.set(userA.socketId, roomId);
  state.socketRoom.set(userB.socketId, roomId);
  state.totalRoomsCreated += 1;

  const socketA = io.sockets.sockets.get(userA.socketId);
  const socketB = io.sockets.sockets.get(userB.socketId);
  if (!socketA || !socketB) return;

  socketA.join(roomId);
  socketB.join(roomId);

  socketA.emit("matched", { roomId, partnerUuid: userB.uuid });
  socketB.emit("matched", { roomId, partnerUuid: userA.uuid });

  await saveRoom(room);
}

async function tryMatchUsers() {
  if (state.waitingQueue.length < 2) return;

  let matched = true;
  while (matched && state.waitingQueue.length > 1) {
    matched = false;

    for (let i = 0; i < state.waitingQueue.length; i += 1) {
      const a = state.waitingQueue[i];
      const socketA = io.sockets.sockets.get(a.socketId);
      if (!socketA) {
        state.waitingQueue.splice(i, 1);
        i -= 1;
        continue;
      }

      for (let j = i + 1; j < state.waitingQueue.length; j += 1) {
        const b = state.waitingQueue[j];
        if (a.uuid === b.uuid) continue;
        const socketB = io.sockets.sockets.get(b.socketId);
        if (!socketB) continue;
        if (state.socketRoom.has(a.socketId) || state.socketRoom.has(b.socketId)) continue;

        state.waitingQueue.splice(j, 1);
        state.waitingQueue.splice(i, 1);
        await createRoom(a, b);
        matched = true;
        break;
      }
      if (matched) break;
    }
  }
}

async function deleteCloudinaryAsset(publicId, resourceType) {
  if (!hasCloudinary || !publicId) return { skipped: true };
  try {
    const result = await cloudinary.uploader.destroy(publicId, {
      resource_type: resourceType || "image",
      invalidate: true,
    });
    return result;
  } catch (error) {
    console.error("Cloudinary delete failed:", publicId, error.message);
    return { error: error.message };
  }
}

async function cleanupExpiredMedia() {
  console.log(`[cleanup] started at ${nowIso()}`);

  const threshold = new Date(Date.now() - 24 * 60 * 60 * 1000);
  if (!hasDb) {
    const expired = state.memory.uploads.filter((u) => new Date(u.created_at) < threshold);
    for (const item of expired) {
      await deleteCloudinaryAsset(item.public_id, item.resource_type);
    }
    state.memory.uploads = state.memory.uploads.filter((u) => new Date(u.created_at) >= threshold);
    console.log(`[cleanup] memory uploads removed: ${expired.length}`);
    return;
  }

  const { rows } = await pool.query(
    `
      SELECT id, public_id, resource_type
      FROM uploaded_media
      WHERE created_at < NOW() - INTERVAL '24 hours'
    `
  );

  for (const row of rows) {
    await deleteCloudinaryAsset(row.public_id, row.resource_type);
  }

  const ids = rows.map((row) => row.id);
  if (ids.length > 0) {
    await pool.query(`DELETE FROM uploaded_media WHERE id = ANY($1::uuid[])`, [ids]);
  }
  console.log(`[cleanup] db uploads removed: ${ids.length}`);
}

app.get("/health", (req, res) => {
  res.json({ ok: true, time: nowIso(), hasDb, hasCloudinary });
});

app.get("/api/config", (req, res) => {
  res.json({
    cloudName: process.env.CLOUDINARY_CLOUD_NAME || "",
    uploadPreset: process.env.CLOUDINARY_UPLOAD_PRESET || "",
    maxImageBytes: 5 * 1024 * 1024,
    maxVideoBytes: 10 * 1024 * 1024,
  });
});

app.post("/api/uploads", async (req, res) => {
  try {
    const { uuid, url, publicId, resourceType, bytes } = req.body || {};
    if (!uuid || !url || !publicId) {
      return res.status(400).json({ error: "uuid, url, publicId는 필수입니다." });
    }

    const allowedResource = resourceType === "image" || resourceType === "video";
    if (!allowedResource) {
      return res.status(400).json({ error: "resourceType은 image 또는 video만 허용됩니다." });
    }

    await saveUpload({ uuid, url, publicId, resourceType, bytes });
    return res.json({ ok: true });
  } catch (error) {
    console.error("upload save error:", error);
    return res.status(500).json({ error: "업로드 메타데이터 저장 실패" });
  }
});

app.get("/api/stats", async (req, res) => {
  try {
    if (!hasDb) {
      const todaySet = state.memory.dauByDate.get(utcDateKey()) || new Set();
      return res.json({
        dau: todaySet.size,
        roomCount: state.totalRoomsCreated,
        waitingUsers: state.waitingQueue.length,
      });
    }

    const dauResult = await pool.query(
      `SELECT COUNT(*)::int AS count FROM daily_active_users WHERE day = CURRENT_DATE`
    );
    return res.json({
      dau: dauResult.rows[0].count,
      roomCount: state.totalRoomsCreated,
      waitingUsers: state.waitingQueue.length,
    });
  } catch (error) {
    console.error("stats error:", error);
    return res.status(500).json({ error: "통계 조회 실패" });
  }
});

io.on("connection", (socket) => {
  socket.on("register", async ({ uuid }) => {
    try {
      if (!uuid || typeof uuid !== "string") {
        socket.emit("error_message", { message: "유효한 UUID가 필요합니다." });
        return;
      }

      state.socketUuid.set(socket.id, uuid);
      addActiveSocket(uuid, socket.id);
      await recordDau(uuid);
      enqueueForMatch(socket, uuid);
      await tryMatchUsers();
    } catch (error) {
      console.error("register error:", error);
      socket.emit("error_message", { message: "등록 중 오류가 발생했습니다." });
    }
  });

  socket.on("find_new_partner", async () => {
    const uuid = state.socketUuid.get(socket.id);
    if (!uuid) return;
    if (state.socketRoom.has(socket.id)) {
      await endChatForSocket(socket, "상대방이 새 매칭을 요청했습니다.");
    } else {
      enqueueForMatch(socket, uuid);
    }
    await tryMatchUsers();
  });

  socket.on("chat_message", async (payload) => {
    try {
      const roomId = state.socketRoom.get(socket.id);
      const senderUuid = state.socketUuid.get(socket.id);
      if (!roomId || !senderUuid) {
        socket.emit("error_message", { message: "채팅방이 없습니다." });
        return;
      }

      const room = state.rooms.get(roomId);
      if (!room) {
        socket.emit("error_message", { message: "유효하지 않은 채팅방입니다." });
        return;
      }

      const { text = "", type = "text", mediaUrl = null, mediaPublicId = null } = payload || {};
      const trimmed = typeof text === "string" ? text.trim() : "";
      if (type === "text" && !trimmed) return;

      const message = {
        roomId,
        senderUuid,
        type,
        text: trimmed,
        mediaUrl,
        mediaPublicId,
        createdAt: nowIso(),
      };

      // 이벤트명 `message`는 엔진 패킷과 혼동될 수 있어 `room_message` 사용
      io.to(roomId).emit("room_message", message);
      await saveMessage(message);
    } catch (error) {
      console.error("chat_message error:", error);
      socket.emit("error_message", { message: "메시지 전송 중 오류가 발생했습니다." });
    }
  });

  socket.on("leave_chat", async () => {
    await endChatForSocket(socket);
  });

  socket.on("disconnect", async () => {
    const uuid = state.socketUuid.get(socket.id);
    removeFromQueueBySocketId(socket.id);

    if (state.socketRoom.has(socket.id)) {
      await endChatForSocket(socket, "상대방 연결이 종료되었습니다.");
    }

    if (uuid) {
      removeActiveSocket(uuid, socket.id);
    }
    state.socketUuid.delete(socket.id);
  });
});

cron.schedule("0 */3 * * *", async () => {
  try {
    await cleanupExpiredMedia();
  } catch (error) {
    console.error("[cleanup] unexpected error:", error);
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ranchat backend listening on http://localhost:${PORT}`);
});
