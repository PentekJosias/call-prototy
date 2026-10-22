"use strict";

const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const { CallManager, CallError } = require("./calls");

/* =========================================================
   FIREBASE (notifications push)
   - SERVICE_ACCOUNT_PATH : chemin ABSOLU du fichier de clé
     (sur Render : /etc/secrets/serviceAccountKey.json via "Secret Files")
   - DISABLE_FCM=1 : désactive FCM (tests locaux)
========================================================= */
let getMessaging = null;
if (process.env.DISABLE_FCM !== "1") {
  const { initializeApp, cert } = require("firebase-admin/app");
  ({ getMessaging } = require("firebase-admin/messaging"));
  const serviceAccount = require(
    process.env.SERVICE_ACCOUNT_PATH || "./serviceAccountKey.json",
  );
  initializeApp({ credential: cert(serviceAccount) });
} else {
  console.log("ℹ️ FCM désactivé (DISABLE_FCM=1)");
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// userId -> { ws, pushToken } (conservé en RAM même quand le socket est fermé)
const users = new Map();

/* =========================================================
   PONT ENTRE LES RÈGLES D'APPEL ET LE RÉSEAU
========================================================= */
const io = {
  isOnline: (id) => users.get(id)?.ws?.readyState === WebSocket.OPEN,
  hasPush: (id) => !!users.get(id)?.pushToken,
  send: (id, msg) => {
    const ws = users.get(id)?.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
      return true;
    }
    return false;
  },
  push: (id, kind, { call, from }) => {
    const token = users.get(id)?.pushToken;
    if (!token) return;
    if (kind === "incoming") {
      envoyerNotificationPush(token, {
        from,
        callId: call.id,
        isVideo: call.isVideo,
        isGroup: manager.isGroup(call),
        notId: call.notId,
      });
    } else if (kind === "missed") {
      envoyerNotificationAppelManque(token, {
        from,
        callId: call.id,
        isVideo: call.isVideo,
        notId: call.notId,
      });
    }
  },
};

const manager = new CallManager(io);

function reply(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

app.get("/", (req, res) => {
  res.send("Serveur WebSocket actif");
});

/* =========================================================
   HEARTBEAT
========================================================= */
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log(`⚠️ Client inactif expulsé : ${ws.userId || "Inconnu"}`);
      return ws.terminate(); // déclenche "close", qui fait le nettoyage
    }
    ws.isAlive = false;
    try {
      ws.ping();
      ws.send(JSON.stringify({ type: "ping" }));
    } catch (e) {
      console.error("Erreur envoi ping :", e);
    }
  });
}, 30000);
interval.unref();

wss.on("close", () => clearInterval(interval));

/* =========================================================
   MESSAGES WEBSOCKET
========================================================= */
wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    ws.isAlive = true;

    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      console.error("❌ Message JSON invalide");
      return;
    }
    if (!data || typeof data.type !== "string") return;
    const { type } = data;

    if (type === "pong") return;

    try {
      /* ---------- Enregistrement ---------- */
      if (type === "register-user") {
        const { userId, pushToken } = data;
        if (typeof userId !== "string" || !userId || userId.length > 64) {
          throw new CallError("invalid-user-id");
        }
        // NB : aucune authentification ici (n'importe qui peut prendre un userId).
        // À brancher sur le système d'identité du système hôte.
        ws.userId = userId;
        const existing = users.get(userId);
        if (existing?.ws && existing.ws !== ws) {
          existing.ws.userId = null; // évite de traiter sa fermeture comme une déconnexion
          existing.ws.terminate();
        }
        users.set(userId, {
          ws,
          pushToken: (typeof pushToken === "string" && pushToken) || existing?.pushToken || null,
        });
        manager.onUserReconnected(userId);
        console.log(`👤 ${userId} enregistré | FCM : ${users.get(userId).pushToken ? "oui" : "non"}`);

        reply(ws, {
          type: "registered",
          userId,
          activeCallId: manager.activeCallOf(userId), // appel à reprendre (reconnexion)
          ringing: manager.ringingFor(userId), // appels qui sonnent encore pour lui
        });
        return;
      }

      if (!ws.userId) throw new CallError("not-registered");
      const me = ws.userId;

      switch (type) {
        /* ---------- Création / invitation ---------- */
        case "create-call": {
          const r = manager.createCall(me, data.targetIds, {
            isVideo: data.isVideo,
            notId: data.notId,
          });
          for (const u of r.unreachable) {
            reply(ws, {
              type: u.reason === "busy" ? "user-busy" : "user-offline",
              targetId: u.userId,
            });
          }
          if (r.callId) {
            reply(ws, { type: "call-created", ...r.snapshot, invited: r.invited, unreachable: r.unreachable });
          } else {
            reply(ws, { type: "call-failed", reason: "unreachable", unreachable: r.unreachable });
          }
          return;
        }

        case "invite-to-call": {
          const r = manager.invite(me, data.callId, data.targetIds);
          reply(ws, { type: "invite-result", callId: data.callId, ...r });
          return;
        }

        /* ---------- Rejoindre / refuser / quitter ---------- */
        case "join-call": {
          const snap = manager.join(me, data.callId, { fresh: !!data.fresh });
          reply(ws, { type: "call-joined", ...snap });
          return;
        }

        case "decline-call":
          manager.decline(me, data.callId);
          return;

        case "leave-call":
          manager.leave(me, data.callId);
          reply(ws, { type: "call-left", callId: data.callId });
          return;

        /* ---------- Infos ---------- */
        case "get-call": {
          // Appli ouverte depuis la notification FCM : retrouver l'appel
          const info = manager.getCallInfo(me, data.callId);
          if (info) reply(ws, { type: "call-info", ...info });
          else reply(ws, { type: "call-expired", callId: data.callId });
          return;
        }

        case "get-ice-servers":
          reply(ws, { type: "ice-servers", iceServers: manager.buildIceServers(me) });
          return;

        /* ---------- Vidéo / état média ---------- */
        case "upgrade-to-video":
          manager.upgradeToVideo(me, data.callId);
          return;

        case "media-state":
          manager.setMediaState(me, data.callId, { audio: data.audio, video: data.video });
          return;

        /* ---------- Relais de signalisation entre deux participants ---------- */
        case "offer": // offer initiale (du dernier arrivé vers un participant plus ancien)
        case "answer":
        case "ice-candidate":
        case "restart-offer": // renégociation (ICE restart, passage en vidéo) : dans les deux sens
          manager.relay(me, type, data);
          return;

        default:
          throw new CallError("unknown-type", `Type inconnu : ${type}`);
      }
    } catch (error) {
      if (error instanceof CallError) {
        reply(ws, {
          type: "call-error",
          code: error.code,
          message: error.message,
          requestType: type,
          callId: data.callId,
        });
      } else {
        console.error(`❌ Erreur sur "${type}" :`, error);
      }
    }
  });

  ws.on("close", () => {
    const id = ws.userId;
    if (!id) return;
    const u = users.get(id);
    if (u?.ws === ws) {
      // Le token FCM est conservé ; l'appel en cours a un délai de grâce avant de quitter
      users.set(id, { ws: null, pushToken: u.pushToken });
      manager.onUserDisconnected(id);
      console.log(`❌ Socket déconnecté pour ${id}`);
    }
  });

  ws.on("error", (error) => {
    console.error(`❌ Erreur WebSocket (${ws.userId || "Inconnu"}) :`, error);
  });
});

/* =========================================================
   NOTIFICATIONS PUSH (FCM) — payloads légers, pas de SDP
========================================================= */
async function envoyerNotificationPush(token, { from, callId, isVideo, isGroup, notId }) {
  if (!getMessaging) return;
  const kind = isVideo ? "vidéo" : "audio";
  const payload = {
    token,
    data: {
      title: isVideo ? "📹 Appel vidéo entrant" : "📞 Appel entrant",
      message: isGroup ? `Appel de groupe ${kind} de ${from}` : `Appel ${kind} de ${from}`,
      type: "incoming-call",
      callerId: String(from),
      callerName: String(from),
      callId: String(callId),
      isVideo: String(!!isVideo),
      isGroup: String(!!isGroup),
      notId: String(notId), // même notId que l'appel manqué : la notification est remplacée
      actions: JSON.stringify([
        { title: "Refuser", callback: "reject", foreground: false },
        { title: "Accepter", callback: "accept", foreground: true },
      ]),
    },
    android: { priority: "high" },
  };
  try {
    const response = await getMessaging().send(payload);
    console.log("📲 Notification FCM envoyée, ID :", response);
  } catch (error) {
    console.error("❌ Erreur lors de l'envoi FCM :", error);
  }
}

async function envoyerNotificationAppelManque(token, { from, callId, isVideo, notId }) {
  if (!getMessaging) return;
  const payload = {
    token,
    data: {
      type: "missed-call",
      title: "Appel manqué",
      message: `Vous avez manqué un appel ${isVideo ? "vidéo" : "audio"} de ${from}`,
      callerId: String(from),
      callId: String(callId),
      notId: String(notId),
    },
    android: { priority: "high" },
  };
  try {
    await getMessaging().send(payload);
    console.log(`📵 Notification appel manqué envoyée (notId : ${notId})`);
  } catch (error) {
    console.error("❌ Erreur lors de l'envoi de l'appel manqué :", error);
  }
}

/* =========================================================
   DÉMARRAGE
========================================================= */
const PORT = process.env.PORT || 3000;
if (require.main === module) {
  server.listen(PORT, () => console.log(`🚀 Serveur WebSocket actif sur le port ${PORT}`));
}

module.exports = { server, wss, manager };
