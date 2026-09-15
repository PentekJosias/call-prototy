/**
 * Backend de signalisation d'appels (WebRTC) + notifications push FCM
 * ---------------------------------------------------------------------------
 * - Express : simple endpoint HTTP de santé (utilisé par Render + par l'app
 *   mobile pour "réveiller" le serveur avant d'ouvrir le WebSocket).
 * - ws      : serveur WebSocket de signalisation (register / call / answer /
 *   ice-candidate / refus / fin d'appel...).
 * - firebase-admin : envoi des notifications push FCM (bannière d'appel /
 *   réveil d'écran) quand l'utilisateur cible n'a pas de socket actif.
 *
 * Déploiement Render : voir README.md
 */

const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const { initializeApp, cert } = require("firebase-admin/app");
const { getMessaging } = require("firebase-admin/messaging");

// -----------------------------------------------------------------------
// 1. Initialisation Firebase Admin
// -----------------------------------------------------------------------
// Deux méthodes possibles, dans cet ordre de priorité :
//   a) Variable d'environnement FIREBASE_SERVICE_ACCOUNT contenant le JSON
//      complet de la clé de compte de service (RECOMMANDÉ sur Render).
//   b) Fichier local serviceAccountKey.json à la racine du projet
//      (pratique en développement local, JAMAIS commité sur Git).
//
// Si aucune des deux n'est disponible, le serveur démarre quand même
// (la signalisation WebSocket fonctionne), mais l'envoi de push FCM est
// désactivé et un avertissement clair est affiché dans les logs.

let firebaseReady = false;

function loadServiceAccount() {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    try {
      return JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    } catch (e) {
      console.error(
        "❌ FIREBASE_SERVICE_ACCOUNT n'est pas un JSON valide :",
        e.message
      );
      return null;
    }
  }

  try {
    // Chargement local (dev uniquement, fichier non versionné)
    return require("./serviceAccountKey.json");
  } catch (e) {
    return null;
  }
}

const serviceAccount = loadServiceAccount();

if (serviceAccount) {
  initializeApp({ credential: cert(serviceAccount) });
  firebaseReady = true;
  console.log(
    `✅ Firebase Admin initialisé (project_id: ${serviceAccount.project_id})`
  );
} else {
  console.warn(
    "⚠️ Aucune clé de service Firebase trouvée. Définissez la variable " +
      "d'environnement FIREBASE_SERVICE_ACCOUNT (JSON complet) ou placez un " +
      "fichier serviceAccountKey.json à la racine. Les notifications push " +
      "FCM sont désactivées ; la signalisation WebSocket reste fonctionnelle."
  );
}

// -----------------------------------------------------------------------
// 2. Serveurs HTTP + WebSocket
// -----------------------------------------------------------------------
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Carte globale en mémoire (RAM) : userId -> { ws, pushToken }
// ⚠️ En mémoire uniquement : les données sont perdues à chaque redémarrage /
// redeploy du service Render, et ne sont pas partagées entre plusieurs
// instances si vous scalez horizontalement (plan payant avec >1 instance).
const users = new Map();

// Stockage temporaire des offres SDP en attente (évite de renvoyer l'offre
// complète via FCM, qui a une limite de taille de payload).
const pendingCalls = new Map();

app.get("/", (req, res) => {
  res.send("Serveur WebSocket actif");
});

// Endpoint de santé simple pour monitoring / Render health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    firebase: firebaseReady,
    connectedUsers: users.size,
    pendingCalls: pendingCalls.size,
    uptime: process.uptime(),
  });
});

// -----------------------------------------------------------------------
// 3. Heartbeat (garde les connexions WebSocket actives, purge les mortes)
// -----------------------------------------------------------------------
const HEARTBEAT_INTERVAL_MS = 30000;

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log(`⚠️ Client inactif expulsé : ${ws.userId || "Inconnu"}`);
      if (ws.userId && users.get(ws.userId)?.ws === ws) {
        const existingUser = users.get(ws.userId);
        users.set(ws.userId, {
          ws: null,
          pushToken: existingUser.pushToken,
        });
      }
      return ws.terminate();
    }

    ws.isAlive = false;

    try {
      ws.ping();
      ws.send(JSON.stringify({ type: "ping" }));
    } catch (e) {
      console.error("Erreur envoi ping :", e);
    }
  });
}, HEARTBEAT_INTERVAL_MS);

wss.on("close", () => {
  clearInterval(interval);
});

// -----------------------------------------------------------------------
// 4. Gestion des connexions WebSocket (signalisation WebRTC)
// -----------------------------------------------------------------------
wss.on("connection", (ws) => {
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", async (message) => {
    ws.isAlive = true;

    let data;
    try {
      data = JSON.parse(message);
    } catch (error) {
      console.error("❌ Erreur de parsing JSON :", error);
      return;
    }

    try {
      const {
        type,
        userId,
        pushToken,
        targetId,
        offer,
        answer,
        candidate,
        isVideo,
        callId,
      } = data;

      if (type === "pong") return;

      // 1. Enregistrement / reconnexion de l'utilisateur
      if (type === "register-user") {
        ws.userId = userId;

        if (users.has(userId)) {
          const existingUser = users.get(userId);
          console.log(
            `🔄 Utilisateur ${userId} déjà présent. Mise à jour de la connexion...`
          );

          if (existingUser.ws && existingUser.ws !== ws) {
            existingUser.ws.userId = null;
            existingUser.ws.terminate();
          }

          users.set(userId, {
            ws,
            pushToken: pushToken || existingUser.pushToken || null,
          });
        } else {
          console.log(`✨ Nouvel utilisateur enregistré : ${userId}`);
          users.set(userId, { ws, pushToken: pushToken || null });
        }

        const currentUser = users.get(userId);
        console.log(
          `👤 Statut : ${userId} | Token FCM : ${
            currentUser.pushToken || "Aucun"
          }`
        );
        console.log(
          "👥 Utilisateurs enregistrés :",
          Array.from(users.keys())
        );

        ws.send(JSON.stringify({ type: "registered", userId }));
        return;
      }

      // 2. Transmettre un appel (A -> B)
      if (type === "call-user") {
        const targetUser = users.get(targetId);
        const targetWs = targetUser?.ws;
        const callTypeLabel = isVideo ? "vidéo" : "audio";

        const newCallId = `call_${Date.now()}_${ws.userId}`;

        pendingCalls.set(newCallId, {
          from: ws.userId,
          targetId,
          offer,
          isVideo: !!isVideo,
        });

        setTimeout(() => pendingCalls.delete(newCallId), 45000);

        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({
              type: "incoming-call",
              callId: newCallId,
              from: ws.userId,
              offer,
              isVideo: !!isVideo,
            })
          );

          if (targetUser.pushToken) {
            await envoyerNotificationPush(
              targetUser.pushToken,
              "Appel Entrant",
              `Appel ${callTypeLabel} de ${ws.userId}`,
              ws.userId,
              newCallId,
              isVideo
            );
          }
        } else if (targetUser?.pushToken) {
          console.log(
            `📱 Utilisateur ${targetId} hors-ligne. Envoi du Push FCM...`
          );
          await envoyerNotificationPush(
            targetUser.pushToken,
            "Appel Entrant",
            `Appel ${callTypeLabel} de ${ws.userId}`,
            ws.userId,
            newCallId,
            isVideo
          );

          ws.send(JSON.stringify({ type: "user-offline", targetId }));
        } else {
          console.log(
            `⚠️ Impossible de joindre ${targetId} : hors-ligne et aucun token FCM.`
          );
          ws.send(JSON.stringify({ type: "user-offline", targetId }));
        }
        return;
      }

      // 3. Récupérer l'offre SDP complète (ouverture via notification FCM)
      if (type === "get-offer") {
        const callData = pendingCalls.get(callId);
        if (callData) {
          ws.send(
            JSON.stringify({
              type: "call-offer-details",
              callId,
              from: callData.from,
              offer: callData.offer,
              isVideo: callData.isVideo,
            })
          );
        } else {
          ws.send(JSON.stringify({ type: "call-expired", callId }));
        }
        return;
      }

      // 4. Transmettre la réponse (B -> A)
      if (type === "answer-call") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({ type: "call-answered", answer }));
        }
        return;
      }

      // 5. Échanger les candidats ICE
      if (type === "ice-candidate") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({ type: "ice-candidate", candidate }));
        }
        return;
      }

      // 6. Refus d'un appel
      if (type === "call-refused") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({ type: "call-refused", from: ws.userId })
          );
        }
        return;
      }

      // 7. Fin d'un appel
      if (type === "call-end") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({
              type: "call-end",
              from: ws.userId,
              target: targetId,
            })
          );
        }
        return;
      }

      // 8. Restart ICE
      if (type === "restart-offer") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({ type: "restart-offer", offer }));
        }
        return;
      }
    } catch (error) {
      console.error("❌ Erreur de traitement du message :", error);
    }
  });

  ws.on("close", () => {
    if (ws.userId) {
      const existingUser = users.get(ws.userId);
      if (existingUser?.ws === ws) {
        users.set(ws.userId, { ws: null, pushToken: existingUser.pushToken });
        console.log(
          `❌ Socket déconnecté pour ${ws.userId} (token FCM conservé)`
        );
      }
    }
  });

  ws.on("error", (error) => {
    console.error(
      `❌ Erreur WebSocket sur l'utilisateur ${ws.userId || "Inconnu"} :`,
      error
    );
  });
});

// -----------------------------------------------------------------------
// 5. Envoi de notification push FCM
// -----------------------------------------------------------------------
async function envoyerNotificationPush(
  tokenDestinataire,
  nomExpediteur,
  texteMessage,
  from,
  callId,
  isVideo = false
) {
  if (!firebaseReady) {
    console.warn(
      "⚠️ Firebase non initialisé : notification push ignorée (voir configuration FIREBASE_SERVICE_ACCOUNT)."
    );
    return;
  }

  if (!tokenDestinataire) {
    console.warn("⚠️ Impossible d'envoyer la notification : aucun token FCM fourni.");
    return;
  }

  const notId = Math.floor(100000 + Math.random() * 900000);

  const payload = {
    token: tokenDestinataire,
    data: {
      title: isVideo ? "📹 Appel vidéo entrant" : "📞 Appel entrant",
      message: texteMessage || `Appel de ${from}`,
      type: "incoming-call",
      callerId: String(from),
      callerName: String(nomExpediteur || from),
      callId: String(callId),
      isVideo: String(isVideo),
      notId: String(notId),
      actions: JSON.stringify([
        { title: "Refuser", callback: "reject", foreground: false },
        { title: "Accepter", callback: "accept", foreground: true },
      ]),
    },
    android: {
      priority: "high",
    },
  };

  try {
    const response = await getMessaging().send(payload);
    console.log("📲 Notification Push FCM envoyée avec succès, ID :", response);
  } catch (error) {
    console.error("❌ Erreur lors de l'envoi FCM :", error);
  }
}

// -----------------------------------------------------------------------
// 6. Démarrage du serveur
// -----------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
server.listen(PORT, () =>
  console.log(`🚀 Serveur WebSocket actif sur le port ${PORT}`)
);

// Arrêt propre (utile sur Render lors des redeploys)
process.on("SIGTERM", () => {
  console.log("🛑 SIGTERM reçu, fermeture propre du serveur...");
  clearInterval(interval);
  server.close(() => process.exit(0));
});
