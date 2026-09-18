const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

// Imports Firebase Admin v10+ / v11+
const { initializeApp, cert } = require("firebase-admin/app");
const { getMessaging } = require("firebase-admin/messaging");

// ⚠️ AJOUT : sur Render (et toute plateforme sans fichier de clé commité),
// la clé de service Firebase est fournie via la variable d'environnement
// FIREBASE_SERVICE_ACCOUNT (contenant le JSON complet en une seule ligne).
// En local, on retombe sur le fichier serviceAccountKey.json s'il existe.
const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  : require("./serviceAccountKey.json");

// Initialisation de Firebase
initializeApp({
  credential: cert(serviceAccount),
});

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Carte globale persistante (RAM) pour conserver les utilisateurs et leurs tokens FCM
const users = new Map();

// Stockage temporaire en mémoire RAM pour les offres d'appel (évite de surcharger FCM)
const pendingCalls = new Map();

app.get("/", (req, res) => {
  res.send("Serveur WebSocket actif");
});

// =========================================================
// HEARTBEAT (Garde les connexions actives)
// =========================================================
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log(`⚠️ Client inactif expulsé : ${ws.userId || "Inconnu"}`);
      if (ws.userId && users.get(ws.userId)?.ws === ws) {
        // Déconnexion : Passe le socket à null sans supprimer l'utilisateur de la Map
        const existingUser = users.get(ws.userId);
        users.set(ws.userId, { 
          ws: null, 
          pushToken: existingUser.pushToken 
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
}, 30000);

wss.on("close", () => {
  clearInterval(interval);
});

// =========================================================
// GESTION DES CONNEXIONS WEBSOCKET
// =========================================================
wss.on("connection", (ws) => {
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", async (message) => {
    ws.isAlive = true;

    try {
      const data = JSON.parse(message);
      // Récupération de la propriété isVideo transmise par le client
      const { type, userId, pushToken, targetId, offer, answer, candidate, isVideo, callId } = data;

      if (type === "pong") {
        return;
      }

      // 1. Enregistrement / Reconnexion de l'utilisateur
      if (type === "register-user") {
        ws.userId = userId;

        // VÉRIFICATION DE LA PRÉSENCE DANS LA MAP
        if (users.has(userId)) {
          const existingUser = users.get(userId);
          console.log(`🔄 Utilisateur ${userId} déjà présent dans la Map. Mise à jour de la connexion...`);

          // Fermer l'ancien socket s'il existe et qu'il est encore actif
          if (existingUser.ws && existingUser.ws !== ws) {
            existingUser.ws.userId = null;
            existingUser.ws.terminate();
          }

          // Mise à jour : Nouveau socket + mise à jour du token
          users.set(userId, {
            ws: ws,
            pushToken: pushToken || existingUser.pushToken || null
          });
        } else {
          // Nouvel utilisateur
          console.log(`✨ Nouvel utilisateur enregistré dans la Map : ${userId}`);
          users.set(userId, {
            ws: ws,
            pushToken: pushToken || null
          });
        }

        const currentUser = users.get(userId);
        console.log(`👤 Statut : ${userId} | Token FCM : ${currentUser.pushToken || "Aucun"}`);
        console.log("👥 Liste globale des utilisateurs enregistrés :", Array.from(users.keys()));

        ws.send(
          JSON.stringify({
            type: "registered",
            userId: userId,
          })
        );
        return;
      }

      // 2. Transmettre un appel (A -> B)
      if (type === "call-user") {
        const targetUser = users.get(targetId);
        const targetWs = targetUser?.ws;
        const callTypeLabel = isVideo ? "vidéo" : "audio";

        // Génération d'un ID unique pour cet appel
        const newCallId = `call_${Date.now()}_${ws.userId}`;

        // AJOUT : notId généré une seule fois ici, pour pouvoir annuler la MÊME
        // notification plus tard (call-refused / call-end) via envoyerAnnulationPush.
        const notId = Math.floor(100000 + Math.random() * 900000);

        // Sauvegarde de l'offre SDP sur le serveur pour téléchargement ultérieur si besoin
        pendingCalls.set(newCallId, {
          from: ws.userId,
          targetId: targetId,
          offer: offer,
          isVideo: !!isVideo,
          notId: notId,                              // AJOUT
          pushToken: targetUser?.pushToken || null    // AJOUT
        });

        // Supprimer l'offre après 45 secondes si non récupérée
        setTimeout(() => pendingCalls.delete(newCallId), 45000);

        // CAS 1 : L'utilisateur est connecté en WebSocket
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          // Envoie l'offre SDP directement via WebSocket (sans restriction de taille)
          targetWs.send(
            JSON.stringify({
              type: "incoming-call",
              callId: newCallId,
              from: ws.userId,
              offer: offer,
              isVideo: !!isVideo
            })
          );

          // Notification FCM légère en parallèle
          if (targetUser.pushToken) {
            await envoyerNotificationPush(
              targetUser.pushToken,
              "Appel Entrant",
              `Appel ${callTypeLabel} de ${ws.userId}`,
              ws.userId,
              newCallId,
              isVideo,
              notId // AJOUT
            );
          }
        }
        // CAS 2 : L'utilisateur est déconnecté mais possède un token FCM
        else if (targetUser?.pushToken) {
          console.log(`📱 Utilisateur ${targetId} hors-ligne. Envoi du Push FCM...`);
          await envoyerNotificationPush(
            targetUser.pushToken,
            "Appel Entrant",
            `Appel ${callTypeLabel} de ${ws.userId}`,
            ws.userId,
            newCallId,
            isVideo,
            notId // AJOUT
          );

          ws.send(
            JSON.stringify({
              type: "user-offline",
              targetId: targetId,
            })
          );
        }
        // CAS 3 : Destinataire introuvable
        else {
          console.log(`⚠️ Impossible de joindre ${targetId} : Hors-ligne et aucun token FCM en mémoire.`);
          ws.send(
            JSON.stringify({
              type: "user-offline",
              targetId: targetId,
            })
          );
        }
        return;
      }

      // 3. Récupérer l'offre SDP complète si l'application est ouverte via la notification FCM
      if (type === "get-offer") {
        const callData = pendingCalls.get(callId);
        if (callData) {
          ws.send(
            JSON.stringify({
              type: "call-offer-details",
              callId: callId,
              from: callData.from,
              offer: callData.offer,
              isVideo: callData.isVideo
            })
          );
        } else {
          ws.send(
            JSON.stringify({
              type: "call-expired",
              callId: callId
            })
          );
        }
        return;
      }

      // 4. Transmettre la réponse (B -> A)
      if (type === "answer-call") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({
              type: "call-answered",
              answer: answer,
            })
          );
        }
        return;
      }

      // 5. Échanger les candidats ICE
      if (type === "ice-candidate") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({
              type: "ice-candidate",
              candidate: candidate,
            })
          );
        }
        return;
      }

      // 6. Refus d'un appel
      if (type === "call-refused") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({
              type: "call-refused",
              from: ws.userId,
            })
          );
        }
        // ⚠️ AJOUT : refus explicite du récepteur → type dédié CALL_DECLINED,
        // SANS notification "Appel manqué" (le récepteur sait déjà qu'il a refusé).
        // Ne pas confondre avec MISSED_CALL (voir call-end), qui lui doit afficher
        // une trace "Appel manqué" côté récepteur.
        const callData = callId ? pendingCalls.get(callId) : null;
        if (callData) {
          await envoyerAnnulationPush(callData.pushToken, callData.notId, "CALL_DECLINED");
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
        // ⚠️ AJOUT : vrai appel manqué (l'appelant raccroche avant réponse) →
        // type MISSED_CALL (valeur par défaut d'envoyerAnnulationPush), qui
        // affichera une trace "Appel manqué" côté récepteur (voir CallMessagingService).
        const callData = callId ? pendingCalls.get(callId) : null;
        if (callData) {
          await envoyerAnnulationPush(callData.pushToken, callData.notId);
        }
        return;
      }

      // 8. Restart ICE
      if (type === "restart-offer") {
        const targetWs = users.get(targetId)?.ws;
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(
            JSON.stringify({
              type: "restart-offer",
              offer: offer,
            })
          );
        }
        return;
      }
    } catch (error) {
      console.error("❌ Erreur de lecture du message :", error);
    }
  });

  // Nettoyage à la déconnexion
  ws.on("close", () => {
    if (ws.userId) {
      const existingUser = users.get(ws.userId);
      if (existingUser?.ws === ws) {
        users.set(ws.userId, { 
          ws: null, 
          pushToken: existingUser.pushToken 
        });
        console.log(`❌ Socket déconnecté pour ${ws.userId} (Utilisateur et Token FCM conservés)`);
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

/**
 * Fonction d'envoi de notification Push FCM optimisée (Taille < 500 octets)
 */
async function envoyerNotificationPush(tokenDestinataire, nomExpediteur, texteMessage, from, callId, isVideo = false, notId) {
  if (!tokenDestinataire) {
    console.warn("⚠️ Impossible d'envoyer la notification : Aucun token FCM fourni.");
    return;
  }

  // AJOUT : notId est maintenant fourni par l'appelant (voir call-user) au lieu
  // d'être généré ici, pour pouvoir annuler la MÊME notification plus tard
  // via envoyerAnnulationPush (call-refused / call-end).
  if (!notId) {
    notId = Math.floor(100000 + Math.random() * 900000);
  }

  const payload = {
    token: tokenDestinataire,
    data: {
      title: isVideo ? "📹 Appel vidéo entrant" : "📞 Appel entrant",
      message: texteMessage || `Appel de ${from}`,
      type: "incoming-call",
      callerId: String(from),
      callerName: String(nomExpediteur || from),
      callId: String(callId), // Transmet uniquement l'identifiant léger de l'appel
      isVideo: String(isVideo),
      notId: String(notId),
      actions: JSON.stringify([
        {
          title: "Refuser",
          callback: "reject",
          foreground: false
        },
        {
          title: "Accepter",
          callback: "accept",
          foreground: true
        }
      ])
    },
    android: {
      priority: "high",
    }
  };

  try {
    const response = await getMessaging().send(payload);
    console.log("📲 Notification Push FCM envoyée avec succès, ID :", response);
  } catch (error) {
    console.error("❌ Erreur lors de l'envoi FCM :", error);
  }
}

/**
 * Envoie un push FCM léger pour faire annuler la notification d'appel entrant
 * côté client. Deux types possibles, traités différemment par
 * CallMessagingService.java :
 *   - "MISSED_CALL"   : vrai appel manqué (l'appelant a raccroché avant
 *                       réponse). Annule la notif ET affiche une trace
 *                       "Appel manqué" consultable par le récepteur.
 *   - "CALL_DECLINED" : refus explicite du récepteur (bouton "Refuser").
 *                       Annule la notif SEULEMENT, sans trace "Appel manqué"
 *                       — le récepteur sait déjà qu'il vient de refuser.
 */
async function envoyerAnnulationPush(tokenDestinataire, notId, type = "MISSED_CALL") {
  if (!tokenDestinataire || !notId) return;

  try {
    await getMessaging().send({
      token: tokenDestinataire,
      data: {
        type: type,
        notId: String(notId),
      },
      android: {
        priority: "high",
      },
    });
    console.log(`📴 Push d'annulation (${type}) envoyé, notId =`, notId);
  } catch (error) {
    console.error("❌ Erreur lors de l'envoi du push d'annulation :", error);
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () =>
  console.log(`🚀 Serveur WebSocket actif sur le port ${PORT}`)
);
