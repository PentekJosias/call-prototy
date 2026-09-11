const { v4: uuidv4 } = require('uuid');
const state = require('./state');
const { sendCallInvite, sendCallCancel } = require('./push');

const RING_TIMEOUT_MS = (Number(process.env.RING_TIMEOUT_SECONDS) || 45) * 1000;

function send(ws, payload) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function handleConnection(ws) {
  let userId = null;

  ws.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return send(ws, { type: 'ERROR', message: 'JSON invalide' });
    }

    switch (msg.type) {
      case 'REGISTER': {
        userId = String(msg.userId);
        state.setSocket(userId, ws);
        send(ws, { type: 'REGISTERED', userId });
        console.log(`[ws] ${userId} connecté`);
        break;
      }

      case 'CALL_INVITE': {
        if (!userId) return send(ws, { type: 'ERROR', message: 'Non enregistré' });

        const callId = msg.callId || uuidv4();
        const call = {
          callId,
          callerId: userId,
          calleeId: String(msg.targetUserId),
          offer: msg.offer,
          status: 'ringing',
          createdAt: Date.now(),
        };
        state.createCall(call);

        // Canal 1 : livraison immédiate si le destinataire a une connexion
        // WebSocket ouverte (app au premier plan ou en arrière-plan mais process vivant).
        const calleeSocket = state.getSocket(call.calleeId);
        if (calleeSocket) {
          send(calleeSocket, {
            type: 'INCOMING_CALL',
            callId,
            callerId: userId,
            callerName: msg.callerName || userId,
            offer: msg.offer,
          });
        }

        // Canal 2 : push FCM data-only, TOUJOURS envoyé en plus, pour réveiller
        // l'app si elle est tuée ou si la socket WS a expiré silencieusement.
        const tokenEntry = state.getPushToken(call.calleeId);
        if (tokenEntry?.fcmToken) {
          try {
            await sendCallInvite({
              fcmToken: tokenEntry.fcmToken,
              callId,
              callerId: userId,
              callerName: msg.callerName || userId,
              offer: msg.offer,
            });
          } catch (e) {
            console.error('[ws] Échec envoi push FCM CALL_INVITE', e.message);
          }
        } else {
          console.warn(`[ws] Aucun token FCM connu pour ${call.calleeId}`);
        }

        call.timeout = setTimeout(() => {
          const current = state.getCall(callId);
          if (current && current.status === 'ringing') {
            console.log(`[ws] Appel ${callId} manqué (timeout)`);
            current.status = 'missed';
            const caller = state.getSocket(current.callerId);
            send(caller, { type: 'CALL_TIMEOUT', callId });
            state.endCall(callId);
          }
        }, RING_TIMEOUT_MS);

        send(ws, { type: 'CALL_RINGING', callId });
        break;
      }

      case 'CALL_ANSWER': {
        const call = state.getCall(msg.callId);
        if (!call) return;
        call.status = 'active';
        const caller = state.getSocket(call.callerId);
        send(caller, { type: 'CALL_ANSWERED', callId: msg.callId, answer: msg.answer });
        break;
      }

      case 'ICE_CANDIDATE': {
        const call = state.getCall(msg.callId);
        if (!call) return;
        const targetId = userId === call.callerId ? call.calleeId : call.callerId;
        const target = state.getSocket(targetId);
        send(target, { type: 'ICE_CANDIDATE', callId: msg.callId, candidate: msg.candidate });
        break;
      }

      case 'CALL_REJECT': {
        const call = state.getCall(msg.callId);
        if (!call) return;
        const caller = state.getSocket(call.callerId);
        send(caller, { type: 'CALL_REJECTED', callId: msg.callId });
        state.endCall(msg.callId);
        break;
      }

      case 'CALL_CANCEL': {
        const call = state.getCall(msg.callId);
        if (!call) return;
        const calleeSocket = state.getSocket(call.calleeId);
        send(calleeSocket, { type: 'CALL_CANCELLED', callId: msg.callId });

        const tokenEntry = state.getPushToken(call.calleeId);
        if (tokenEntry?.fcmToken) {
          sendCallCancel({ fcmToken: tokenEntry.fcmToken, callId: msg.callId }).catch((e) =>
            console.error('[ws] Échec envoi push CALL_CANCEL', e.message)
          );
        }
        state.endCall(msg.callId);
        break;
      }

      case 'CALL_END': {
        const call = state.getCall(msg.callId);
        if (!call) return;
        const otherId = userId === call.callerId ? call.calleeId : call.callerId;
        const other = state.getSocket(otherId);
        send(other, { type: 'CALL_ENDED', callId: msg.callId });
        state.endCall(msg.callId);
        break;
      }

      default:
        send(ws, { type: 'ERROR', message: `Type de message inconnu: ${msg.type}` });
    }
  });

  ws.on('close', () => {
    if (userId) {
      state.removeSocket(userId);
      console.log(`[ws] ${userId} déconnecté`);
    }
  });
}

module.exports = { handleConnection };
