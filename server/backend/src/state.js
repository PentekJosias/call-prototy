/**
 * État en mémoire du serveur de signalisation.
 * Pour une vraie prod multi-instance, remplacer par Redis (pub/sub + hash).
 * Pour un serveur unique (cas KamSoft actuel), la mémoire suffit largement.
 */

// userId -> WebSocket
const sockets = new Map();

// userId -> { fcmToken, platform, updatedAt }
const pushTokens = new Map();

// callId -> { callId, callerId, calleeId, offer, status, createdAt, timeout }
const activeCalls = new Map();

function setSocket(userId, ws) {
  sockets.set(userId, ws);
}

function removeSocket(userId) {
  sockets.delete(userId);
}

function getSocket(userId) {
  return sockets.get(userId);
}

function isOnline(userId) {
  const ws = sockets.get(userId);
  return !!ws && ws.readyState === ws.OPEN;
}

function setPushToken(userId, fcmToken, platform) {
  pushTokens.set(userId, { fcmToken, platform, updatedAt: Date.now() });
}

function getPushToken(userId) {
  return pushTokens.get(userId);
}

function createCall(call) {
  activeCalls.set(call.callId, call);
}

function getCall(callId) {
  return activeCalls.get(callId);
}

function updateCall(callId, patch) {
  const call = activeCalls.get(callId);
  if (!call) return null;
  Object.assign(call, patch);
  return call;
}

function endCall(callId) {
  const call = activeCalls.get(callId);
  if (call?.timeout) clearTimeout(call.timeout);
  activeCalls.delete(callId);
}

module.exports = {
  setSocket,
  removeSocket,
  getSocket,
  isOnline,
  setPushToken,
  getPushToken,
  createCall,
  getCall,
  updateCall,
  endCall,
};
