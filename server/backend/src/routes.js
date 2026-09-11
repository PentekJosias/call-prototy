const express = require('express');
const state = require('./state');

const router = express.Router();

router.get('/health', (req, res) => {
  res.json({ ok: true, time: Date.now() });
});

/**
 * Le client RN appelle ceci à chaque démarrage / refresh de token FCM.
 * On garde le token indépendamment de la connexion WebSocket, car c'est ce
 * token qui permet de réveiller l'app quand elle est tuée.
 */
router.post('/register-token', (req, res) => {
  const { userId, fcmToken, platform } = req.body || {};
  if (!userId || !fcmToken) {
    return res.status(400).json({ ok: false, error: 'userId et fcmToken requis' });
  }
  state.setPushToken(userId, fcmToken, platform || 'android');
  console.log(`[routes] Token FCM enregistré pour ${userId}`);
  res.json({ ok: true });
});

module.exports = router;
