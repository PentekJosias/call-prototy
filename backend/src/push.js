const admin = require('firebase-admin');
const path = require('path');

let initialized = false;

function initFirebase() {
  if (initialized) return;

  const serviceAccountPath = path.resolve(
    process.env.FIREBASE_SERVICE_ACCOUNT_PATH || './firebase-service-account.json'
  );

  admin.initializeApp({
    credential: admin.credential.cert(require(serviceAccountPath)),
  });

  initialized = true;
  console.log('[push] Firebase Admin initialisé');
}

/**
 * Envoie un push "data-only" (aucune clé `notification`) : c'est OBLIGATOIRE
 * pour un appel entrant. Un message `notification` classique serait affiché
 * automatiquement par le système avec une UI générique et n'atteindrait pas
 * le code natif de l'app quand elle est en arrière-plan/tuée. En "data-only",
 * c'est toujours notre code (Notifee / CallKeep côté RN) qui décide de l'UI.
 */
async function sendCallInvite({ fcmToken, callId, callerId, callerName, offer }) {
  initFirebase();

  const message = {
    token: fcmToken,
    android: {
      priority: 'high',
    },
    apns: {
      headers: {
        'apns-priority': '10',
        'apns-push-type': 'voip', // nécessite un vrai VoIP push token côté iOS (react-native-voip-push-notification)
      },
    },
    data: {
      type: 'CALL_INVITE',
      callId,
      callerId: String(callerId),
      callerName: callerName || '',
      offer: JSON.stringify(offer),
    },
  };

  return admin.messaging().send(message);
}

async function sendCallCancel({ fcmToken, callId }) {
  initFirebase();
  return admin.messaging().send({
    token: fcmToken,
    android: { priority: 'high' },
    data: { type: 'CALL_CANCEL', callId },
  });
}

module.exports = { sendCallInvite, sendCallCancel, initFirebase };
