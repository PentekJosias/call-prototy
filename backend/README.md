# Backend – App Appel (signalisation WebRTC + push FCM)

Serveur Node.js (Express + `ws`) qui sert de serveur de **signalisation WebRTC** pour l'app Cordova, avec envoi de **notifications push FCM** (Firebase Cloud Messaging) quand le destinataire n'est pas connecté en WebSocket.

C'est exactement le code qui se trouvait dans `www/js/serveur.js` de l'app, réorganisé en projet Node autonome, déployable séparément de l'app mobile (l'app pointe déjà vers `wss://node-b44u.onrender.com`, un déploiement Render existant).

## Structure

```
backend/
├── server.js            # Serveur Express + WebSocket + FCM
├── package.json
├── serviceAccountKey.json   # Clé de service Firebase Admin (récupérée de l'app)
└── .gitignore
```

## Installation locale

```bash
npm install
node server.js
```

Le serveur écoute par défaut sur le port `3000` (variable d'environnement `PORT`).

## Déploiement (Render, Railway, Fly.io, VPS…)

1. Pousser ce dossier sur un dépôt Git (assurez-vous que `serviceAccountKey.json` n'est **pas** commité en clair sur un dépôt public — voir section sécurité ci-dessous).
2. Build command : `npm install`
3. Start command : `node server.js` (ou `npm start`)
4. Définir le port via la variable d'environnement `PORT` si la plateforme l'exige (Render/Railway le font automatiquement).
5. Une fois déployé, notez l'URL WebSocket (`wss://votre-domaine`) et mettez à jour `SIGNALING_SERVER_URL` dans `www/js/index.js` côté app si l'URL change.

## ⚠️ Sécurité — clé de service Firebase

Le fichier `serviceAccountKey.json` donne un accès administrateur complet à votre projet Firebase (`callapp-7efcd`). Recommandations :

- Ne jamais le pousser sur un dépôt public (il est déjà exclu via `.gitignore`).
- Sur la plateforme de déploiement, préférez charger son contenu via une **variable d'environnement** plutôt qu'un fichier commité, puis adapter `server.js` :
  ```js
  const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
    ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
    : require("./serviceAccountKey.json");
  ```
- Si cette clé a déjà été exposée (par ex. dans le zip original ou un dépôt public), il est prudent de la **révoquer et d'en régénérer une nouvelle** depuis la console Firebase (Paramètres du projet → Comptes de service).

## Protocole WebSocket (messages `type`)

| Type envoyé par le client | Description |
|---|---|
| `register-user` | Enregistre `userId` + `pushToken` (FCM) à la connexion |
| `call-user` | Démarre un appel vers `targetId` avec une offre SDP (`offer`, `isVideo`) |
| `answer-call` | Répond à un appel avec `answer` (SDP) |
| `ice-candidate` | Transmet un candidat ICE au correspondant |
| `call-refused` | Signale le refus d'un appel |
| `call-end` | Signale la fin d'un appel |
| `restart-offer` | Renégociation ICE (ex : upgrade audio → vidéo) |
| `get-offer` | Récupère une offre SDP en attente (après ouverture via notification push) |

| Type reçu par le client | Description |
|---|---|
| `registered` | Confirmation d'enregistrement |
| `incoming-call` | Appel entrant (offre SDP incluse si le destinataire est en ligne) |
| `call-answered` | Réponse SDP du correspondant |
| `ice-candidate` | Candidat ICE du correspondant |
| `call-refused` / `call-end` | Fin/refus d'appel |
| `user-offline` | Le destinataire est hors-ligne (avec ou sans push envoyé) |
| `call-offer-details` / `call-expired` | Résultat de `get-offer` |
| `ping` | Heartbeat toutes les 30s (le serveur ferme les connexions mortes) |

## Notes sur l'implémentation actuelle

- **Stockage en mémoire (RAM)** : les utilisateurs (`users`) et les appels en attente (`pendingCalls`) sont stockés dans des `Map()` en mémoire — tout est perdu au redémarrage du serveur. Pour une utilisation en production avec plusieurs utilisateurs simultanés/instances, un stockage externe (Redis) serait plus robuste.
- **Une seule instance** : comme l'état est en RAM, le serveur ne peut pas être scalé horizontalement (plusieurs instances) sans partager l'état (ex. Redis pub/sub) — un utilisateur connecté sur l'instance A ne serait pas joignable depuis l'instance B.
- Le nom de fichier attendu par le code est `serviceAccountKey.json` (avec un seul "s" à "service") — c'est corrigé ici par rapport au fichier original `servicesAccountKey.json` trouvé à la racine du zip.
