# Backend – Signalisation d'appels (WebSocket + FCM)

Backend Node.js pour l'application Cordova d'appels (VOIP/WebRTC). Gère :
- la signalisation WebRTC (offer/answer/ICE) via WebSocket,
- l'enregistrement des utilisateurs et de leur token FCM,
- l'envoi de notifications push (bannière d'appel / réveil d'écran) via Firebase Cloud Messaging quand l'utilisateur cible n'est pas connecté en WebSocket.

## 1. Fichiers du projet

```
backend/
├── server.js          # serveur Express + WebSocket + FCM
├── package.json
├── .gitignore
├── .env.example
└── README.md
```

Le fichier `serviceAccountKey.json` (clé Firebase) n'est **pas** inclus dans le dépôt Git (voir `.gitignore`). En local, placez-le à la racine de `backend/` pour tester. En production sur Render, utilisez la variable d'environnement `FIREBASE_SERVICE_ACCOUNT` (voir ci-dessous) — c'est la méthode recommandée et la plus sûre.

## 2. Lancer en local

```bash
cd backend
npm install
# Placez votre serviceAccountKey.json ici si vous voulez tester les push FCM
npm start
```

Le serveur écoute par défaut sur `http://localhost:3000`. Le WebSocket est accessible sur `ws://localhost:3000`.

## 3. Déployer sur Render

### a) Créer le service
1. Poussez le contenu de ce dossier `backend/` dans un dépôt Git (GitHub/GitLab).
   ⚠️ Ne poussez jamais `serviceAccountKey.json` — vérifiez que `.gitignore` est bien pris en compte.
2. Sur [render.com](https://render.com) : **New +** → **Web Service** → connectez votre dépôt.
3. Paramètres du service :
   - **Runtime** : Node
   - **Build Command** : `npm install`
   - **Start Command** : `npm start`
   - **Plan** : Free (suffisant pour tester ; voir limites ci-dessous)

### b) Configurer la clé Firebase (obligatoire pour les push FCM)
Dans l'onglet **Environment** du service Render, ajoutez une variable :

- **Key** : `FIREBASE_SERVICE_ACCOUNT`
- **Value** : le contenu JSON complet de votre clé de compte de service, sur une seule ligne (voir le fichier fourni séparément `FIREBASE_SERVICE_ACCOUNT_pour_render.txt`).

Collez tout le JSON tel quel dans le champ de valeur — Render gère les valeurs longues sans problème.

### c) Déployer
Cliquez sur **Create Web Service**. Render installe les dépendances et démarre `npm start`. Une fois déployé, vous obtenez une URL du type :

```
https://votre-service.onrender.com
```

### d) Mettre à jour le client Cordova
Dans `www/js/index.js` et `www/js/veille.js`, mettez à jour :

```js
const SIGNALING_SERVER_URL = "wss://votre-service.onrender.com";
const HTTP_SERVER_URL = "https://votre-service.onrender.com";
```

(Le projet pointait déjà vers `wss://node-b44u.onrender.com` — remplacez par votre propre URL Render si vous déployez une nouvelle instance.)

## 4. Vérifier que ça fonctionne

- `GET /` → doit répondre `Serveur WebSocket actif`.
- `GET /health` → renvoie un JSON avec le statut, si Firebase est bien initialisé, le nombre d'utilisateurs connectés, etc.
- Les logs Render affichent `✅ Firebase Admin initialisé (project_id: ...)` si la clé est correctement configurée.

## 5. Limites importantes à connaître

- **Plan gratuit Render** : le service se met en veille après une période d'inactivité et le premier appel entrant après une veille peut être lent à réveiller le serveur (d'où le `fetch(HTTP_SERVER_URL)` fait par le client avant d'ouvrir le WebSocket, déjà présent dans `index.js`/`veille.js`). Pour un usage en production avec appels temps réel fiables, un plan payant (pas de mise en veille) est recommandé.
- **Stockage en mémoire (RAM)** : la liste des utilisateurs connectés et les appels en attente sont stockés uniquement en mémoire (`Map`). Ils sont perdus à chaque redémarrage/redeploy, et ne sont pas partagés si vous scalez à plusieurs instances. Pour une vraie mise en production multi-instances, il faudrait externaliser cet état (ex. Redis).
- **Sécurité** : ce serveur n'authentifie pas les utilisateurs (n'importe qui connaissant un `userId` peut techniquement s'enregistrer sous ce nom). Si l'app est destinée à un usage public, ajoutez une authentification (ex. token JWT vérifié à la connexion WebSocket).
