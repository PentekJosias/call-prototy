require('dotenv').config();

const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');

const routes = require('./routes');
const { handleConnection } = require('./wsHandlers');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/', routes);

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', handleConnection);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`KamSoft backend démarré sur le port ${PORT}`);
  console.log(`  REST  : http://localhost:${PORT}/health`);
  console.log(`  WS    : ws://localhost:${PORT}/ws`);
});
