console.log('Iniciando index.js...');
require('dotenv').config();
const express = require('express');

const app = express();

// Endpoint de salud (para comprobar que el servidor vive)
app.get('/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// Arranque del servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API escuchando en http://localhost:${PORT}`);
});
