console.log('Iniciando index.js...');
require('dotenv').config();

const express = require('express');
const { 
  connectMongo, 
  db, // instancia compartida desde db.js
  registerUser, 
  findUserByCredentials,
  getDuelMovies, 
  registerDuelResult, 
  addToWatchlist, 
  removeFromWatchlist, 
  getWatchlist 
} = require('./db');

const app = express();
app.use(express.json()); // parsea JSON en el body

const MONGO_URI = process.env.MONGO_URL || 'mongodb://mongo:27017';
const MONGO_DB  = process.env.MONGO_DB  || 'cine';

// =========================================================================
// MIDDLEWARE DE AUTENTICACIÓN SIMPLE
// =========================================================================

/**
 * Middleware para asegurar que la petición incluye el userId necesario.
 * (Simula protección sin JWT: requiere { userId: "..." } en body o params)
 */
function protectRouteSimple(req, res, next) {
  if (!db) {
    return res.status(503).json({ 
      error: "Servicio no disponible", 
      message: "La base de datos no está conectada." 
    });
  }
  const userId = req.body.userId || req.params.userId;
  if (!userId) {
    return res.status(401).json({ 
      error: "No autorizado", 
      message: "Se requiere un 'userId' para acceder a la Watchlist." 
    });
  }
  req.userId = userId;
  next();
}

// =========================================================================
// INICIALIZACIÓN DE BASE DE DATOS (SIN KAFKA)
// =========================================================================

async function setupDatabase() {
  try {
    await connectMongo(MONGO_URI, MONGO_DB);
    console.log(`[DB] Conectado a MongoDB: ${MONGO_DB}`);
  } catch (error) {
    console.error("[ERROR FATAL] No se pudo conectar a MongoDB:", error.message);
    process.exit(1);
  }
}

// =========================================================================
// ENDPOINTS DE LA API
// =========================================================================

// Salud
app.get('/health', async (req, res) => {
  try {
    // obtenemos una referencia válida siempre (reuse si ya está conectada)
    const database = await connectMongo(MONGO_URI, MONGO_DB);
    const pong = await database.command({ ping: 1 });

    res.json({
      status: 'ok',
      time: new Date().toISOString(),
      db_connected: pong && pong.ok === 1
    });
  } catch (err) {
    res.json({
      status: 'ok',
      time: new Date().toISOString(),
      db_connected: false,
      error: err.message
    });
  }
});


// (opcional) raíz
app.get('/', (req, res) => {
  res.json({ message: 'Cine API funcionando' });
});

// -------------------------------------------------------------------------
// AUTENTICACIÓN (/api/auth)
// -------------------------------------------------------------------------

// POST /api/auth/register
app.post('/api/auth/register', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ 
      error: 'Faltan campos', 
      message: 'Debe proporcionar un nombre de usuario y una contraseña.' 
    });
  }
  try {
    const user = await registerUser(username, password);
    if (!user) {
      return res.status(409).json({ 
        error: 'Usuario ya existe', 
        message: 'El nombre de usuario ya está registrado.' 
      });
    }
    res.status(201).json({ 
      message: 'Usuario registrado con éxito', 
      user: { userId: user._id, username: user.username } 
    });
  } catch (error) {
    console.error("Error en el registro:", error);
    res.status(500).json({ error: 'Error interno del servidor.' });
  }
});

// POST /api/auth/login
app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'Faltan credenciales' });
  }
  try {
    const user = await findUserByCredentials(username, password);
    if (!user) {
      return res.status(401).json({ error: 'Credenciales inválidas' });
    }
    res.json({ message: 'Inicio de sesión exitoso', user });
  } catch (error) {
    console.error("Error en el login:", error);
    res.status(500).json({ error: 'Error interno del servidor.' });
  }
});

// -------------------------------------------------------------------------
// CINEDUEL ENDPOINTS (Públicos)
// -------------------------------------------------------------------------

// GET /api/duel
app.get('/api/duel', async (req, res) => {
  try {
    const movies = await getDuelMovies();
    if (movies.length === 2) {
      res.json({ data: movies });
    } else {
      res.status(503).json({ 
        error: 'No se pudo obtener la cantidad necesaria de películas para el duelo.', 
        details: 'La base de datos puede estar vacía o el filtro es demasiado estricto.' 
      });
    }
  } catch (error) {
    console.error("Error en /api/duel:", error);
    res.status(500).json({ error: 'Error interno del servidor al obtener las películas.' });
  }
});

// POST /api/duel/win/:id
app.post('/api/duel/win/:id', async (req, res) => {
  const winnerId = parseInt(req.params.id, 10);
  if (isNaN(winnerId)) {
    return res.status(400).json({ error: 'ID de película inválido.' });
  }
  try {
    const success = await registerDuelResult(winnerId);
    if (success) {
      res.json({ message: `Victoria registrada para la película ID ${winnerId}.` });
    } else {
      res.status(404).json({ error: `No se encontró la película con ID ${winnerId} o no se pudo actualizar.` });
    }
  } catch (error) {
    console.error(`Error al registrar victoria para ID ${winnerId}:`, error);
    res.status(500).json({ error: 'Error interno del servidor al registrar el resultado del duelo.' });
  }
});

// -------------------------------------------------------------------------
// WATCHLIST ENDPOINTS (Protegidos)
// -------------------------------------------------------------------------

// POST /api/watchlist    (usamos POST para enviar userId en body)
app.post('/api/watchlist', protectRouteSimple, async (req, res) => {
  const userId = req.userId;
  try {
    const watchlist = await getWatchlist(userId);
    res.json({ data: watchlist });
  } catch (error) {
    console.error(`Error al obtener watchlist de ${userId}:`, error);
    res.status(500).json({ error: 'Error interno del servidor al obtener la Watchlist.' });
  }
});

// POST /api/watchlist/add/:movieId
app.post('/api/watchlist/add/:movieId', protectRouteSimple, async (req, res) => {
  const userId = req.userId;
  const movieIntId = parseInt(req.params.movieId, 10);
  if (isNaN(movieIntId)) {
    return res.status(400).json({ error: 'ID de película inválido.' });
  }
  try {
    const inserted = await addToWatchlist(userId, movieIntId);
    if (inserted) {
      res.status(201).json({ message: `Película ID ${movieIntId} añadida a la Watchlist.` });
    } else {
      res.status(409).json({ error: 'La película ya está en la Watchlist o el usuario no existe.' });
    }
  } catch (error) {
    console.error(`Error al añadir película ${movieIntId} a watchlist de ${userId}:`, error);
    res.status(500).json({ error: 'Error interno del servidor al añadir a la Watchlist.' });
  }
});

// POST /api/watchlist/remove/:movieId
app.post('/api/watchlist/remove/:movieId', protectRouteSimple, async (req, res) => {
  const userId = req.userId;
  const movieIntId = parseInt(req.params.movieId, 10);
  if (isNaN(movieIntId)) {
    return res.status(400).json({ error: 'ID de película inválido.' });
  }
  try {
    const deleted = await removeFromWatchlist(userId, movieIntId);
    if (deleted) {
      res.json({ message: `Película ID ${movieIntId} eliminada de la Watchlist.` });
    } else {
      res.status(404).json({ error: 'La película no se encontró en la Watchlist o el usuario no existe.' });
    }
  } catch (error) {
    console.error(`Error al eliminar película ${movieIntId} de watchlist de ${userId}:`, error);
    res.status(500).json({ error: 'Error interno del servidor al eliminar de la Watchlist.' });
  }
});

// =========================================================================
// ARRANQUE DEL SERVIDOR
// =========================================================================

const PORT = process.env.PORT || 3000;

setupDatabase()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`[Express] API escuchando en http://localhost:${PORT}`);
      console.log(`[Express] Endpoints de Autenticación: /api/auth/register | /api/auth/login`);
      console.log(`[Express] Endpoints de Duelo: /api/duel | /api/duel/win/:id`);
      console.log(`[Express] Endpoints de Watchlist (requiere userId en body): /api/watchlist`);
    });
  })
  .catch(err => {
    console.error("[FATAL] No se pudo iniciar la API:", err);
    process.exit(1);
  });