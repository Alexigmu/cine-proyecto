console.log('Iniciando index.js...');
require('dotenv').config();

const express = require('express');
const { Kafka } = require('kafkajs');
const { 
    connectMongo, 
    db, // Importamos la instancia de la DB directamente
    registerUser, 
    findUserByCredentials,
    getDuelMovies, 
    registerDuelResult, 
    addToWatchlist, 
    removeFromWatchlist, 
    getWatchlist 
} = require('./db');

const app = express();
app.use(express.json()); // Middleware para parsear JSON en el body de las peticiones

const MONGO_URI = process.env.MONGO_URL || 'mongodb://mongo:27017';
const MONGO_DB = process.env.MONGO_DB || 'cine';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';
const KAFKA_TOPIC = 'peliculas_raw';

let kafka;

// =========================================================================
// MIDDLEWARE DE AUTENTICACIÓN SIMPLE
// =========================================================================

/**
 * Middleware para asegurar que la petición incluye el userId necesario.
 * Esto simula la protección de rutas sin JWT.
 * REQUIERE: El body de la petición debe contener { userId: "..." }
 */
function protectRouteSimple(req, res, next) {
    // Si no está conectado a la DB, no se puede autenticar
    if (!db) {
        return res.status(503).json({ error: "Servicio no disponible", message: "La base de datos no está conectada." });
    }
    
    // Verificación simple: ¿El body de la petición tiene un userId?
    const userId = req.body.userId || req.params.userId;

    if (!userId) {
        // Código 401: No autorizado (falta de credenciales)
        return res.status(401).json({ error: "No autorizado", message: "Se requiere un 'userId' para acceder a la Watchlist." });
    }
    
    // Si pasa la verificación, lo añadimos al objeto de la petición
    req.userId = userId;
    next();
}


// =========================================================================
// CONEXIÓN Y CONSUMIDOR DE KAFKA
// =========================================================================

async function setupDatabaseAndConsumer() {
    try {
        // Conexión a MongoDB
        await connectMongo(MONGO_URI, MONGO_DB); // La instancia 'db' se expone globalmente desde 'db.js'
        console.log(`[CONSUMER] Conectado a MongoDB: ${MONGO_DB}`);

        // ----------------------------------------------------
        // CONSUMIDOR KAFKA
        // ----------------------------------------------------
        kafka = new Kafka({ 
            clientId: 'cine-api',
            brokers: [KAFKA_BROKER],
        });
        console.log(`[CONSUMER] Conectando al broker Kafka: ${KAFKA_BROKER}`);

        const consumer = kafka.consumer({ groupId: 'cine-data-processor' });
        await consumer.connect();
        await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });

        // Función para procesar los mensajes (ingesta)
        const processMessage = async ({ topic, partition, message }) => {
            const data = JSON.parse(message.value.toString());
            const peliculas = db.collection('peliculas');
            
            // Upsert: si existe, actualiza. Si no, inserta.
            const filter = { id: data.id }; 
            const updateResult = await peliculas.updateOne(
                filter,
                { $set: data }, 
                { upsert: true }
            );
            
            const action = updateResult.upsertedCount > 0 ? 'INSERTADO' : (updateResult.modifiedCount > 0 ? 'ACTUALIZADO' : 'SIN CAMBIOS');
            console.log(`[MONGODB] Mensaje ID ${data.id} en partición ${partition}: ${action}`);
        };

        await consumer.run({
            eachMessage: processMessage,
        });

        console.log(`[CONSUMER] Escuchando en el topic: ${KAFKA_TOPIC}`);

    } catch (error) {
        console.error("[ERROR FATAL] Fallo en la configuración inicial o Kafka/MongoDB:", error.message);
        process.exit(1); 
    }
}

// =========================================================================
// ENDPOINTS DE LA API
// =========================================================================

// Endpoint de salud (para comprobar que el servidor vive)
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        time: new Date().toISOString(),
        db_connected: !!db,
        kafka_configured: !!kafka
    });
});

// -------------------------------------------------------------------------
// AUTENTICACIÓN (/api/auth)
// -------------------------------------------------------------------------

/**
 * POST /api/auth/register
 * Crea un nuevo usuario. 
 * Body: { username: '...', password: '...' }
 */
app.post('/api/auth/register', async (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.status(400).json({ error: 'Faltan campos', message: 'Debe proporcionar un nombre de usuario y una contraseña.' });
    }

    try {
        const user = await registerUser(username, password);

        if (!user) {
            return res.status(409).json({ error: 'Usuario ya existe', message: 'El nombre de usuario ya está registrado.' });
        }

        // Devolvemos el userId para que el cliente lo use en las peticiones de Watchlist
        res.status(201).json({ message: 'Usuario registrado con éxito', user: { userId: user._id, username: user.username } });

    } catch (error) {
        console.error("Error en el registro:", error);
        res.status(500).json({ error: 'Error interno del servidor.' });
    }
});

/**
 * POST /api/auth/login
 * Inicia sesión.
 * Body: { username: '...', password: '...' }
 */
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

        // Devolvemos el userId para que el cliente lo use en las peticiones de Watchlist
        res.json({ message: 'Inicio de sesión exitoso', user }); 

    } catch (error) {
        console.error("Error en el login:", error);
        res.status(500).json({ error: 'Error interno del servidor.' });
    }
});

// -------------------------------------------------------------------------
// CINEDUEL ENDPOINTS (Públicos)
// -------------------------------------------------------------------------

/**
 * GET /api/duel
 * Devuelve un array con 2 películas aleatorias para el duelo.
 */
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

/**
 * POST /api/duel/win/:id
 * Registra una victoria para la película con ese ID.
 */
app.post('/api/duel/win/:id', async (req, res) => {
    const winnerId = parseInt(req.params.id);
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

// Aplicamos el middleware de protección simple a todas las rutas de /api/watchlist
// Nota: Usaremos el userId del body para hacer estas peticiones.

/**
 * GET /api/watchlist
 * Obtiene la lista completa de películas en la Watchlist del usuario.
 * Body: { userId: '...' }
 */
app.post('/api/watchlist', protectRouteSimple, async (req, res) => {
    // Usamos POST para pasar el userId en el body por seguridad, aunque la acción sea GET
    const userId = req.userId;
    try {
        const watchlist = await getWatchlist(userId);
        res.json({ data: watchlist });
    } catch (error) {
        console.error(`Error al obtener watchlist de ${userId}:`, error);
        res.status(500).json({ error: 'Error interno del servidor al obtener la Watchlist.' });
    }
});

/**
 * POST /api/watchlist/add/:movieId
 * Añade una película a la Watchlist.
 * Body: { userId: '...' }
 */
app.post('/api/watchlist/add/:movieId', protectRouteSimple, async (req, res) => {
    const userId = req.userId;
    const movieIntId = parseInt(req.params.movieId);

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

/**
 * POST /api/watchlist/remove/:movieId
 * Elimina una película de la Watchlist.
 * Body: { userId: '...' }
 */
app.post('/api/watchlist/remove/:movieId', protectRouteSimple, async (req, res) => {
    // Usamos POST para pasar el userId en el body por seguridad, aunque la acción sea DELETE
    const userId = req.userId;
    const movieIntId = parseInt(req.params.movieId);

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

// Ejecución de la configuración de DB y Kafka antes de iniciar el servidor HTTP
setupDatabaseAndConsumer().then(() => {
    app.listen(PORT, () => {
        console.log(`[Express] API escuchando en http://localhost:${PORT}`);
        console.log(`[Express] Endpoints de Autenticación: /api/auth/register | /api/auth/login`);
        console.log(`[Express] Endpoints de Duelo: /api/duel | /api/duel/win/:id`);
        console.log(`[Express] Endpoints de Watchlist (Requiere userId en body): /api/watchlist`);
    });
}).catch(err => {
    console.error("[FATAL] No se pudo iniciar la API:", err);
    process.exit(1);
});