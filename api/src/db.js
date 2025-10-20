const { MongoClient } = require('mongodb');

let client;
let db;

async function connectMongo(uri, dbName) {
  if (db) return db; // si ya hay conexión, la reusa
  client = new MongoClient(uri, { maxPoolSize: 10 });
  await client.connect();
  db = client.db(dbName);

  // Asegurar que las colecciones tienen índices necesarios
  await db.collection('peliculas').createIndex({ id: 1 }, { unique: true });
  await db.collection('users').createIndex({ username: 1 }, { unique: true });

  return db;
}

async function closeMongo() {
  if (client) {
    await client.close();
    client = null;
    db = null;
  }
}

// =========================================================================
// LÓGICA DE AUTENTICACIÓN SIMPLE
// NOTA: En un entorno de producción, las contraseñas deberían hashearse con bcrypt.
// =========================================================================

/**
 * Registra un nuevo usuario en la colección 'users'.
 * @param {string} username - Nombre de usuario (único).
 * @param {string} password - Contraseña (texto plano).
 * @returns {object|null} El documento del usuario insertado (o null si el usuario ya existe).
 */
async function registerUser(username, password) {
    if (!db) return null;
    const usersCollection = db.collection('users');
    
    // Comprobar si el usuario ya existe
    const existingUser = await usersCollection.findOne({ username });
    if (existingUser) {
        return null; // Usuario ya registrado
    }

    const newUser = {
        username,
        password, // Guardado en texto plano (SOLO PARA ESTE PROYECTO DE PRUEBA)
        watchlist: [],
        created_at: new Date()
    };

    const result = await usersCollection.insertOne(newUser);
    return { 
        _id: result.insertedId, 
        username: newUser.username 
    };
}

/**
 * Busca un usuario por sus credenciales.
 * @param {string} username - Nombre de usuario.
 * @param {string} password - Contraseña.
 * @returns {object|null} El documento del usuario (o null si no coincide).
 */
async function findUserByCredentials(username, password) {
    if (!db) return null;
    const usersCollection = db.collection('users');

    // Busca un usuario que coincida exactamente con el usuario Y la contraseña (inseguro, pero simple)
    const user = await usersCollection.findOne({ 
        username, 
        password 
    });

    if (user) {
        // Devolver solo los datos relevantes (sin la contraseña)
        return { 
            userId: user._id.toHexString(), // Convertir ObjectId a string para usar en la API
            username: user.username 
        };
    }
    return null;
}

// =========================================================================
// LÓGICA (Duel y Watchlist)
// =========================================================================

// --- Cineduel ---

/**
 * Obtiene dos películas aleatorias para un duelo.
 */
async function getDuelMovies() {
    if (!db) return [];
    const peliculas = db.collection('peliculas');

    // Uso de la pipeline de agregación para obtener 2 documentos aleatorios
    // $sample es eficiente para selecciones aleatorias en MongoDB.
    const movies = await peliculas.aggregate([
        { $match: { poster_path: { $ne: null } } }, // Asegura que tenga un póster
        { $sample: { size: 2 } }, 
        { $project: { _id: 0, password: 0 } } // Excluye campos internos
    ]).toArray();

    return movies;
}

/**
 * Registra una victoria para la película.
 */
async function registerDuelResult(winnerId) {
    if (!db) return false;
    const peliculas = db.collection('peliculas');

    const result = await peliculas.updateOne(
        { id: winnerId },
        { $inc: { wins: 1 } } // Incrementa el campo 'wins' en 1
    );

    return result.matchedCount === 1;
}

// --- Watchlist ---

/**
 * Obtiene la lista de IDs de películas en la Watchlist de un usuario.
 */
async function getWatchlist(userId) {
    if (!db) return [];
    const usersCollection = db.collection('users');
    const peliculasCollection = db.collection('peliculas');

    // 1. Encontrar el documento del usuario
    const user = await usersCollection.findOne(
        { _id: new MongoClient.ObjectId(userId) }, 
        { projection: { watchlist: 1, _id: 0 } }
    );
    
    if (!user || user.watchlist.length === 0) {
        return [];
    }

    // 2. Usar los IDs de la watchlist para buscar los detalles de las películas
    const movieIds = user.watchlist;
    
    const watchlistDetails = await peliculasCollection.find({ 
        id: { $in: movieIds } 
    }).project({ _id: 0 }).toArray();

    return watchlistDetails;
}

/**
 * Añade una película a la Watchlist.
 */
async function addToWatchlist(userId, movieId) {
    if (!db) return false;
    const usersCollection = db.collection('users');

    const result = await usersCollection.updateOne(
        { _id: new MongoClient.ObjectId(userId), watchlist: { $ne: movieId } }, // Condición: no añadir si ya existe
        { $push: { watchlist: movieId } }
    );

    return result.modifiedCount === 1; // Devuelve true si la película fue añadida
}

/**
 * Elimina una película de la Watchlist.
 */
async function removeFromWatchlist(userId, movieId) {
    if (!db) return false;
    const usersCollection = db.collection('users');

    const result = await usersCollection.updateOne(
        { _id: new MongoClient.ObjectId(userId) },
        { $pull: { watchlist: movieId } } // $pull elimina el elemento del array
    );

    return result.modifiedCount === 1;
}

module.exports = { 
    connectMongo, 
    closeMongo,
    // Autenticación
    registerUser,
    findUserByCredentials,
    // Datos de Negocio
    getDuelMovies,
    registerDuelResult,
    getWatchlist,
    addToWatchlist,
    removeFromWatchlist,
    db // Exponemos la instancia DB
};