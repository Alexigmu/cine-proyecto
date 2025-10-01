require('dotenv').config();
const { connectMongo, closeMongo } = require('./db');

(async () => {
  try {
    const uri = process.env.MONGO_URL || 'mongodb://localhost:27017';
    const name = process.env.MONGO_DB || 'cine';
    const db = await connectMongo(uri, name);
    const cols = await db.listCollections().toArray();
    console.log('Colecciones:', cols.map(c => c.name));
  } catch (e) {
    console.error('Error de conexión:', e.message);
  } finally {
    await closeMongo();
  }
})();
