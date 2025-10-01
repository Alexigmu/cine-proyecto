const { MongoClient } = require('mongodb');

let client;
let db;

async function connectMongo(uri, dbName) {
  if (db) return db; // si ya hay conexión, la reusa
  client = new MongoClient(uri, { maxPoolSize: 10 });
  await client.connect();
  db = client.db(dbName);
  return db;
}

async function closeMongo() {
  if (client) {
    await client.close();
    client = null;
    db = null;
  }
}

module.exports = { connectMongo, closeMongo };
