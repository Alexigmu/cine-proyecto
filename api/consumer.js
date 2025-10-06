require('dotenv').config();
const { Kafka } = require('kafkajs');
const { connectMongo } = require('./db'); // Módulo de conexión que ya tienes

// 1. CONFIGURACIÓN
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const MONGO_URI = process.env.MONGO_URL;
const MONGO_DB_NAME = process.env.MONGO_DB;

const TOPIC_NAME = 'peliculas_raw';
const GROUP_ID = 'cine-data-processor'; // ID para que Kafka sepa qué mensajes has leído

// 2. CONEXIÓN A KAFKA
const kafka = new Kafka({
  clientId: 'api-consumer',
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: GROUP_ID });


// 3. LÓGICA DE PROCESAMIENTO
async function runConsumer() {
  console.log(`[CONSUMER] Conectando al broker Kafka: ${KAFKA_BROKER}`);
  await consumer.connect();

  console.log(`[CONSUMER] Conectando a MongoDB: ${MONGO_DB_NAME}`);
  const db = await connectMongo(MONGO_URI, MONGO_DB_NAME);
  const collection = db.collection('peliculas');

  // Asegurar el índice único (lo que quitamos del Python)
  await collection.createIndex({ id: 1 }, { unique: true });
  console.log('[CONSUMER] Índice de MongoDB asegurado.');


  // 4. SUSCRIPCIÓN Y PROCESAMIENTO DE MENSAJES
  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = JSON.parse(message.value.toString());
        const movieId = value.id;
        
        // El código de MongoDB que antes estaba en Python, ahora está aquí
        const result = await collection.replaceOne(
          { id: movieId },
          value,
          { upsert: true } // Upsert: Inserta si no existe, reemplaza si sí
        );

        let action = result.upsertedCount > 0 ? 'INSERTADO' : (result.modifiedCount > 0 ? 'ACTUALIZADO' : 'SIN CAMBIOS');
        console.log(`[MONGODB] Mensaje ID ${movieId} en partición ${partition}: ${action}`);

      } catch (error) {
        console.error(`[CONSUMER ERROR] Fallo al procesar mensaje: ${error.message}`);
      }
    },
  });
}

runConsumer().catch(async (e) => {
  console.error(`[FATAL ERROR] El consumidor ha fallado: ${e.message}`);
  await consumer.disconnect();
  // El proceso saldrá y Docker Compose (si tiene restart: always) intentará reiniciarlo
  process.exit(1); 
});