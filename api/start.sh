cd /usr/src/app/src
# Inicia el servidor Express en segundo plano (Server)
node index.js &
# Inicia el Consumidor de Kafka (Data Processor)
node consumer.js
# Mantiene el contenedor vivo
wait -n