# Inicia el servidor Express en segundo plano (Server)
node index.js &
# Inicia el Consumidor de Kafka (Data Processor)
node consumer.js
# Este comando espera a que cualquiera de los procesos termine y evita que el contenedor se cierre
wait -n