# 1. IMAGEN BASE
FROM node:20-slim
# 2. CONFIGURACIÓN
# Establece el modo de entorno. Esto lo lee Node.js (Express)
ENV NODE_ENV=development
# Establece el directorio de trabajo dentro del contenedor
WORKDIR /usr/src/app
# 3. OPTIMIZACIÓN DE LA CACHÉ
# Copia solo los archivos de dependencias primero. 
# Si package.json no cambia, la capa de instalación se cachea (build rápido).
COPY package*.json ./ 
# 4. INSTALACIÓN DE DEPENDENCIAS
# Instala las dependencias de Node.js (incluyendo 'kafkajs')
RUN npm install
# 5. INSTALACIÓN DE HERRAMIENTAS ADICIONALES
# Instalamos 'bash' para poder ejecutar el script de arranque (start.sh) 
# que lanzará los dos procesos (Servidor y Consumidor)
RUN apt-get update && apt-get install -y bash --no-install-recommends && rm -rf /var/lib/apt/lists/*
# 6. CÓDIGO DE APLICACIÓN
# Copia todo el código de la API (index.js, db.js, consumer.js, etc.)
COPY . .
# 7. SCRIPT DE ARRANQUE
# Copia el script que lanzará los dos procesos en paralelo
COPY start.sh /usr/src/app/
# 8. PUERTO (DOCUMENTACIÓN)
# Informa a Docker de qué puerto usará la aplicación
EXPOSE 3000
# 9. COMANDO DE EJECUCIÓN
# Ejecuta el script de arranque con bash (que lanzará index.js y consumer.js)
CMD ["bash", "start.sh"]