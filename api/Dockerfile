FROM node:20-slim
ENV NODE_ENV=development
#Mover el WORKDIR a la carpeta donde está index.js ***
WORKDIR /usr/src/app/src 
#Copia solo los archivos de dependencias. (A la raíz de la API)
COPY package*.json /usr/src/app/ 
#Ejecuta npm install en /usr/src/app/. La instalación debe hacerse donde está package.json (la raíz de la API)
WORKDIR /usr/src/app
RUN npm install
RUN apt-get update && apt-get install -y bash --no-install-recommends && rm -rf /var/lib/apt/lists/*
#Copiar todos los archivos de la API a la raíz; Copia start.sh, consumer.js, y la carpeta src/ a /usr/src/app/
COPY . . 
RUN chmod +x ./start.sh 
#Volver a establecer el WORKDIR a la carpeta de ejecución de código (src), esencial para que Node pueda ejecutar 'index.js' y 'consumer.js' con rutas relativas simples.
WORKDIR /usr/src/app/src
EXPOSE 3000
#Usamos '..' para encontrar start.sh; El WORKDIR es /usr/src/app/src, por lo que start.sh está en /usr/src/app (un nivel arriba).
CMD ["bash", "../start.sh"]