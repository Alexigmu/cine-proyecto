FROM node:22-alpine
ENV NODE_ENV=development
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm ci
COPY . .
EXPOSE 3000
CMD ["sh", "./start.sh"]
