FROM node:18-alpine

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm ci --omit=dev

COPY index.js ./

EXPOSE 3000

CMD [ "node", "index.js" ]
