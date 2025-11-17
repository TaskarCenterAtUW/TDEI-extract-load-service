FROM node:16.17-alpine

WORKDIR /usr
COPY package.json ./
COPY tsconfig.json ./
COPY src ./src
RUN ls -a
RUN npm install @azure/service-bus@7.9.4
RUN npm install @azure/storage-blob@12.18.0
RUN npm install
RUN npm run build

EXPOSE 8080

CMD [ "node", "./build/server.js" ]