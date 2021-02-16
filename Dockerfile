FROM node:10 as build
WORKDIR /app
COPY package*.json /app/
RUN npm install
 COPY ./ /app/
 RUN npm run build --production


FROM nginx:alpine


COPY --from=build /app/dist/saptabazar-dashboard /usr/share/nginx/html


CMD ["nginx", "-g", "daemon off;"]