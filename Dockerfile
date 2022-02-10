FROM node:12.19

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get update && apt-get -y upgrade && apt-get -y install gettext-base postgresql-client && apt-get clean

# == Custom Below =============================================================

ENV DEFINITION_DIRECTORY "example_definition"
ENV LISTEN_PORT "8803"
ENV PGUSER "postgres"
ENV PGPASSWORD "postgres"
ENV PGDATABASE "postgres"
ENV PGHOST "127.0.0.1"

WORKDIR /esqlate-server
COPY package.json tsconfig.json tslint.json ./

RUN npm install

COPY test ./test
COPY src ./src
COPY tsconfig.json tslint.json 
COPY example_definition ./example_definition

ADD https://raw.githubusercontent.com/forbesmyester/wait-for-pg/master/wait-for-pg ./wait-for-pg

CMD echo "Compiling..." && ./node_modules/.bin/tsc && echo "Waiting for PostgreSQL..." && chmod +x ./wait-for-pg && ./wait-for-pg && echo "Starting up..." && node dist/server.js
