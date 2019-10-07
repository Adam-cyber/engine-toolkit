FROM mhart/alpine-node:8
COPY . /app
COPY manifest.json /var/

COPY ./dist/engine /app/engine

WORKDIR /app
RUN npm install -i

RUN apk update \
        && apk upgrade \
        && apk --no-cache add ca-certificates

ENV VERITONE_WEBHOOK_READY="http://0.0.0.0:8080/readyz"
ENV VERITONE_WEBHOOK_PROCESS="http://0.0.0.0:8080/process"
ENTRYPOINT [ "/app/engine", "node", "index.js" ]
