FROM alpine:latest
RUN apk update && apk --no-cache add ca-certificates
ADD dist/exif-extraction-engine /app/exif-extraction-engine

ADD manifest.json /var/manifest.json
ADD ./dist/engine /app/engine
ENV VERITONE_WEBHOOK_READY="http://0.0.0.0:8080/readyz"
ENV VERITONE_WEBHOOK_PROCESS="http://0.0.0.0:8080/process"

EXPOSE 8080

ENTRYPOINT [ "/app/engine", "/app/exif-extraction-engine" ]
