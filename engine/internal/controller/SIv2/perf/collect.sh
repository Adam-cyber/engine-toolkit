#!/bin/bash

while read line
do
  curl -s \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $VERITONE_API_TOKEN" \
    -d "{\"query\":\"query (\$jobId: ID!) {\n  job(id: \$jobId) {\n tasks {\n  records {\n  id\n  output\n  }\n }\n  }\n}\n\",\"variables\":{\"jobId\":\"$line\"}}" \
    https://api.${ENV}.veritone.com/v3/graphql | jq -r '.data.job.tasks.records[].output.info'
done
