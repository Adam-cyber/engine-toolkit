#! /bin/bash

tdo=$1
url=$2

for i in `seq 1 10`;
do
    curl -s \
        -X POST \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $VERITONE_API_TOKEN" \
        -d "{\"query\":\"mutation(\$targetId: ID!) {\n  createJob(input: {\n    targetId: \$targetId\n    tasks: [{\n    \tengineId: \\\"9e611ad7-2d3b-48f6-a51b-0a1ba40feab4\\\"\n    \tpayload: {\n        url: \\\"$url\\\"\n      }\n    }]\n  }) {\n    id\n  }\n}\n\",\"variables\":{\"targetId\":\"$tdo\"}}" \
        https://api.${ENV}.veritone.com/v3/graphql | jq -r '.data.createJob.id'
done