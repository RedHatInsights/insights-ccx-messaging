#!/bin/bash -e

if [ -z "$NAMESPACE" ]
then
  echo "\$NAMESPACE is empty"
  exit 1
fi


FILE_PATH="deploy/test.tar.gz"
CLUSTER_ID="95d99ff8-c2cc-4dc0-9d38-04c8ba8ffafb"

io_hash=$(git ls-remote https://github.com/openshift/insights-operator.git HEAD |cut -f1)
BASIC_AUTH=$(oc get secret env-$NAMESPACE-keycloak -n $NAMESPACE -o json | jq '.data | map_values(@base64d)' | jq -r -j '"\(.defaultUsername):\(.defaultPassword)" | @base64')
HOSTNAME=$(oc get route -l app=ingress -o json | jq -r '.items[0].spec.host')

curl -F "file=@$PWD/${FILE_PATH};type=application/vnd.redhat.openshift.periodic+tar" \
    -H "Authorization: Basic $BASIC_AUTH" \
    -H "User-Agent: insights-operator/${io_hash} cluster/${CLUSTER_ID}" \
    -H "x-rh-request_id: testtesttest" https://$HOSTNAME/api/ingress/v1/upload --insecure | jq
