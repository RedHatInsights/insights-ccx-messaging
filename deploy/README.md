# Deployments

This folder contains all the Clowdapps using the ccx-messaging library.

## How tun run it in ephemeral

1st, expert the following env var because the default GraphQL used by `bonfire` is not available.

```
export QONTRACT_BASE_URL=https://app-interface.apps.rosa.appsrep09ue1.03r5.p3.openshiftapps.com/graphql
```

Run

```
bonfire deploy \
    -c deploy/test.yaml \
    -n $NAMESPACE ccx-data-pipeline \
    -C archive-sync \
    --set-parameter archive-sync/IMAGE_TAG=pr-189-latest
```

Replace the `pr-189-latest` tag with yours. If you want to make changes, you
may need to push them to that branch, wait for the pr-check to build the image
and upload it to quay and then rerun this command. It can happen that OCP
doesn't pull the latest image, so you may want to use `pr-189-$IMAGE_SHA` in
order to force the pulling.

This command should also deploy ingress, Kafka and Minio, core dependencies for
this service.

### 1. Send an archive

Upload an archive to ingress using `./deploy/upload-ephemeral.sh`. You can
generate one using [Molodec](https://gitlab.cee.redhat.com/ccx/molodec). Or
you can also send an empty archive, but there might be some issues.

### 2. Check the archive is sent from ingress to Kafka

You can check the Kafka messages by running a `kcat` pod:
```shell
oc create -f deploy/kcat_pod.yaml
KAFKA_URL="$(oc get service -o name -l strimzi.io/kind=Kafka | grep kafka-bootstrap | sed 's/service\///').${NAMESPACE}.svc:9092"
echo $KAFKA_URL
oc rsh kcat
# This will log you into the new pod, where you run commands like
    $ kcat -L -b $KAFKA_URL_HERE!!
```

### 3. Check the archive is received by archive-sync

Run

```shell
oc logs -l app=archive-sync
```

and check the logs for any error.


## Troubleshooting

### Checking the loaded configuration

If you want to check the configuration, you can open a terminal in the pod
```shell
oc rsh $(oc get pods -l app=archive-sync -o name)
```
and run `python`:
```python
import yaml

Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
f = open("/data/config.yaml")
config = yaml.load(f.read(), Loader=Loader)
```

Then you can import the functions from `ccx_messaging.utils.clowder` and see
how it's updated. Updating the kafka config may fail because there is a
connection to the broker already running.
