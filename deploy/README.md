# Deployments

This folder contains all the Clowdapps using the ccx-messaging library.

## How tun run it in ephemeral

Run

```
bonfire deploy \
    -c deploy/test.yaml \
    -n $NAMESPACE ccx-data-pipeline \
    -C archive-sync \
    --set-parameter archive-sync/IMAGE_TAG=pr-189-latest
```
