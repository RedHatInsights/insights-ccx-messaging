# Stage 1: Base and Build
FROM registry.access.redhat.com/ubi9-minimal:latest AS base

ENV VENV=/ccx-messaging-venv \
    HOME=/insights-ccx-messaging \
    REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt

ENV PATH="$VENV/bin:$PATH"
WORKDIR $HOME

COPY . $HOME

RUN microdnf install --nodocs -y python3.11 python3.11-devel gcc-c++ unzip tar git-core && \
    python3.11 -m venv $VENV && \
    pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .


# Stage 2: Final
FROM base AS final

RUN microdnf remove -y git-core gcc-c++ python3.11-devel && \
    microdnf clean all && \
    rpm -e --nodeps sqlite-libs krb5-libs libxml2 readline pam openssh openssh-clients

RUN mkdir -p $HOME/memray-profiles && \
    chmod -R g=u $HOME $VENV /etc/passwd && \
    chgrp -R 0 $HOME $VENV

# Volume for memray profile outputs
VOLUME ["$HOME/memray-profiles"]

USER 1001
