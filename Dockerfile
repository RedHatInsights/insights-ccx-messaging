# Stage 1: Base and Build
FROM registry.access.redhat.com/ubi9-minimal:latest AS base

ENV VENV=/ccx-messaging-venv \
    HOME=/insights-ccx-messaging \
    REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt

ENV PATH="$VENV/bin:$PATH"
WORKDIR $HOME

COPY . $HOME

RUN microdnf install --nodocs -y python3.11 unzip tar git-core && \
    python3.11 -m venv $VENV && \
    pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .


# Stage 2: Final
FROM base AS final

RUN microdnf remove -y git-core && \
    microdnf clean all && \
    rpm -e --nodeps sqlite-libs krb5-libs libxml2 readline pam openssh openssh-clients

RUN chmod -R g=u $HOME $VENV /etc/passwd && \
    chgrp -R 0 $HOME $VENV

USER 1001
