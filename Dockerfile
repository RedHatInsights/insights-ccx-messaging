# Stage 1: Base
FROM registry.access.redhat.com/ubi9-minimal:latest AS base

ENV VENV=/ccx-messaging-venv \
    HOME=/insights-ccx-messaging \
    REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt

ENV PATH="$VENV/bin:$PATH"
WORKDIR $HOME

RUN curl -ksL https://certs.corp.redhat.com/certs/2015-IT-Root-CA.pem -o /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt && \
    curl -ksL https://certs.corp.redhat.com/certs/2022-IT-Root-CA.pem -o /etc/pki/ca-trust/source/anchors/2022-IT-Root-CA.pem && \
    update-ca-trust

RUN microdnf install --nodocs -y python3.11 unzip tar git-core && \
    python3.11 -m venv $VENV && \
    pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir setuptools -U

# Stage 2: Builder
FROM base AS builder

COPY . $HOME

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .

# Stage 3: Final
FROM base AS final

RUN microdnf remove -y git-core && \
    microdnf clean all && \
    rpm -e --nodeps sqlite-libs krb5-libs libxml2 readline pam openssh openssh-clients

COPY --from=builder $VENV $VENV
COPY --from=builder $HOME $HOME

RUN chmod -R g=u $HOME $VENV /etc/passwd && \
    chgrp -R 0 $HOME $VENV

USER 1001
