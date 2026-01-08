# Copyright 2022 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing JSON schemas used by the applications."""

# Schema of the input message consumed from Kafka.
INPUT_MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "url": {"type": "string"},
        "b64_identity": {"type": "string", "contentEncoding": "base64"},
        "timestamp": {"type": "string"},
    },
    "required": ["url", "b64_identity", "timestamp"],
}

IDENTITY_SCHEMA = {
    "type": "object",
    "properties": {
        "identity": {
            "type": "object",
            "properties": {
                "account_number": {"type": "string"},
                "auth_type": {"type": "string"},
                "internal": {
                    "type": "object",
                    "properties": {
                        "auth_time": {"type": "number"},
                        "org_id": {"type": "string"},
                    },
                    "required": ["org_id"],
                },
                "type": {"type": "string"},  # type is a property of the identity
                "user": {
                    "type": "object",
                    "properties": {
                        "email": {"type": "string"},
                        "first_name": {"type": "string"},
                        "is_active": {"type": "boolean"},
                        "is_internal": {"type": "boolean"},
                        "is_org_admin": {"type": "boolean"},
                        "last_name": {"type": "string"},
                        "locale": {"type": "string"},
                        "username": {"type": "string"},
                    },
                },
            },
            "required": ["internal"],
        },
    },
    "required": ["identity"],
}

ARCHIVE_SYNCED_SCHEMA = {
    "type": "object",
    "properties": {
        "path": {"type": "string"},
        "original_path": {"type": "string"},
        "metadata": {
            "type": "object",
            "properties": {
                "cluster_id": {"type": "string"},
                "external_organization": {"type": "string"},
            },
            "required": ["cluster_id"],
        },
    },
    "required": ["path", "metadata"],
}

RULES_RESULTS_SCHEMA = {
    "type": "object",
    "properties": {
        "path": {"type": "string"},
        "metadata": {
            "type": "object",
            "properties": {
                "cluster_id": {"type": "string"},
                "external_organization": {"type": "string"},
            },
            "required": ["cluster_id"],
        },
        "report": {"type": "object"},
    },
    "required": ["path", "metadata", "report"],
}
