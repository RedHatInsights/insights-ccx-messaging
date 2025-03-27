"""Handlers for CLI commands and some utility functions."""

import argparse
import importlib.metadata
import logging
import os
import sys

from app_common_python import isClowderEnabled
from insights_messaging.appbuilder import AppBuilder

from ccx_messaging.utils.clowder import apply_clowder_config
from ccx_messaging.utils.logging import setup_watchtower
from ccx_messaging.utils.sentry import init_sentry


def parse_args() -> argparse.Namespace:
    """Parse the command line options and arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs="?", help="Application configuration.")
    parser.add_argument("--version", help="Show version.", action="store_true")
    return parser.parse_args()


def print_version() -> None:
    """Log version information."""
    logger = logging.getLogger(__name__)
    logger.info(
        "Python interpreter version: %d.%d.%d",
        sys.version_info.major,
        sys.version_info.minor,
        sys.version_info.micro,
    )
    logger.info(
        "%s version: %s",
        sys.argv[0],
        importlib.metadata.version("ccx-messaging"),
    )
    try:
        ocp_rules_version = importlib.metadata.version("ccx-rules-ocp")
        logger.info("ccx-rules-ocp version: %s", ocp_rules_version)

    except importlib.metadata.PackageNotFoundError:
        pass


def apply_config(config) -> int:
    """Apply configuration file provided as argument and run consumer."""
    with open(config) as file_:
        if isClowderEnabled() and os.getenv("CLOWDER_ENABLED") in ["True", "true", "1", "yes"]:
            manifest = apply_clowder_config(file_.read())
        else:
            manifest = file_.read()

        app_builder = AppBuilder(manifest)
        logging_config = app_builder.service["logging"]
        logging.config.dictConfig(logging_config)
        print_version()
        try:
            consumer = app_builder.build_app()
            setup_watchtower(logging_config)
            consumer.run()
            return 0

        except ModuleNotFoundError as ex:
            logging.error("Module not found: %s. Did you miss some dependency?", ex.name)
            return 1


def ccx_messaging() -> None:
    """Handle the ccx-messaging CLI command."""
    args = parse_args()

    if args.version:
        logging.basicConfig(format="%(message)s", level=logging.INFO)
        print_version()
        sys.exit(0)

    init_sentry(
        os.environ.get("SENTRY_DSN", None),
        None,
        os.environ.get("SENTRY_ENVIRONMENT", None),
        os.environ.get("SENTRY_ENABLED", "false").lower() == "true",
    )

    if args.config:
        retval = apply_config(args.config)
        sys.exit(retval)

    logger = logging.getLogger(__name__)
    logger.error(
        "Application configuration not provided. \
        Use 'ccx-data-pipeline <config>' to run the application",
    )
    sys.exit(1)
