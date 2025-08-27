"""Handlers for CLI commands and some utility functions (GC + mem watchdog)."""

import argparse
import atexit
import gc
import importlib.metadata
import logging
import logging.config
import os
import sys
import threading
import time
import tracemalloc
from typing import Optional

from app_common_python import isClowderEnabled
from insights_messaging.appbuilder import AppBuilder

from ccx_messaging.utils.clowder import apply_clowder_config
from ccx_messaging.utils.logging import setup_watchtower
from ccx_messaging.utils.sentry import init_sentry

# ----------------------------------------------------------------------
# Konštanty a watchdog na čistenie pamäte
# ----------------------------------------------------------------------
RAM_CLEAN_THRESHOLD = int(float(os.getenv("RAM_CLEAN_THRESHOLD_MIB", 150)) * 1024 * 1024)
RAM_CHECK_INTERVAL = float(os.getenv("RAM_CHECK_INTERVAL", 60))  # sekundy


def _get_rss_bytes() -> Optional[int]:
    """Reálna rezidentná pamäť (RSS) v bajtoch, None ak psutil nie je dostupné."""
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss
    except Exception:
        return None


def _get_py_heap_bytes() -> Optional[int]:
    """Veľkosť Python haldy podľa tracemalloc (v bajtoch), ak je zapnutý."""
    if tracemalloc.is_tracing():
        current, _ = tracemalloc.get_traced_memory()
        return current
    return None


def _malloc_trim() -> None:
    """Voliteľné: vrátiť uvoľnenú pamäť OS (glibc >= 2.10, Linux)."""
    if sys.platform.startswith("linux"):
        try:
            import ctypes

            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass


def _start_mem_cleaner(
    threshold_bytes: int = RAM_CLEAN_THRESHOLD, interval: float = RAM_CHECK_INTERVAL
) -> None:
    """Vlákno sledujúce pamäť a spúšťajúce GC + trim pri prekročení prahu."""
    log = logging.getLogger(__name__)

    def _watch():
        while True:
            rss = _get_rss_bytes()
            py = _get_py_heap_bytes()

            log.debug(
                "[mem] RSS=%.1f MiB | PyHeap=%5s",
                (rss or 0) / 1_048_576,
                f"{py/1_048_576:.1f} MiB" if py is not None else "–",
            )

            if rss is not None and rss >= threshold_bytes:
                unreachable = gc.collect()
                _malloc_trim()
                log.warning(
                    "[gc] RSS %.1f MiB ≥ %.1f MiB → zozbieraných %d objektov, trim() OK",
                    rss / 1_048_576,
                    threshold_bytes / 1_048_576,
                    unreachable,
                )
            time.sleep(interval)

    threading.Thread(target=_watch, daemon=True, name="MemCleaner").start()


# ----------------------------------------------------------------------
# Zvyšok pôvodného súboru (nezmenené okrem volania _start_mem_cleaner)
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs="?", help="Application configuration.")
    parser.add_argument("--version", help="Show version.", action="store_true")
    return parser.parse_args()


def print_version() -> None:
    logger = logging.getLogger(__name__)
    logger.info(
        "Python interpreter version: %d.%d.%d",
        *sys.version_info[:3],
    )
    logger.info("%s version: %s", sys.argv[0], importlib.metadata.version("ccx-messaging"))
    try:
        ocp_rules_version = importlib.metadata.version("ccx-rules-ocp")
        logger.info("ccx-rules-ocp version: %s", ocp_rules_version)
    except importlib.metadata.PackageNotFoundError:
        pass


def apply_config(config) -> int:
    with open(config) as file_:
        if isClowderEnabled() and os.getenv("CLOWDER_ENABLED", "").lower() in ("true", "1", "yes"):
            manifest = apply_clowder_config(file_.read())
        else:
            manifest = file_.read()

        if os.getenv("MEMORY_PROFILER_ENABLED", "false").lower() == "true":
            tracemalloc.start(10)
            logging.getLogger(__name__).info("Memory tracking enabled – tracemalloc started")
            atexit.register(tracemalloc.stop)

        _start_mem_cleaner()  # ⬅️  nový watchdog

        app_builder = AppBuilder(manifest)
        logging_config = app_builder.service.get("logging")
        if logging_config:
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
    args = parse_args()

    if args.version:
        logging.basicConfig(format="%(message)s", level=logging.INFO)
        print_version()
        sys.exit(0)

    init_sentry(
        os.environ.get("SENTRY_DSN"),
        None,
        os.environ.get("SENTRY_ENVIRONMENT"),
        os.environ.get("SENTRY_ENABLED", "false").lower() == "true",
    )

    if args.config:
        sys.exit(apply_config(args.config))

    logging.getLogger(__name__).error(
        "Application configuration not provided. "
        "Use 'ccx-data-pipeline <config>' to run the application",
    )
    sys.exit(1)
