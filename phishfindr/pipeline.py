#!/opt/phishfindr/venv/bin/python

#!/usr/bin/env python3
"""
Phishfindr pipeline entrypoint.

This module is designed to be executed as a package with:
    python -m phishfindr --output postgres --once

It exposes `main()` so __main__.py or other runners can call it.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dotenv import load_dotenv
from typing import Iterable, List, Dict, Any

# Package-relative imports (works when run as module)
from .collectors.o365_collector import O365Collector
from .utils.normalizer import normalize_event
from .outputs.json_output import JSONOutput
from .outputs.opensearch_output import OpenSearchOutput
from .outputs.postgres_output import PostgresOutput

# Load environment variables from .env (if present)
load_dotenv()

logger = logging.getLogger("phishfindr")


def build_postgres_dsn() -> str:
    """Construct a DSN from environment variables (fallbacks safe defaults)."""
    db = os.getenv("POSTGRES_DB", "phishfindr")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"dbname={db} user={user} password={pwd} host={host} port={port}"


def choose_output(backend: str):
    """Return an output backend instance for the given name."""
    if backend == "json":
        return JSONOutput("events.json")
    if backend == "opensearch":
        # You can enhance OpenSearchOutput to accept host/port/auth from env if needed
        return OpenSearchOutput()
    if backend == "postgres":
        dsn = build_postgres_dsn()
        return PostgresOutput(dsn)
    raise ValueError(f"Unsupported output backend: {backend}")


def run_pipeline(once: bool = False, output_backend: str = "json", batch: bool = False):
    """
    Run the collector -> normalizer -> output pipeline.

    - once: fetch once and exit (useful for testing).
    - output_backend: 'json'|'opensearch'|'postgres'
    - batch: if True, write events in batches; otherwise per-event writes.
    """
    logger.info("Starting Phishfindr pipeline (once=%s, backend=%s, batch=%s)", once, output_backend, batch)
    collector = O365Collector()
    output = choose_output(output_backend)

    try:
        if once:
            # collect returns an iterable of raw events
            raw_events = list(collector.collect())
            normalized = [normalize_event(e) for e in raw_events]

            if batch and hasattr(output, "write_events"):
                output.write_events(normalized)
            else:
                # write per-event using whichever method is available
                for ev in normalized:
                    if hasattr(output, "write"):
                        output.write(ev)
                    elif hasattr(output, "write_events"):
                        output.write_events([ev])
                    else:
                        raise RuntimeError("Output backend exposes neither write nor write_events")
            logger.info("One-shot pipeline run complete.")
        else:
            # continuous mode: collector.run_forever yields events
            for raw in collector.run_forever():
                ev = normalize_event(raw)
                if batch and hasattr(output, "write_events"):
                    output.write_events([ev])
                else:
                    if hasattr(output, "write"):
                        output.write(ev)
                    elif hasattr(output, "write_events"):
                        output.write_events([ev])
                    else:
                        raise RuntimeError("Output backend exposes neither write nor write_events")
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (KeyboardInterrupt).")
    except Exception:
        logger.exception("Unhandled exception in pipeline loop.")
        raise
    finally:
        # graceful shutdown if backend supports it
        if hasattr(output, "close"):
            try:
                output.close()
            except Exception:
                logger.exception("Error closing output backend")


def main(argv: List[str] | None = None) -> int:
    """
    CLI entrypoint. Returns an exit code.
    """
    parser = argparse.ArgumentParser(prog="phishfindr", description="Phishfindr pipeline runner")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument(
        "--output",
        choices=["json", "opensearch", "postgres"],
        default="json",
        help="Output backend (default: json)",
    )
    parser.add_argument("--batch", action="store_true", help="Use batch writes when supported")

    args = parser.parse_args(argv)

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        run_pipeline(once=args.once, output_backend=args.output, batch=args.batch)
        return 0
    except Exception:
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
