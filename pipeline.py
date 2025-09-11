#!/opt/phishfindr/venv/bin/python
import argparse
import logging
from collectors.o365_collector import O365Collector
from outputs.json_output import JSONOutput
from outputs.opensearch_output import OpenSearchOutput


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def run_pipeline(once: bool = False, output_backend: str = "json"):
    collector = O365Collector(interval=300)  # poll every 5 minutes

    if output_backend == "json":
        output = JSONOutput()
    elif output_backend == "opensearch":
        output = OpenSearchOutput()
    else:
        raise ValueError(f"Unsupported output backend: {output_backend}")

    logging.info("Starting pipeline with output=%s...", output_backend)

    try:
        if once:
            # Fetch once and exit
            for event in collector.collect():
                output.write(event)
            logging.info("One-shot pipeline run complete.")
        else:
            # Continuous polling
            for event in collector.run_forever():
                output.write(event)
    except KeyboardInterrupt:
        logging.info("Pipeline stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PhishFinder O365 pipeline")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run pipeline once and exit (instead of polling forever)"
    )
    parser.add_argument(
        "--output",
        choices=["json", "opensearch"],
        default="json",
        help="Select output backend (default: json)"
    )
    args = parser.parse_args()

    run_pipeline(once=args.once, output_backend=args.output)

